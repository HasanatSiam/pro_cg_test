import re
import json
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from config import redis_url
from redis import Redis
from flask import Blueprint, jsonify
from datetime import datetime, timezone


reidis_bp = Blueprint('redis', __name__)
redis_client = Redis.from_url(redis_url, decode_responses=True)

#redis

@reidis_bp.route('/scheduled_tasks', methods=['GET'])
def get_redbeat_scheduled_tasks():
    try:
        tasks_output = []
        local_tz = ZoneInfo("Asia/Dhaka")

        def get_next_run_from_redis(redis_client, key_str, local_tz):
            score = redis_client.zscore("redbeat::schedule", key_str)
            if score is None:
                return None
            dt_utc = datetime.fromtimestamp(score, timezone.utc)
            dt_local = dt_utc.astimezone(local_tz)
            return {
                # "utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S %Z"),
                # "local": dt_local.strftime("%Y-%m-%d %H:%M:%S %Z"),
                "utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "local": dt_local.strftime("%A, %d %B %Y, %I:%M %p %Z (%z)")
            }

        for key_str in redis_client.scan_iter("redbeat:*"):
            if key_str == "redbeat::schedule":
                continue  # skip internal RedBeat key
            key_type = redis_client.type(key_str)
            # if key_type != "hash":
            #     continue  # skip non-hash keys


            if key_type != "hash":
                print(f"Skipping key (not hash): {key_str} -> type: {key_type}")
                continue

            # ttl = redis_client.ttl(key_str)
            # if ttl == -2:
            #     continue  # key has expired

            task_data = redis_client.hgetall(key_str)
            if not task_data:
                continue

            # task_data is dict[str, str] due to decode_responses=True

            definition_raw = task_data.get("definition", "{}")
            try:
                definition = json.loads(definition_raw)
            except json.JSONDecodeError:
                definition = {}



            # try:
            #     definition = json.loads(task_data.get("definition", "{}"))
            # except json.JSONDecodeError:
            #     definition = {}

            # task_name = definition.get("name") or key_str.split(":", 1)[-1]
            def clean_task_name(name):
                # Remove trailing underscore + UUID pattern, e.g. _e43d6b82-5ad3-47e0-b215-ff9912a7f6d8
                return re.sub(r'_[0-9a-fA-F-]{36}$', '', name)

            raw_name = definition.get("name") or key_str.split(":", 1)[-1]
            task_name = clean_task_name(raw_name)
    

            # Exclude tasks starting with celery.
            task_full_name = definition.get("task", "")
            if task_full_name.startswith("celery."):
                continue

            # Parse meta info
            try:
                meta_raw = json.loads(task_data.get('meta', '{}')) or {}
                last_run_at = meta_raw.get("last_run_at", {})
                total_run_count = meta_raw.get("total_run_count", 0)
                

                

                if all(k in last_run_at for k in ("year", "month", "day", "hour", "minute", "second")):
                    dt_utc = datetime(
                        year=last_run_at["year"],
                        month=last_run_at["month"],
                        day=last_run_at["day"],
                        hour=last_run_at["hour"],
                        minute=last_run_at["minute"],
                        second=last_run_at["second"],
                        microsecond=last_run_at.get("microsecond", 0),
                        tzinfo=timezone.utc
                    )
                    dt_local = dt_utc.astimezone(local_tz)
                    last_run = {
                        # "utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S %Z"),
                        # "local": dt_local.strftime("%Y-%m-%d %H:%M:%S %Z")
                        "utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "local": dt_local.strftime("%A, %d %B %Y, %I:%M %p %Z (%z)")
                    }
                else:
                    last_run = None

            except Exception:
                last_run = None
                total_run_count = 0

            # Get next run from Redis sorted set score
            next_run = get_next_run_from_redis(redis_client, key_str, local_tz)

            # Identify interval or cron schedule
            schedule_info = {}
            if "schedule" in definition:
                schedule = definition["schedule"]
                # if "every" in schedule:
                if isinstance(schedule, dict) and "every" in schedule:
                    schedule_info["type"] = "interval"
                    schedule_info["every"] = schedule["every"]
                elif any(k in schedule for k in ["minute", "hour", "day_of_week", "day_of_month", "month_of_year"]):
                    schedule_info["type"] = "crontab"
                    schedule_info["expression"] = schedule
                else:
                    schedule_info["type"] = "unknown"
            else:
                schedule_info["type"] = "immediate"

            if next_run is None:
                schedule_info["status"] = "inactive"
            else:
            # Parse the UTC datetime string
                try:
                    next_run_utc = datetime.strptime(next_run["utc"], "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
                    now_utc = datetime.now(timezone.utc)

                    if next_run_utc <= now_utc:
                        schedule_info["status"] = "expired"
                    else:
                        schedule_info["status"] = "active"
                except Exception:
                    schedule_info["status"] = "unknown"

            tasks_output.append({
                "task_key": key_str,
                "task_name": task_name,
                "schedule_type": schedule_info["type"],
                "schedule_status": schedule_info.get("status"),
                "schedule_details": schedule_info.get("expression") or {"every": schedule_info.get("every")},
                "next_run": next_run,
                "last_run": last_run,
                "total_run_count": total_run_count,
            })

        return jsonify({
            "total_tasks": len(tasks_output),
            "tasks": tasks_output
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@reidis_bp.route('/scheduled_tasks/<path:task_key>', methods=['GET'])
def get_redbeat_scheduled_task(task_key):
    try:
        local_tz = ZoneInfo("Asia/Dhaka")
        redis_key = task_key if task_key.startswith("redbeat:") else f"redbeat:{task_key}"

        if redis_key == "redbeat::schedule":
            return jsonify({"error": "Invalid task key"}), 400

        if not redis_client.exists(redis_key):
            return jsonify({"error": "Task not found"}), 404

        task_type = redis_client.type(redis_key)
        if task_type != "hash":
            return jsonify({"error": "Invalid task format"}), 400

        task_data = redis_client.hgetall(redis_key)
        if not task_data:
            return jsonify({"error": "Task data missing or empty"}), 404

        try:
            definition = json.loads(task_data.get("definition", "{}"))
        except json.JSONDecodeError:
            definition = {}

        def clean_task_name(name):
            return re.sub(r'_[0-9a-fA-F-]{36}$', '', name)

        raw_name = definition.get("name") or redis_key.split(":", 1)[-1]
        task_name = clean_task_name(raw_name)

        if definition.get("task", "").startswith("celery."):
            return jsonify({"error": "Internal Celery task, not user-defined"}), 400

        # Parse meta info
        try:
            meta_raw = json.loads(task_data.get("meta", '{}')) or {}
            last_run_at = meta_raw.get("last_run_at", {})
            total_run_count = meta_raw.get("total_run_count", 0)

            if all(k in last_run_at for k in ("year", "month", "day", "hour", "minute", "second")):
                dt_utc = datetime(
                    year=last_run_at["year"],
                    month=last_run_at["month"],
                    day=last_run_at["day"],
                    hour=last_run_at["hour"],
                    minute=last_run_at["minute"],
                    second=last_run_at["second"],
                    microsecond=last_run_at.get("microsecond", 0),
                    tzinfo=timezone.utc
                )
                dt_local = dt_utc.astimezone(local_tz)
                last_run = {
                    "utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "local": dt_local.strftime("%A, %d %B %Y, %I:%M %p %Z (%z)")
                }
            else:
                last_run = None

        except Exception:
            last_run = None
            total_run_count = 0

        # Get next run
        def get_next_run_from_redis(redis_client, key_str, local_tz):
            score = redis_client.zscore("redbeat::schedule", key_str)
            if score is None:
                return None
            dt_utc = datetime.fromtimestamp(score, timezone.utc)
            dt_local = dt_utc.astimezone(local_tz)
            return {
                "utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "local": dt_local.strftime("%A, %d %B %Y, %I:%M %p %Z (%z)")
            }

        next_run = get_next_run_from_redis(redis_client, redis_key, local_tz)

        schedule_info = {}
        if "schedule" in definition:
            schedule = definition["schedule"]
            if isinstance(schedule, dict) and "every" in schedule:
                schedule_info["type"] = "interval"
                schedule_info["every"] = schedule["every"]
            elif any(k in schedule for k in ["minute", "hour", "day_of_week", "day_of_month", "month_of_year"]):
                schedule_info["type"] = "crontab"
                schedule_info["expression"] = schedule
            else:
                schedule_info["type"] = "unknown"
        else:
            schedule_info["type"] = "immediate"

        schedule_info["status"] = "active" if next_run else "inactive"

        return jsonify({
            "task_key": redis_key,
            "task_name": task_name,
            "schedule_type": schedule_info["type"],
            "schedule_status": schedule_info.get("status"),
            "schedule_details": schedule_info.get("expression") or {"every": schedule_info.get("every")},
            "next_run": next_run,
            "last_run": last_run,
            "total_run_count": total_run_count,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500



