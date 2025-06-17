import uuid
import logging
from celery.schedules import crontab
from sqlalchemy import or_
from datetime import datetime, timedelta
from flask import request, jsonify, make_response       # Flask utilities for handling requests and responses

from celery import current_app as celery  # Access the current Celery app

from redbeat_s.red_functions import create_redbeat_schedule, update_redbeat_schedule, delete_schedule_from_redis
from ad_hoc.ad_hoc_functions import execute_ad_hoc_task, execute_ad_hoc_task_v1

from flask import Blueprint, request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity



from executors.extensions import db
from executors.models import (
    DefAsyncTask,
    DefAsyncTaskParam,
    DefAsyncTaskSchedule,
    DefAsyncTaskRequest,
    DefAsyncTaskSchedulesV,
    DefAsyncExecutionMethods,
    DefAsyncTaskScheduleNew,
)

tasks_bp = Blueprint('tasks', __name__)


@tasks_bp.route('/Create_ExecutionMethod', methods=['POST'])
@jwt_required()
def Create_ExecutionMethod():
    try:
        execution_method = request.json.get('execution_method')
        internal_execution_method = request.json.get('internal_execution_method')
        executor = request.json.get('executor')
        description = request.json.get('description')

        # Validate required fields
        if not execution_method or not internal_execution_method:
            return jsonify({"error": "Missing required fields: execution_method or internal_execution_method"}), 400

        # Check if the execution method already exists
        existing_method = DefAsyncExecutionMethods.query.filter_by(internal_execution_method=internal_execution_method).first()
        if existing_method:
            return jsonify({"error": f"Execution method '{internal_execution_method}' already exists"}), 409

        # Create a new execution method object
        new_method = DefAsyncExecutionMethods(
            execution_method=execution_method,
            internal_execution_method=internal_execution_method,
            executor=executor,
            description=description
        )

        # Add to session and commit
        db.session.add(new_method)
        db.session.commit()

        return jsonify({"message": "Execution method created successfully", "data": new_method.json()}), 201

    except Exception as e:
        return jsonify({"message": "Error creating execution method", "error": str(e)}), 500



@tasks_bp.route('/Show_ExecutionMethods', methods=['GET'])
@jwt_required()
def Show_ExecutionMethods():
    try:
        methods = DefAsyncExecutionMethods.query.order_by(DefAsyncExecutionMethods.internal_execution_method.desc()).all()
        if not methods:
            return jsonify({"message": "No execution methods found"}), 404
        return jsonify([method.json() for method in methods]), 200
    except Exception as e:
        return jsonify({"message": "Error retrieving execution methods", "error": str(e)}), 500


@tasks_bp.route('/Show_ExecutionMethods/v1', methods=['GET'])
def Show_ExecutionMethods_v1():
    try:
        methods = DefAsyncExecutionMethods.query.order_by(DefAsyncExecutionMethods.internal_execution_method.desc()).all()
        if not methods:
            return jsonify({"message": "No execution methods found"}), 404
        return jsonify([method.json() for method in methods]), 200
    except Exception as e:
        return jsonify({"message": "Error retrieving execution methods", "error": str(e)}), 500


@tasks_bp.route('/Show_ExecutionMethods/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def paginated_execution_methods(page, limit):
    try:
        paginated = DefAsyncExecutionMethods.query.order_by(DefAsyncExecutionMethods.creation_date.desc()).paginate(page=page, per_page=limit, error_out=False)

        if not paginated.items:
            return jsonify({"message": "No execution methods found"}), 404

        return jsonify({
            "items": [method.json() for method in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200

    except Exception as e:
        return jsonify({"message": "Error retrieving execution methods", "error": str(e)}), 500



@tasks_bp.route('/def_async_execution_methods/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_execution_methods(page, limit):
    try:
        search_query = request.args.get('internal_execution_method', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAsyncExecutionMethods.query

        if search_query:
            query = query.filter(
                or_(
                    DefAsyncExecutionMethods.internal_execution_method.ilike(f'%{search_query}%'),
                    DefAsyncExecutionMethods.internal_execution_method.ilike(f'%{search_underscore}%'),
                    DefAsyncExecutionMethods.internal_execution_method.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefAsyncExecutionMethods.creation_date.desc()).paginate(
            page=page, per_page=limit, error_out=False
        )

        if not paginated.items:
            return jsonify({"message": "No execution methods found"}), 404

        return jsonify({
            "items": [method.json() for method in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200

    except Exception as e:
        return jsonify({"message": "Error searching execution methods", "error": str(e)}), 500



@tasks_bp.route('/Show_ExecutionMethod/<string:internal_execution_method>', methods=['GET'])
@jwt_required()
def Show_ExecutionMethod(internal_execution_method):
    try:
        method = DefAsyncExecutionMethods.query.get(internal_execution_method)
        if not method:
            return jsonify({"message": f"Execution method '{internal_execution_method}' not found"}), 404
        return jsonify(method.json()), 200
    except Exception as e:
        return jsonify({"message": "Error retrieving execution method", "error": str(e)}), 500


@tasks_bp.route('/Update_ExecutionMethod/<string:internal_execution_method>', methods=['PUT'])
@jwt_required()
def Update_ExecutionMethod(internal_execution_method):
    try:
        execution_method = DefAsyncExecutionMethods.query.filter_by(internal_execution_method=internal_execution_method).first()

        if execution_method:
            # Only update fields that are provided in the request
            if 'execution_method' in request.json:
                execution_method.execution_method = request.json.get('execution_method')
            if 'executor' in request.json:
                execution_method.executor = request.json.get('executor')
            if 'description' in request.json:
                execution_method.description = request.json.get('description')

            execution_method.last_updated_by = 101

            # Update the last update timestamp
            execution_method.last_update_date = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({"message": "Execution method updated successfully"}), 200)

        return make_response(jsonify({"message": f"Execution method with internal_execution_method '{internal_execution_method}' not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error updating execution method", "error": str(e)}), 500)


@tasks_bp.route('/Delete_ExecutionMethod/<string:internal_execution_method>', methods=['DELETE'])
@jwt_required()
def Delete_ExecutionMethod(internal_execution_method):
    try:
        # Find the execution method by internal_execution_method
        execution_method = DefAsyncExecutionMethods.query.filter_by(internal_execution_method=internal_execution_method).first()

        # If the execution method does not exist, return a 404 response
        if not execution_method:
            return jsonify({"message": f"Execution method with internal_execution_method '{internal_execution_method}' not found"}), 404

        # Delete the execution method from the database
        db.session.delete(execution_method)
        db.session.commit()

        return jsonify({"message": f"Execution method with internal_execution_method '{internal_execution_method}' successfully deleted"}), 200

    except Exception as e:
        return jsonify({"error": "Failed to delete execution method", "details": str(e)}), 500


# Create a task definition
@tasks_bp.route('/Create_Task', methods=['POST'])
@jwt_required()
def Create_Task():
    try:
        user_task_name = request.json.get('user_task_name')
        task_name = request.json.get('task_name')
        execution_method = request.json.get('execution_method')
        internal_execution_method = request.json.get('internal_execution_method')
        executor = request.json.get('executor')
        script_name = request.json.get('script_name')
        script_path = request.json.get('script_path')
        description = request.json.get('description')
        srs = request.json.get('srs')
        sf  = request.json.get('sf')

        new_task = DefAsyncTask(
            user_task_name = user_task_name,
            task_name = task_name,
            execution_method = execution_method,
            internal_execution_method = internal_execution_method,
            executor = executor,
            script_name = script_name,
            script_path = script_path,
            description = description,
            cancelled_yn = 'N',
            srs = srs,
            sf  = sf,
            created_by = 101
            #last_updated_by=last_updated_by

        )
        db.session.add(new_task)
        db.session.commit()

        return {"message": "DEF async task created successfully"}, 201

    except Exception as e:
        return {"message": "Error creating Task", "error": str(e)}, 500


@tasks_bp.route('/def_async_tasks', methods=['GET'])
@jwt_required()
def Show_Tasks():
    try:
        tasks = DefAsyncTask.query.order_by(DefAsyncTask.def_task_id.desc()).all()
        return make_response(jsonify([task.json() for task in tasks]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting DEF async Tasks", "error": str(e)}), 500)


@tasks_bp.route('/def_async_tasks/v1', methods=['GET'])
def Show_Tasks_v1():
    try:
        tasks = DefAsyncTask.query.order_by(DefAsyncTask.def_task_id.desc()).all()
        return make_response(jsonify([task.json() for task in tasks]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting DEF async Tasks", "error": str(e)}), 500)


@tasks_bp.route('/def_async_tasks/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def Show_Tasks_Paginated(page, limit):
    try:
        tasks = DefAsyncTask.query.order_by(DefAsyncTask.creation_date.desc())
        paginated = tasks.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [model.json() for model in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error getting DEF async Tasks", "error": str(e)}), 500)


@tasks_bp.route('/def_async_tasks/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def def_async_tasks_show_tasks(page, limit):
    try:
        search_query = request.args.get('user_task_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAsyncTask.query
        if search_query:
            
            query = query.filter(or_(
                DefAsyncTask.user_task_name.ilike(f'%{search_query}%'),
                DefAsyncTask.user_task_name.ilike(f'%{search_underscore}%'),
                DefAsyncTask.user_task_name.ilike(f'%{search_space}%')
            ))
        paginated = query.order_by(DefAsyncTask.def_task_id.desc()).paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [task.json() for task in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error fetching tasks", "error": str(e)}), 500)



@tasks_bp.route('/Show_Task/<task_name>', methods=['GET'])
@jwt_required()
def Show_Task(task_name):
    try:
        task = DefAsyncTask.query.filter_by(task_name=task_name).first()

        if not task:
            return {"message": f"Task with name '{task_name}' not found"}, 404

        return make_response(jsonify(task.json()), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error getting the task", "error": str(e)}), 500)


@tasks_bp.route('/Update_Task/<string:task_name>', methods=['PUT'])
@jwt_required()
def Update_Task(task_name):
    try:
        task = DefAsyncTask.query.filter_by(task_name=task_name).first()
        if task:
            # Only update fields that are provided in the request
            if 'user_task_name' in request.json:
                task.user_task_name = request.json.get('user_task_name')
            if 'execution_method' in request.json:
                task.execution_method = request.json.get('execution_method')
            if 'script_name' in request.json:
                task.script_name = request.json.get('script_name')
            if 'description' in request.json:
                task.description = request.json.get('description')
            if 'srs' in request.json:
                task.srs = request.json.get('srs')
            if 'sf' in request.json:
                task.sf = request.json.get('sf')
            if 'last_updated_by' in request.json:
                task.last_updated_by = request.json.get('last_updated_by')

            # Update the timestamps to reflect the modification time
            task.updated_at = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({"message": "DEF async Task updated successfully"}), 200)

        return make_response(jsonify({"message": f"DEF async Task with name '{task_name}' not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error updating DEF async Task", "error": str(e)}), 500)


@tasks_bp.route('/Cancel_Task/<string:task_name>', methods=['PUT'])
@jwt_required()
def Cancel_Task(task_name):
    try:
        # Find the task by task_name in the DEF_ASYNC_TASKS table
        task = DefAsyncTask.query.filter_by(task_name=task_name).first()

        if task:
            # Update the cancelled_yn field to 'Y' (indicating cancellation)
            task.cancelled_yn = 'Y'

            db.session.commit()

            return make_response(jsonify({"message": f"Task {task_name} has been cancelled successfully"}), 200)

        return make_response(jsonify({"message": f"Task {task_name} not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error cancelling Task", "error": str(e)}), 500)






@tasks_bp.route('/Add_TaskParams/<string:task_name>', methods=['POST'])
@jwt_required()
def Add_TaskParams(task_name):
    try:
        # Check if the task exists in the DEF_ASYNC_TASKS table
        existing_task = DefAsyncTask.query.filter_by(task_name=task_name).first()
        if not existing_task:
            return jsonify({"error": f"Task '{task_name}' does not exist"}), 404

        task_name = existing_task.task_name
        # Fetch parameters from the request
        parameters = request.json.get('parameters', [])
        if not parameters:
            return jsonify({"error": "No parameters provided"}), 400

        new_params = []
        for param in parameters:
            #seq = param.get('seq')
            parameter_name = param.get('parameter_name')
            data_type = param.get('data_type')
            description = param.get('description')
            created_by = request.json.get('created_by')

            # Validate required fields
            if not (parameter_name and data_type):
                return jsonify({"error": "Missing required parameter fields"}), 400

            # Create a new parameter object
            new_param = DefAsyncTaskParam(
                task_name=task_name,
                #seq=seq,
                parameter_name=parameter_name,
                data_type=data_type,
                description=description,
                created_by=created_by
            )
            new_params.append(new_param)

        # Add all new parameters to the session and commit
        db.session.add_all(new_params)
        db.session.commit()

        return make_response(jsonify([param.json() for param in new_params])), 201
    except Exception as e:
        return jsonify({"error": "Failed to create task parameters", "details": str(e)}), 500



@tasks_bp.route('/Show_TaskParams/<string:task_name>', methods=['GET'])
@jwt_required()
def Show_Parameter(task_name):
    try:
        parameters = DefAsyncTaskParam.query.filter_by(task_name=task_name).all()

        if not parameters:
            return make_response(jsonify({"message": f"No parameters found for task '{task_name}'"}), 404)

        return make_response(jsonify([param.json() for param in parameters]), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error getting Task Parameters", "error": str(e)}), 500)



@tasks_bp.route('/Show_TaskParams/<string:task_name>/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def Show_TaskParams_Paginated(task_name, page, limit):
    try:
        query = DefAsyncTaskParam.query.filter_by(task_name=task_name)
        paginated = query.order_by(DefAsyncTaskParam.def_param_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        if not paginated.items:
            return make_response(jsonify({"message": f"No parameters found for task '{task_name}'"}), 404)

        return make_response(jsonify({
            "items": [param.json() for param in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error getting Task Parameters", "error": str(e)}), 500)



@tasks_bp.route('/Update_TaskParams/<string:task_name>/<int:def_param_id>', methods=['PUT'])
@jwt_required()
def Update_TaskParams(task_name, def_param_id):
    try:
        # Get the updated values from the request body
        parameter_name = request.json.get('parameter_name')
        data_type = request.json.get('data_type')
        description = request.json.get('description')
        last_updated_by = 101

        # Find the task parameter by task_name and seq
        param = DefAsyncTaskParam.query.filter_by(task_name=task_name, def_param_id=def_param_id).first()

        # If the parameter does not exist, return a 404 response
        if not param:
            return jsonify({"message": f"Parameter with def_param_id '{def_param_id}' not found for task '{task_name}'"}), 404

        # Update the fields with the new values
        if parameter_name:
            param.parameter_name = parameter_name
        if data_type:
            param.data_type = data_type
        if description:
            param.description = description
        if last_updated_by:
            param.last_updated_by = last_updated_by

        # Commit the changes to the database
        db.session.commit()

        return jsonify({"message": "Task parameter updated successfully", "task_param": param.json()}), 200

    except Exception as e:
        return jsonify({"error": "Error updating task parameter", "details": str(e)}), 500



@tasks_bp.route('/Delete_TaskParams/<string:task_name>/<int:def_param_id>', methods=['DELETE'])
@jwt_required()
def Delete_TaskParams(task_name, def_param_id):
    try:
        # Find the task parameter by task_name and seq
        param = DefAsyncTaskParam.query.filter_by(task_name=task_name, def_param_id=def_param_id).first()

        # If the parameter does not exist, return a 404 response
        if not param:
            return jsonify({"message": f"Parameter with def_param_id '{def_param_id}' not found for task '{task_name}'"}), 404

        # Delete the parameter from the database
        db.session.delete(param)
        db.session.commit()

        return jsonify({"message": f"Parameter with def_param_id '{def_param_id}' successfully deleted from task '{task_name}'"}), 200

    except Exception as e:
        return jsonify({"error": "Failed to delete task parameter", "details": str(e)}), 500



@tasks_bp.route('/Create_TaskSchedule', methods=['POST'])
@jwt_required()
def Create_TaskSchedule():
    try:
        user_schedule_name = request.json.get('user_schedule_name', 'Immediate')
        task_name = request.json.get('task_name')
        parameters = request.json.get('parameters', {})
        schedule_type = request.json.get('schedule_type')
        schedule_data = request.json.get('schedule', {})

        if not task_name:
            return jsonify({'error': 'Task name is required'}), 400

        # Fetch task details from the database
        task = DefAsyncTask.query.filter_by(task_name=task_name).first()
        if not task:
            return jsonify({'error': f'No task found with task_name: {task_name}'}), 400

        # Prevent scheduling if the task is cancelled
        if getattr(task, 'cancelled_yn', 'N') == 'Y':
            return jsonify({'error': f"Task '{task_name}' is cancelled and cannot be scheduled."}), 400

        user_task_name = task.user_task_name
        executor = task.executor
        script_name = task.script_name

        schedule_name = str(uuid.uuid4())
        # redbeat_schedule_name = f"{user_schedule_name}_{schedule_name}"
        redbeat_schedule_name = None
        if schedule_type != "IMMEDIATE":
            redbeat_schedule_name = f"{user_schedule_name}_{schedule_name}"

        args = [script_name, user_task_name, task_name, user_schedule_name, redbeat_schedule_name, schedule_type, schedule_data]
        kwargs = {}

        # Validate task parameters
        task_params = DefAsyncTaskParam.query.filter_by(task_name=task_name).all()
        for param in task_params:
            param_name = param.parameter_name
            if param_name in parameters:
                kwargs[param_name] = parameters[param_name]
            else:
                return jsonify({'error': f'Missing value for parameter: {param_name}'}), 400

        # Handle scheduling based on schedule type
        cron_schedule = None
        schedule_minutes = None

        if schedule_type == "WEEKLY_SPECIFIC_DAYS":
            values = schedule_data.get('VALUES', [])  # e.g., ["Monday", "Wednesday"]
            day_map = {
                "SUN": 0, "MON": 1, "TUE": 2, "WED": 3,
                "THU": 4, "FRI": 5, "SAT": 6
            }
            days_of_week = ",".join(str(day_map[day.upper()]) for day in values if day.upper() in day_map)
            cron_schedule = crontab(minute=0, hour=0, day_of_week=days_of_week)

        elif schedule_type == "MONTHLY_SPECIFIC_DATES":
            values = schedule_data.get('VALUES', [])  # e.g., ["5", "15"]
            dates_of_month = ",".join(values)
            cron_schedule = crontab(minute=0, hour=0, day_of_month=dates_of_month)

        elif schedule_type == "ONCE":
            one_time_date = schedule_data.get('VALUES')  # e.g., {"date": "2025-03-01 14:30"}
            if not one_time_date:
                return jsonify({'error': 'Date is required for one-time execution'}), 400
            dt = datetime.strptime(one_time_date, "%Y-%m-%d %H:%M")
            cron_schedule = crontab(minute=dt.minute, hour=dt.hour, day_of_month=dt.day, month_of_year=dt.month)

        elif schedule_type == "PERIODIC":
            # Extract frequency type and frequency value from schedule_data
            frequency_type_raw = schedule_data.get('FREQUENCY_TYPE', 'MINUTES')
            frequency_type = frequency_type_raw.upper().strip().rstrip('s').replace('(', '').replace(')', '')
            frequency = schedule_data.get('FREQUENCY', 1)

            # Log frequency values to help with debugging
            print(f"Frequency Type: {frequency_type}")
            print(f"Frequency: {frequency}")
           
            # Handle different frequency types
            if frequency_type == 'MONTHS':
               schedule_minutes = frequency * 30 * 24 * 60  # Approximate calculation: 1 month = 30 days
            elif frequency_type == 'WEEKS':
               schedule_minutes = frequency * 7 * 24 * 60  # 7 days * 24 hours * 60 minutes
            elif frequency_type == 'DAYS':
               schedule_minutes = frequency * 24 * 60  # 1 day = 24 hours = 1440 minutes
            elif frequency_type == 'HOURS':
               schedule_minutes = frequency * 60  # 1 hour = 60 minutes
            elif frequency_type == 'MINUTES':
               schedule_minutes = frequency  # Frequency is already in minutes
            else:
               return jsonify({'error': f'Invalid frequency type: {frequency_type}'}), 400

        # elif schedule_type == "MONTHLY_LAST_DAY":

        #     try:
        #         today = datetime.today()
        #         start_year = today.year
        #         start_month = today.month

        #         # Calculate how many months left in the year including current month
        #         months_left = 12 - start_month + 1  # +1 to include the current month itself

        #         for i in range(months_left):
        #             # Calculate the target year and month
        #             year = start_year  # same year, no spanning next year
        #             month = start_month + i

        #             # Find the first day of the next month
        #             if month == 12:
        #                 next_month = datetime(year + 1, 1, 1)
        #             else:
        #                 next_month = datetime(year, month + 1, 1)

        #             # Calculate the last day of the current month
        #             last_day_dt = next_month - timedelta(days=1)
        #             last_day = last_day_dt.day

        #             # Create a cron schedule for the last day of this month at midnight
        #             cron_schedule = crontab(
        #                 minute=0,
        #                 hour=0,
        #                 day_of_month=last_day,
        #                 month_of_year=month
        #             )

        #             redbeat_schedule_name = f"{user_schedule_name}_{uuid.uuid4()}"

        #             args_with_schedule = [
        #                 script_name,
        #                 user_task_name,
        #                 task_name,
        #                 user_schedule_name,
        #                 redbeat_schedule_name,
        #                 schedule_type,
        #                 schedule_data
        #             ]

        #             # Create Redis schedule entry via RedBeat
        #             create_redbeat_schedule(
        #                 schedule_name=redbeat_schedule_name,
        #                 executor=executor,
        #                 cron_schedule=cron_schedule,
        #                 args=args_with_schedule,
        #                 kwargs=kwargs,
        #                 celery_app=celery
        #             )

        #             # Create database record for the schedule
        #             new_schedule = DefAsyncTaskScheduleNew(
        #                 user_schedule_name=user_schedule_name,
        #                 redbeat_schedule_name=redbeat_schedule_name,
        #                 task_name=task_name,
        #                 args=args_with_schedule,
        #                 kwargs=kwargs,
        #                 parameters=kwargs,
        #                 schedule_type=schedule_type,
        #                 schedule={"scheduled_for": f"{year}-{month:02}-{last_day} 00:00"},
        #                 cancelled_yn='N',
        #                 created_by=101
        #             )

        #             db.session.add(new_schedule)

        #         db.session.commit()

        #         return jsonify({
        #             "message": f"Monthly last-day tasks scheduled for the remaining {months_left} months of {start_year}"
        #         }), 201

        #     except Exception as e:
        #         db.session.rollback()
        #         return jsonify({
        #             "error": "Failed to schedule monthly last-day tasks",
        #             "details": str(e)
        #         }), 500


        
        # Handle Ad-hoc Requests
        elif schedule_type == "IMMEDIATE":
            try:
                result = execute_ad_hoc_task_v1(
                    user_schedule_name=user_schedule_name,
                    executor=executor,
                    task_name=task_name,
                    args=args,
                    kwargs=kwargs,
                    schedule_type=schedule_type,
                    cancelled_yn='N',
                    created_by=101
                )
                return jsonify(result), 201
            except Exception as e:
                return jsonify({"error": "Failed to execute ad-hoc task", "details": str(e)}), 500

        else:
            return jsonify({'error': 'Invalid schedule type'}), 400
        # Handle Scheduled Tasks
        try:
            create_redbeat_schedule(
                schedule_name=redbeat_schedule_name,
                executor=executor,
                schedule_minutes=schedule_minutes if schedule_minutes else None,
                cron_schedule=cron_schedule if cron_schedule else None,
                args=args,
                kwargs=kwargs,
                celery_app=celery
            )
        except Exception as e:
            return jsonify({"error": "Failed to create RedBeat schedule", "details": str(e)}), 500

        # if schedule_type != "IMMEDIATE":
        # Store schedule in DB
        new_schedule = DefAsyncTaskScheduleNew(
            user_schedule_name=user_schedule_name,
            redbeat_schedule_name=redbeat_schedule_name,
            task_name=task_name,
            args=args,
            kwargs=kwargs,
            parameters=kwargs,
            schedule_type=schedule_type,
            schedule=schedule_data,
            # ready_for_redbeat="N",
            cancelled_yn='N',
            created_by=101
        )

        db.session.add(new_schedule)
        db.session.commit()

        return jsonify({
            "message": "Task schedule created successfully!",
            "schedule_id": new_schedule.def_task_sche_id
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": "Failed to create task schedule", "details": str(e)}), 500


@tasks_bp.route('/Show_TaskSchedules', methods=['GET'])
@jwt_required()
def Show_TaskSchedules():
    try:
    #     schedules = DefAsyncTaskSchedulesV.query \
    # .filter(DefAsyncTaskSchedulesV.ready_for_redbeat != 'Y') \
    # .order_by(desc(DefAsyncTaskSchedulesV.def_task_sche_id)) \
    # .all()
        schedules = DefAsyncTaskSchedulesV.query.order_by(DefAsyncTaskSchedulesV.def_task_sche_id.desc()).all()
        # Return the schedules as a JSON response
        return jsonify([schedule.json() for schedule in schedules])

    except Exception as e:
        # Handle any errors and return them as a JSON response
        return jsonify({"error": str(e)}), 500



@tasks_bp.route('/def_async_task_schedules/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def paginated_task_schedules(page, limit):
    try:
        paginated = DefAsyncTaskSchedulesV.query.order_by(
            DefAsyncTaskSchedulesV.def_task_sche_id.desc()
        ).paginate(page=page, per_page=limit, error_out=False)

        return jsonify({
            "items": [schedule.json() for schedule in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200
    except Exception as e:
        return jsonify({"message": "Error fetching task schedules", "error": str(e)}), 500


@tasks_bp.route('/def_async_task_schedules/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_task_schedules(page, limit):
    try:
        search_query = request.args.get('task_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAsyncTaskSchedulesV.query

        if search_query:
            query = query.filter(
                or_(
                    DefAsyncTaskSchedulesV.task_name.ilike(f'%{search_query}%'),
                    DefAsyncTaskSchedulesV.task_name.ilike(f'%{search_underscore}%'),
                    DefAsyncTaskSchedulesV.task_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefAsyncTaskSchedulesV.def_task_sche_id.desc()).paginate(
            page=page, per_page=limit, error_out=False
        )

        return jsonify({
            "items": [schedule.json() for schedule in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200
    except Exception as e:
        return jsonify({"message": "Error searching task schedules", "error": str(e)}), 500





@tasks_bp.route('/Show_TaskSchedule/<string:task_name>', methods=['GET'])
@jwt_required()
def Show_TaskSchedule(task_name):
    try:
        schedule = DefAsyncTaskSchedule.query.filter_by(task_name=task_name).first()
        if schedule:
            return make_response(jsonify(schedule.json()), 200)

        return make_response(jsonify({"message": f"Task Periodic Schedule for {task_name} not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving Task Periodic Schedule", "error": str(e)}), 500)



@tasks_bp.route('/Update_TaskSchedule/<string:task_name>', methods=['PUT'])
@jwt_required()
def Update_TaskSchedule(task_name):
    try:
        redbeat_schedule_name = request.json.get('redbeat_schedule_name')
        if not redbeat_schedule_name:
            return jsonify({"message": "redbeat_schedule_name is required in the payload"}), 400

        schedule = DefAsyncTaskScheduleNew.query.filter_by(
            task_name=task_name, redbeat_schedule_name=redbeat_schedule_name
        ).first()
        executors = DefAsyncTask.query.filter_by(task_name=task_name).first()

        if not schedule:
            return jsonify({"message": f"Task Periodic Schedule for {redbeat_schedule_name} not found"}), 404

        # if schedule.ready_for_redbeat != 'N':
        #     return jsonify({
        #         "message": f"Task Periodic Schedule for {redbeat_schedule_name} is not marked as 'N'. Update is not allowed."
        #     }), 400

        # Update fields
        schedule.parameters = request.json.get('parameters', schedule.parameters)
        schedule.kwargs = request.json.get('parameters', schedule.kwargs)
        schedule.schedule_type = request.json.get('schedule_type', schedule.schedule_type)
        schedule.schedule = request.json.get('schedule', schedule.schedule)
        schedule.last_updated_by = 102  # Static user ID

        # Handle scheduling logic
        cron_schedule = None
        schedule_minutes = None

        if schedule.schedule_type == "WEEKLY_SPECIFIC_DAYS":
            values = schedule.schedule.get('VALUES', [])
            day_map = {"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6}
            days_of_week = ",".join(str(day_map[day.upper()]) for day in values if day.upper() in day_map)
            cron_schedule = crontab(minute=0, hour=0, day_of_week=days_of_week)

        elif schedule.schedule_type == "MONTHLY_SPECIFIC_DATES":
            values = schedule.schedule.get('VALUES', [])
            dates_of_month = ",".join(values)
            cron_schedule = crontab(minute=0, hour=0, day_of_month=dates_of_month)

        elif schedule.schedule_type == "ONCE":
            one_time_date = schedule.schedule.get('VALUES')
            dt = datetime.strptime(one_time_date, "%Y-%m-%d %H:%M")
            cron_schedule = crontab(minute=dt.minute, hour=dt.hour, day_of_month=dt.day, month_of_year=dt.month)

        elif schedule.schedule_type == "PERIODIC":
            frequency_type = schedule.schedule.get('FREQUENCY_TYPE', 'minutes').lower()
            frequency = schedule.schedule.get('FREQUENCY', 1)
            
            if frequency_type == 'months':
                schedule_minutes = frequency * 30 * 24 * 60
            elif frequency_type == 'days':
                schedule_minutes = frequency * 24 * 60
            elif frequency_type == 'hours':
                schedule_minutes = frequency * 60
            else:
                schedule_minutes = frequency  # Default to minutes

        # Ensure at least one scheduling method is provided
        if not schedule_minutes and not cron_schedule:
            return jsonify({"message": "Either 'schedule_minutes' or 'cron_schedule' must be provided."}), 400

        # Update RedBeat schedule
        try:
            update_redbeat_schedule(
                schedule_name=redbeat_schedule_name,
                task=executors.executor,
                schedule_minutes=schedule_minutes,
                cron_schedule=cron_schedule,
                args=schedule.args,
                kwargs=schedule.kwargs,
                celery_app=celery
            )
        except Exception as e:
            db.session.rollback()
            return jsonify({"message": "Error updating Redis. Database changes rolled back.", "error": str(e)}), 500

        db.session.commit()
        return jsonify({"message": f"Task Schedule for {redbeat_schedule_name} updated successfully in database and Redis"}), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Error updating Task Schedule", "error": str(e)}), 500


@tasks_bp.route('/Cancel_TaskSchedule/<string:task_name>', methods=['PUT'])
@jwt_required()
def Cancel_TaskSchedule(task_name):
    try:
        # Extract redbeat_schedule_name from payload
        redbeat_schedule_name = request.json.get('redbeat_schedule_name')
        if not redbeat_schedule_name:
            return make_response(jsonify({"message": "redbeat_schedule_name is required in the payload"}), 400)

        # Find the task schedule in the database
        schedule = DefAsyncTaskScheduleNew.query.filter_by(task_name=task_name, redbeat_schedule_name=redbeat_schedule_name).first()

        if not schedule:
            return make_response(jsonify({"message": f"Task periodic schedule for {redbeat_schedule_name} not found"}), 404)

        # Check if ready_for_redbeat is 'N' (only then cancellation is allowed)
        # if schedule.ready_for_redbeat != 'N':
        #     return make_response(jsonify({"message": f"Cancellation not allowed. Task periodic schedule for {redbeat_schedule_name} is already processed in Redis"}), 400)

        # Update the `cancelled_yn` field to 'Y' (marking it as cancelled)
        schedule.cancelled_yn = 'Y'

        # Commit the change to the database
        db.session.commit()

        # Now, call the function to delete the schedule from Redis
        redis_response, redis_status = delete_schedule_from_redis(redbeat_schedule_name)

        # If there is an issue deleting from Redis, rollback the database update
        if redis_status != 200:
            db.session.rollback()
            return make_response(jsonify({"message": "Task schedule cancelled, but failed to delete from Redis", "error": redis_response['error']}), 500)

        # Return success message if both operations are successful
        return make_response(jsonify({"message": f"Task periodic schedule for {redbeat_schedule_name} has been cancelled successfully in the database and deleted from Redis"}), 200)

    except Exception as e:
        db.session.rollback()  # Rollback on failure
        return make_response(jsonify({"message": "Error cancelling task periodic schedule", "error": str(e)}), 500)


@tasks_bp.route('/Reschedule_Task/<string:task_name>', methods=['PUT'])
@jwt_required()
def Reschedule_TaskSchedule(task_name):
    try:
        data = request.get_json()
        redbeat_schedule_name = data.get('redbeat_schedule_name')
        if not redbeat_schedule_name:
            return make_response(jsonify({'error': 'redbeat_schedule_name is required'}), 400)

        # Find the cancelled schedule in DB
        schedule = DefAsyncTaskScheduleNew.query.filter_by(
            task_name=task_name,
            redbeat_schedule_name=redbeat_schedule_name,
            cancelled_yn='Y'
        ).first()

        if not schedule:
            return make_response(jsonify({'error': 'Cancelled schedule not found'}), 404)

        # Determine cron or periodic schedule
        cron_schedule = None
        schedule_minutes = None
        schedule_data = schedule.schedule
        schedule_type = schedule.schedule_type

        if schedule_type == "WEEKLY_SPECIFIC_DAYS":
            values = schedule_data.get('VALUES', [])
            day_map = {
                "SUN": 0, "MON": 1, "TUE": 2, "WED": 3,
                "THU": 4, "FRI": 5, "SAT": 6
            }
            days_of_week = ",".join(str(day_map[day.upper()]) for day in values if day.upper() in day_map)
            cron_schedule = crontab(minute=0, hour=0, day_of_week=days_of_week)

        elif schedule_type == "MONTHLY_SPECIFIC_DATES":
            values = schedule_data.get('VALUES', [])
            dates_of_month = ",".join(values)
            cron_schedule = crontab(minute=0, hour=0, day_of_month=dates_of_month)

        elif schedule_type == "ONCE":
            one_time_date = schedule_data.get('VALUES')
            dt = datetime.strptime(one_time_date, "%Y-%m-%d %H:%M")
            cron_schedule = crontab(minute=dt.minute, hour=dt.hour, day_of_month=dt.day, month_of_year=dt.month)

        elif schedule_type == "PERIODIC":
            frequency_type = schedule_data.get('FREQUENCY_TYPE', '').upper()
            frequency = schedule_data.get('FREQUENCY', 1)
            if frequency_type == 'MONTHS':
                schedule_minutes = frequency * 30 * 24 * 60
            elif frequency_type == 'WEEKS':
                schedule_minutes = frequency * 7 * 24 * 60
            elif frequency_type == 'DAYS':
                schedule_minutes = frequency * 24 * 60
            elif frequency_type == 'HOURS':
                schedule_minutes = frequency * 60
            elif frequency_type == 'MINUTES':
                schedule_minutes = frequency
            else:
                return make_response(jsonify({'error': f'Invalid frequency type: {frequency_type}'}), 400)

        else:
            return make_response(jsonify({'error': f'Cannot reschedule type: {schedule_type}'}), 400)
        
        executor = DefAsyncTask.query.filter_by(task_name=task_name).first()
        if not executor:
            return make_response(jsonify({'error': f'Executor not found for task {task_name}'}),404)
        # Restore schedule in Redis
        try:
            create_redbeat_schedule(
                schedule_name=redbeat_schedule_name,
                executor=executor.executor,     
                schedule_minutes=schedule_minutes,
                cron_schedule=cron_schedule,
                args=schedule.args,
                kwargs=schedule.kwargs,
                celery_app=celery
            )
            print(executor)
            
        except Exception as e:
            return make_response(jsonify({'error': 'Failed to recreate RedBeat schedule', 'details': str(e)}), 500)

        # Update DB
        schedule.cancelled_yn = 'N'
        schedule.last_updated_by = get_jwt_identity()
        schedule.last_update_date = datetime.utcnow()
        db.session.commit()

        return make_response(jsonify({'message': f"Schedule '{redbeat_schedule_name}' has been rescheduled."}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'error': 'Failed to reschedule task', 'details': str(e)}), 500)



@tasks_bp.route('/Cancel_AdHoc_Task/<string:task_name>/<string:user_schedule_name>/<string:schedule_id>/<string:task_id>', methods=['PUT'])
def Cancel_AdHoc_Task(task_name, user_schedule_name, schedule_id, task_id):
    """
    Cancels an ad-hoc task by updating the database and revoking the Celery task.

    Args:
        task_name (str): The name of the Celery task.
        user_schedule_name (str): The name of the user schedule.
        schedule_id (str): The database schedule ID.
        task_id (str): The Celery task ID.

    Returns:
        JSON response indicating success or failure.
    """
    try:
        # Find the task schedule by schedule_id and user_schedule_name
        schedule = DefAsyncTaskSchedule.query.filter_by(
            def_task_sche_id=schedule_id,
            user_schedule_name=user_schedule_name,
            task_name=task_name
        ).first()

        if schedule:
            # Update the cancelled_yn field to 'Y' (indicating cancellation)
            schedule.cancelled_yn = 'Y'

            # Commit the change to the database
            db.session.commit()

            # Now, revoke the Celery task
            try:
                celery.control.revoke(task_id, terminate=True)
                logging.info(f"Ad-hoc task with ID '{task_id}' revoked successfully.")
            except Exception as e:
                db.session.rollback()
                return make_response(jsonify({
                    "message": "Task schedule cancelled, but failed to revoke Celery task.",
                    "error": str(e)
                }), 500)

            # Return success message if both operations are successful
            return make_response(jsonify({
                "message": f"Ad-hoc task for schedule_id {schedule_id} has been successfully cancelled and revoked."
            }), 200)

        # If no schedule was found
        return make_response(jsonify({
            "message": f"No ad-hoc task found for schedule_id {schedule_id} and user_schedule_name {user_schedule_name}."
        }), 404)

    except Exception as e:
        db.session.rollback()
        logging.error(f"Error cancelling ad-hoc task: {str(e)}")
        return make_response(jsonify({
            "message": "Error cancelling ad-hoc task.",
            "error": str(e)
        }), 500)

    finally:
        db.session.close()


@tasks_bp.route('/view_requests_v1', methods=['GET'])
@jwt_required()
def get_all_tasks():
    try:
        fourteen_days = datetime.utcnow() - timedelta(days=1)
        tasks = DefAsyncTaskRequest.query.filter(DefAsyncTaskRequest.creation_date >= fourteen_days).order_by(DefAsyncTaskRequest.creation_date.desc())
        #tasks = DefAsyncTaskRequest.query.limit(100000).all()
        if not tasks:
            return jsonify({"message": "No tasks found"}), 404
        return jsonify([task.json() for task in tasks]), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



#def_async_task_requests
@tasks_bp.route('/view_requests/<int:page>/<int:page_limit>', methods=['GET'])
@jwt_required()
def view_requests(page, page_limit):
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=14)

        query = DefAsyncTaskRequest.query.filter(
            DefAsyncTaskRequest.creation_date >= cutoff_date
        ).order_by(DefAsyncTaskRequest.creation_date.desc())

        paginated = query.paginate(page=page, per_page=page_limit, error_out=False)

        if not paginated.items:
            return jsonify({"message": "No tasks found"}), 404

        return jsonify({
            "items": [task.json() for task in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@tasks_bp.route('/view_requests/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def def_async_task_requests_view_requests(page, limit):
    try:
        search_query = request.args.get('task_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        day_limit = datetime.utcnow() - timedelta(days=30)
        query = DefAsyncTaskRequest.query.filter(DefAsyncTaskRequest.creation_date >= day_limit)

        if search_query:
            query = query.filter(or_(
                DefAsyncTaskRequest.task_name.ilike(f'%{search_query}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_underscore}%'),
                DefAsyncTaskRequest.task_name.ilike(f'%{search_space}%')
            ))

        paginated = query.order_by(DefAsyncTaskRequest.creation_date.desc()) \
                         .paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [req.json() for req in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error fetching view requests", "error": str(e)}), 500)


