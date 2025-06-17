#app.py
import os 
import time
import re
import json
import uuid
import requests
import traceback
import logging
import time
import re
from redis import Redis
from zoneinfo import ZoneInfo
from itertools import count
from functools import wraps
from flask_cors import CORS 
from dotenv import load_dotenv            # To load environment variables from a .env file
from celery.schedules import crontab
from celery.result import AsyncResult      # For checking the status of tasks
from redbeat import RedBeatSchedulerEntry
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine, Text, desc, cast, TIMESTAMP, func, or_
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify, make_response       # Flask utilities for handling requests and responses
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from werkzeug.security import generate_password_hash, check_password_hash
from executors import flask_app # Import Flask app and tasks
from executors.extensions import db
from celery import current_app as celery  # Access the current Celery app
from executors.models import (
    DefAsyncTask,
    DefAsyncTaskParam,
    DefAsyncTaskSchedule,
    DefAsyncTaskRequest,
    DefAsyncTaskSchedulesV,
    DefAsyncExecutionMethods,
    DefAsyncTaskScheduleNew,
    DefTenant,
    DefUser,
    DefPerson,
    DefUserCredential,
    DefAccessProfile,
    DefUsersView,
    Message,
    DefTenantEnterpriseSetup,
    DefTenantEnterpriseSetupV,
    DefAccessModel,
    DefAccessModelLogic,
    DefAccessModelLogicAttribute,

    DefAccessPointElement,
    DefDataSource,
)
from redbeat_s.red_functions import create_redbeat_schedule, update_redbeat_schedule, delete_schedule_from_redis
from ad_hoc.ad_hoc_functions import execute_ad_hoc_task, execute_ad_hoc_task_v1
from config import redis_url


redis_client = Redis.from_url(redis_url, decode_responses=True)

jwt = JWTManager(flask_app)

# Set up the logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



# def generate_tenant_id():
#     try:
#         # Query the max tenant_id from the arc_tenants table
#         max_tenant_id = db.session.query(db.func.max(DefTenantEnterpriseSetup.tenant_id)).scalar()
#         if max_tenant_id is not None:
#             # If max_tenant_id is not None, set the start value for the counter
#             tenant_id_counter = count(start=max_tenant_id + 1)
#         else:
#             # If max_tenant_id is None, set a default start value based on the current timestamp
#             tenant_id_counter = count(start=int(datetime.timestamp(datetime.utcnow())))

#         # Generate a unique tenant_id using the counter
#         return next(tenant_id_counter)
#     except Exception as e:
#         # Handle specific exceptions as needed
#         print(f"Error generating tenant ID: {e}")
#         return None



# def generate_tenant_enterprise_id():
#     try:
#         # Query the max tenant_id from the arc_tenants table
#         max_tenant_id = db.session.query(db.func.max(DefTenantEnterpriseSetup.tenant_id)).scalar()
#         if max_tenant_id is not None:
#             # If max_tenant_id is not None, set the start value for the counter
#             tenant_id_counter = count(start=max_tenant_id + 1)
#         else:
#             # If max_tenant_id is None, set a default start value based on the current timestamp
#             tenant_id_counter = count(start=int(datetime.timestamp(datetime.utcnow())))

#         # Generate a unique tenant_id using the counter
#         return next(tenant_id_counter)
#     except Exception as e:
#         # Handle specific exceptions as needed
#         print(f"Error generating tenant ID: {e}")
#         return None





 
    
# @flask_app.route('/users', methods=['POST'])
# #@jwt_required()
# #@role_required('/users', 'POST')
# def create_user():
#     try:
#         data = request.get_json()
#         user_id         = generate_user_id()
#         user_name       = data['user_name']
#         user_type       = data['user_type']
#         email_addresses = data['email_addresses']
#         first_name      = data['first_name']
#         middle_name     = data['middle_name']
#         last_name       = data['last_name']
#         job_title       = data['job_title']
#         # Get user information from the JWT token
#         #created_by      = get_jwt_identity()
#         #last_updated_by = get_jwt_identity()
#         created_by      = data['created_by']
#         last_updated_by = data['last_updated_by']
#         # created_on      = data['created_on']
#         # last_updated_on = data['last_updated_on']
#         tenant_id       = data['tenant_id']
#         password        = data['password']
#         # privilege_names = data.get('privilege_names', [])
#         # role_names      = data.get('role_names', [])

#         existing_user  = DefUser.query.filter(DefUser.user_name == user_name).first()
#         # existing_user = ArcPerson.query.filter((ArcPerson.user_id == user_id) | (ArcPerson.username == username)).first()
#         existing_email = DefUser.query.filter(DefUser.email_addresses == email_addresses).first()

#         if existing_user:
#             if existing_user.user_name == user_name:
#                 return make_response(jsonify({"message": "Username already exists"}), 400)
        
#         if existing_email:
#             return make_response(jsonify({"message": "Email address already exists"}), 409)
            
#         hashed_password = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)
        
#         #current_timestamp = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
#         # Create a user record for the new user
#         def_user = DefUser(
#             user_id         = user_id,
#             user_name       = user_name,
#             user_type       = user_type,
#             email_addresses = email_addresses,
#             created_by      = created_by,
#             created_on      = current_timestamp(),
#             last_updated_by = last_updated_by,
#             last_updated_on = current_timestamp(),
#             tenant_id       = tenant_id
            
#         )

#         # Add the person record for the new user
#         db.session.add(def_user)
#         db.session.commit()

#         # Check if user_type is "person" before creating ArcPerson record
#         if user_type.lower() == "person":
#             def_person_data = {
#                 "user_id"    : user_id,
#                 "first_name" : first_name,
#                 "middle_name": middle_name,
#                 "last_name"  : last_name,
#                 "job_title"  : job_title
#             }

#             def_person_response = requests.post("http://localhost:5000/defpersons", json=def_person_data)

#             # Check the response from create_arc_user API
#             if def_person_response.status_code != 201:
#                 return jsonify({"message": "Def person's record creation failed"}), 500

    
#         # Create user credentials
#         user_credentials_data = {
#             "user_id" : user_id,
#             "password": hashed_password
#         }
#         user_credentials_response = requests.post("http://localhost:5000/def_user_credentials", json=user_credentials_data)

#         if user_credentials_response.status_code != 201:
#             # Handle the case where user credentials creation failed
#             return make_response(jsonify({"message": "User credentials creation failed"}), 500)


#     #    # Assign privileges to the user
#     #     user_privileges_data = {
#     #         "user_id": user_id,
#     #         "privilege_names": privilege_names
#     #     }
#     #     user_privileges_response = requests.post("http://localhost:5000/user_granted_privileges", json=user_privileges_data)

#     #     if user_privileges_response.status_code != 201:
#     #         return jsonify({'message': 'User privileges assignment failed'}), 500

#     #     # Assign roles to the user
#     #     user_roles_data = {
#     #         "user_id": user_id,
#     #         "role_names": role_names
#     #     }
#     #     user_roles_response = requests.post("http://localhost:5000/user_granted_roles", json=user_roles_data)

#     #     if user_roles_response.status_code != 201:
#     #         return jsonify({'message': 'User roles assignment failed'}), 500

#     #     db.session.commit()

#         return jsonify({"message": "User created successfully"}), 201
    
#     except IntegrityError as e:
#         return jsonify({"message": "User id already exists"}), 400  # 400 Bad Request for unique constraint violation
   
#     except Exception as e:
#         return jsonify({"message": str(e)}), 500
#         traceback.print_exc()




# @flask_app.route('/users/<int:user_id>', methods=['DELETE'])
# def delete_specific_user(user_id):
#     try:
#         user = DefUser.query.filter_by(user_id=user_id).first()
#         if not user:
#             return make_response(jsonify({'message': 'User not found'}), 404)

#         # Delete DefPerson if user_type is "person"
#         if user.user_type and user.user_type.lower() == "person":
#             person = DefPerson.query.filter_by(user_id=user_id).first()
#             if person:
#                 db.session.delete(person)

#         # Delete DefUserCredential if exists
#         user_credential = DefUserCredential.query.filter_by(user_id=user_id).first()
#         if user_credential:
#             db.session.delete(user_credential)

#         # Delete DefAccessProfile(s) if exist
#         access_profiles = DefAccessProfile.query.filter_by(user_id=user_id).all()
#         for profile in access_profiles:
#             db.session.delete(profile)

#         # Delete the DefUser record
#         db.session.delete(user)
#         db.session.commit()

#         return make_response(jsonify({'message': 'User and related records deleted successfully'}), 200)

#     except Exception as e:
#         db.session.rollback()
#         return make_response(jsonify({'message': 'Error deleting user', 'error': str(e)}), 500)
  




# @flask_app.route('/api/v1/Create_TaskSchedule', methods=['POST'])
# def Create_TaskSchedule_v1():
#     try:
#         user_schedule_name = request.json.get('user_schedule_name', 'Immediate')
#         task_name = request.json.get('task_name')
#         parameters = request.json.get('parameters', {})
#         schedule_type = request.json.get('schedule_type')
#         schedule_data = request.json.get('schedule', {})

#         if not task_name:
#             return jsonify({'error': 'Task name is required'}), 400

#         # Fetch task details from the database
#         task = DefAsyncTask.query.filter_by(task_name=task_name).first()
#         if not task:
#             return jsonify({'error': f'No task found with task_name: {task_name}'}), 400

#         user_task_name = task.user_task_name
#         executor = task.executor
#         script_name = task.script_name

#         schedule_name = str(uuid.uuid4())
#         redbeat_schedule_name = f"{user_schedule_name}_{schedule_name}"
#         args = [script_name, user_task_name, task_name, user_schedule_name, redbeat_schedule_name, schedule_data]
#         kwargs = {}

#         # Validate task parameters
#         task_params = DefAsyncTaskParam.query.filter_by(task_name=task_name).all()
#         for param in task_params:
#             param_name = param.parameter_name
#             if param_name in parameters:
#                 kwargs[param_name] = parameters[param_name]
#             else:
#                 return jsonify({'error': f'Missing value for parameter: {param_name}'}), 400

#         # Handle scheduling based on schedule type
#         cron_schedule = None
#         schedule_minutes = None

#         if schedule_type == "WEEKLY_SPECIFIC_DAYS":
#             values = schedule_data.get('VALUES', [])  # e.g., ["Monday", "Wednesday"]
#             day_map = {
#                 "SUN": 0, "MON": 1, "TUE": 2, "WED": 3,
#                 "THU": 4, "FRI": 5, "SAT": 6
#             }
#             days_of_week = ",".join(str(day_map[day.upper()]) for day in values if day.upper() in day_map)
#             cron_schedule = crontab(minute=0, hour=0, day_of_week=days_of_week)

#         elif schedule_type == "MONTHLY_SPECIFIC_DATES":
#             values = schedule_data.get('VALUES', [])  # e.g., ["5", "15"]
#             dates_of_month = ",".join(values)
#             cron_schedule = crontab(minute=0, hour=0, day_of_month=dates_of_month)

#         elif schedule_type == "ONCE":
#             one_time_date = schedule_data.get('VALUES')  # e.g., {"date": "2025-03-01 14:30"}
#             if not one_time_date:
#                 return jsonify({'error': 'Date is required for one-time execution'}), 400
#             dt = datetime.strptime(one_time_date, "%Y-%m-%d %H:%M")
#             cron_schedule = crontab(minute=dt.minute, hour=dt.hour, day_of_month=dt.day, month_of_year=dt.month)

#         elif schedule_type == "PERIODIC":
#             frequency_type = schedule_data.get('FREQUENCY_TYPE', 'minutes').lower()
#             frequency = schedule_data.get('FREQUENCY', 1)

#             if frequency_type == 'month':
#                 schedule_minutes = frequency * 30 * 24 * 60
#             elif frequency_type == 'day':
#                 schedule_minutes = frequency * 24 * 60
#             elif frequency_type == 'hour':
#                 schedule_minutes = frequency * 60
#             else:
#                 schedule_minutes = frequency  # Default to minutes

#         # Handle Ad-hoc Requests
#         elif schedule_type == "IMMEDIATE":
#             try:
#                 result = execute_ad_hoc_task_v1(
#                     user_schedule_name=user_schedule_name,
#                     executor=executor,
#                     task_name=task_name,
#                     args=args,
#                     kwargs=kwargs,
#                     schedule_type=schedule_type,
#                     cancelled_yn='N',
#                     created_by=101
#                 )
#                 return jsonify(result), 201
#             except Exception as e:
#                 return jsonify({"error": "Failed to execute ad-hoc task", "details": str(e)}), 500

#         else:
#             return jsonify({'error': 'Invalid schedule type'}), 400
#         # Handle Scheduled Tasks
#         try:
#             create_redbeat_schedule(
#                 schedule_name=redbeat_schedule_name,
#                 executor=executor,
#                 schedule_minutes=schedule_minutes if schedule_minutes else None,
#                 cron_schedule=cron_schedule if cron_schedule else None,
#                 args=args,
#                 kwargs=kwargs,
#                 celery_app=celery
#             )
#         except Exception as e:
#             return jsonify({"error": "Failed to create RedBeat schedule", "details": str(e)}), 500

#         # Store schedule in DB
#         new_schedule = DefAsyncTaskScheduleNew(
#             user_schedule_name=user_schedule_name,
#             redbeat_schedule_name=redbeat_schedule_name,
#             task_name=task_name,
#             args=args,
#             kwargs=kwargs,
#             parameters=kwargs,
#             schedule_type=schedule_type,
#             schedule=schedule_data,
#             ready_for_redbeat="N",
#             cancelled_yn='N',
#             created_by=101
#         )

#         db.session.add(new_schedule)
#         db.session.commit()

#         return jsonify({
#             "message": "Task schedule created successfully!",
#             "schedule_id": new_schedule.def_task_sche_id
#         }), 201

#     except Exception as e:
#         db.session.rollback()
#         return jsonify({"error": "Failed to create task schedule", "details": str(e)}), 500


# @flask_app.route('/Update_TaskSchedule/<string:task_name>', methods=['PUT'])
# def Update_TaskSchedule(task_name):

#     try:
#         # Retrieve redbeat_schedule_name from request payload
#         redbeat_schedule_name = request.json.get('redbeat_schedule_name')
#         if not redbeat_schedule_name:
#             return make_response(jsonify({"message": "redbeat_schedule_name is required in the payload"}), 400)

#         # Retrieve the schedule from the database
#         schedule = DefAsyncTaskScheduleNew.query.filter_by(
#             task_name=task_name, redbeat_schedule_name=redbeat_schedule_name
#         ).first()

#         # Check if schedule exists
#         if not schedule:
#             return make_response(jsonify({"message": f"Task Periodic Schedule for {redbeat_schedule_name} not found"}), 404)

#         # Check if ready_for_redbeat is 'N' (allow updates only if it's 'N')
#         if schedule.ready_for_redbeat != 'N':
#             return make_response(jsonify({
#                 "message": f"Task Periodic Schedule for {redbeat_schedule_name} is not marked as 'N'. Update is not allowed."
#             }), 400)

#         # Update database fields based on the request data
#         if 'parameters' in request.json:
#             schedule.parameters = request.json.get('parameters')
#             schedule.kwargs = request.json.get('parameters')
#         if 'schedule_type' in request.json:
#             schedule.schedule_type = request.json.get('schedule_type')
#         if 'schedule' in request.json:
#             schedule.schedule = request.json.get('schedule')

#         schedule.last_updated_by = 102  # Static user ID

#         # Commit changes to the database
#         db.session.commit()

#         return make_response(jsonify({
#             "message": f"Task Periodic Schedule for {redbeat_schedule_name} updated successfully in the database"
#         }), 200)

#     except Exception as e:
#         db.session.rollback()  # Rollback in case of an error
#         return make_response(jsonify({"message": "Error updating Task Periodic Schedule", "error": str(e)}), 500)


# @flask_app.route('/view_requests/<int:page>/<int:page_limit>', methods=['GET'])
# # @jwt_required()
# def view_requests(page, page_limit):
#     try:
#         fourteen_days = datetime.utcnow() - timedelta(days=14)
#         # Filter by date
#         query = DefAsyncTaskRequest.query.filter(
#             DefAsyncTaskRequest.creation_date >= fourteen_days
#         )
        
#         # Total number of matching tasks
#         total = query.count()
#         total_pages = (total + page_limit - 1) // page_limit

#         # Paginate using offset and limit
#         requests = query.order_by(DefAsyncTaskRequest.creation_date.desc())\
#                      .offset((page - 1) * page_limit).limit(page_limit).all()

#         if not requests:
#             return jsonify({"message": "No tasks found"}), 404

#         return make_response(jsonify({
#             "items": [req.json() for req in requests],
#             "total": requests.total,
#             "pages": requests.pages,
#             "page": requests.page
#         }), 200)

#     except Exception as e:
#         return jsonify({"error": str(e)}), 500







































if __name__ == "__main__":
    flask_app.run(debug=True)
