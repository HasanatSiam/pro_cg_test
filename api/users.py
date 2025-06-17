from flask import Blueprint, request, jsonify, make_response
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity
from itertools import count

from datetime import datetime
from sqlalchemy import func, or_
from werkzeug.security import generate_password_hash, check_password_hash

from executors.extensions import db
from executors.models import (
    DefUser,
    DefPerson,
    DefUsersView,
    DefUserCredential
)

users_bp = Blueprint('users', __name__)

def current_timestamp():
    return datetime.now().strftime('%d-%m-%Y %H:%M:%S')




def generate_user_id():
    try:
        # Query the max user_id from the arc_users table
        max_user_id = db.session.query(db.func.max(DefUser.user_id)).scalar()
        if max_user_id is not None:
            # If max_user_id is not None, set the start value for the counter
            user_id_counter = count(start=max_user_id + 1)
        else:
            # If max_user_id is None, set a default start value
            user_id_counter = count(start=int(datetime.timestamp(datetime.utcnow())))
        
        # Generate a unique user_id using the counter
        return next(user_id_counter)
    except Exception as e:
        print(f"Error generating user ID: {e}")
        return None




@users_bp.route('/defusers', methods=['POST'])
def create_def_user():
    try:
        # Parse data from the request body
        data = request.get_json()
        user_id         = generate_user_id()
        user_name       = data['user_name']
        user_type       = data['user_type']
        email_addresses = data['email_addresses']
        created_by      = data['created_by']
        created_on      = current_timestamp()
        last_updated_by = data['last_updated_by']
        last_updated_on = current_timestamp()
        tenant_id       = data['tenant_id']
        profile_picture = data.get('profile_picture') or {
            "original": "uploads/profiles/default/profile.jpg",
            "thumbnail": "uploads/profiles/default/thumbnail.jpg"
        }
        

       # Convert the list of email addresses to a JSON-formatted string
       # email_addresses_json = json.dumps(email_addresses)  # Corrected variable name

       # Create a new ArcUser object
        new_user = DefUser(
          user_id         = user_id,
          user_name       = user_name,
          user_type       = user_type,
          email_addresses = email_addresses,  # Corrected variable name
          created_by      = created_by,
          created_on      = created_on,
          last_updated_by = last_updated_by,
          last_updated_on = last_updated_on,
          tenant_id       = tenant_id,
          profile_picture = profile_picture
        )
        # Add the new user to the database session
        db.session.add(new_user)
        # Commit the changes to the database
        db.session.commit()

        # Return a success response
        return make_response(jsonify({"message": "Def USER created successfully!",
                                       "User Id": user_id}), 201)

    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    

    
@users_bp.route('/defusers', methods=['GET'])
def get_users():
    try:
        users = DefUser.query.all()
        return make_response(jsonify([user.json() for user in users]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'error getting users', 'error': str(e)}), 500)
    

@users_bp.route('/defusers/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_def_users(page, limit):
    try:
        query = DefUser.query.order_by(DefUser.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting paginated users', 'error': str(e)}), 500)



@users_bp.route('/defusers/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_users(page, limit):
    try:
        search_query = request.args.get('user_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefUser.query

        if search_query:
            query = query.filter(
                or_(
                    DefUser.user_name.ilike(f'%{search_query}%'),
                    DefUser.user_name.ilike(f'%{search_underscore}%'),
                    DefUser.user_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefUser.user_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching users", "error": str(e)}), 500)


# get a user by id
@users_bp.route('/defusers/<int:user_id>', methods=['GET'])
def get_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            return make_response(jsonify({'user': user.json()}), 200)
        return make_response(jsonify({'message': 'user not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'error getting user', 'error': str(e)}), 500)
    
    
@users_bp.route('/defusers/<int:user_id>', methods=['PUT'])
def update_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            data = request.get_json()
            if 'user_name' in data:
                user.user_name = data['user_name']
            if 'email_addresses' in data:
                user.email_addresses = data['email_addresses']
            if 'last_updated_by' in data:
                user.last_updated_by = data['last_updated_by']
            user.last_updated_on = current_timestamp()
            db.session.commit()
            return make_response(jsonify({'message': 'user updated'}), 200)
        return make_response(jsonify({'message': 'user not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'error updating user', 'error': str(e)}), 500)
    
    
    
@users_bp.route('/defusers/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    try:
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            db.session.delete(user)
            db.session.commit()
            return make_response(jsonify({'message': 'User deleted successfully'}), 200)
        return make_response(jsonify({'message': 'user not found'}), 404)
    except:
        return make_response(jsonify({'message': 'error deleting user'}), 500)



@users_bp.route('/def_combined_user/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_combined_users(page, limit):
    try:
        query = DefUsersView.query.order_by(DefUsersView.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching combined users', 'error': str(e)}), 500)
        

@users_bp.route('/def_combined_user/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_combined_users(page, limit):
    user_name = request.args.get('user_name', '').strip()
    try:
        query = DefUsersView.query
        if user_name:
            query = query.filter(DefUsersView.user_name.ilike(f'%{user_name}%'))
        query = query.order_by(DefUsersView.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [user.json() for user in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error searching combined users', 'error': str(e)}), 500)


@users_bp.route('/defpersons', methods=['POST'])
def create_arc_person():
    try:
        data = request.get_json()
        user_id     = data['user_id']
        first_name  = data['first_name']
        middle_name = data['middle_name']
        last_name   = data['last_name']
        job_title   = data['job_title']  
        
        # create arc persons object 
        person =  DefPerson(
            user_id     = user_id,
            first_name  = first_name,
            middle_name = middle_name,
            last_name   = last_name,
            job_title   = job_title
        ) 
        
        # Add arc persons data to the database session
        db.session.add(person)
        # Commit the changes to the database
        db.session.commit()
        # Return a success response
        return make_response(jsonify({"message": "Def person's data created succesfully"}), 201)
    
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    
    
@users_bp.route('/defpersons', methods=['GET'])
def get_persons():
    try:
        persons = DefPerson.query.all()
        return make_response(jsonify([person.json() for person in persons]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'error getting persons', 'error': str(e)}), 500)
    



@users_bp.route('/defpersons/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_persons(page, limit):
    try:
        query = DefPerson.query.order_by(DefPerson.user_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [person.json() for person in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting paginated persons', 'error': str(e)}), 500)

@users_bp.route('/defpersons/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_persons(page, limit):
    try:
        search_query = request.args.get('name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefPerson.query

        if search_query:
            query = query.filter(
                or_(
                    func.lower(DefPerson.first_name).ilike(f'%{search_query}%'),
                    func.lower(DefPerson.first_name).ilike(f'%{search_underscore}%'),
                    func.lower(DefPerson.first_name).ilike(f'%{search_space}%'),
                    func.lower(DefPerson.last_name).ilike(f'%{search_query}%'),
                    func.lower(DefPerson.last_name).ilike(f'%{search_underscore}%'),
                    func.lower(DefPerson.last_name).ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefPerson.user_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [person.json() for person in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching persons", "error": str(e)}), 500)


@users_bp.route('/defpersons/<int:user_id>', methods=['GET'])
def get_person(user_id):
    try:
        person = DefPerson.query.filter_by(user_id=user_id).first()
        if person:
            return make_response(jsonify({'person': person.json()}), 200)
        return make_response(jsonify({'message': 'Person not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting person', 'error': str(e)}), 500) 


@users_bp.route('/defpersons/<int:user_id>', methods=['PUT'])
def update_person(user_id):
    try:
        person = DefPerson.query.filter_by(user_id=user_id).first()
        if person:
            data = request.get_json()
            # Update only the fields provided in the JSON data
            if 'first_name' in data:
                person.first_name = data['first_name']
            if 'middle_name' in data:
                person.middle_name = data['middle_name']
            if 'last_name' in data:
                person.last_name = data['last_name']
            if 'job_title' in data:
                person.job_title = data['job_title']
            db.session.commit()
            return make_response(jsonify({'message': 'person updated'}), 200)
        return make_response(jsonify({'message': 'person not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'error updating person', 'error': str(e)}), 500)
    
    
@users_bp.route('/defpersons/<int:user_id>', methods=['DELETE'])
def delete_person(user_id):
    try:
        person = DefPerson.query.filter_by(user_id=user_id).first()
        if person:
            db.session.delete(person)
            db.session.commit()
            return make_response(jsonify({'message': 'Person deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Person not found'}), 404)
    except:
        return make_response(jsonify({'message': 'Error deleting user'}), 500)


    
@users_bp.route('/def_user_credentials', methods=['POST'])
def create_user_credential():
    try:
        # Parse data from the request body
        data = request.get_json()
        user_id  = data['user_id']
        password = data['password']

        # Create a new DefUserCredentials object
        credential = DefUserCredential(
            user_id  = user_id,
            password = password
        )

        # Add the new credentials to the database session
        db.session.add(credential)
        # Commit the changes to the database
        db.session.commit()

        # Return a success response
        return make_response(jsonify({"message": "User credentials created successfully!"}), 201)

    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    


    
    
@users_bp.route('/reset_user_password', methods=['PUT'])
#@jwt_required()
def reset_user_password():
    #current_user_id = get_jwt_identity()
    data = request.get_json()
    current_user_id = data['user_id']
    old_password = data['old_password']
    new_password = data['new_password']
    
    user = DefUserCredential.query.get(current_user_id)
    if not user:
        return jsonify({'message': 'User not found'}), 404
    
    if not check_password_hash(user.password, old_password):
        return jsonify({'message': 'Invalid old password'}), 401
    
    hashed_new_password = generate_password_hash(new_password, method='pbkdf2:sha256', salt_length=16)
    user.password = hashed_new_password
    
    db.session.commit()
    
    return jsonify({'message': 'Password reset successful'}), 200


@users_bp.route('/def_user_credentials/<int:user_id>', methods=['DELETE'])
def delete_user_credentials(user_id):
    try:
        credential = DefUserCredential.query.filter_by(user_id=user_id).first()
        if credential:
            db.session.delete(credential)
            db.session.commit()
            return make_response(jsonify({'message': 'User credentials deleted successfully'}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except:
        return make_response(jsonify({'message': 'Error deleting user credentials'}), 500)
 




@users_bp.route('/users', methods=['POST'])
def register_user():
    try:
        data = request.get_json()
        # Extract user fields
        user_id         = generate_user_id()
        user_name       = data['user_name']
        user_type       = data['user_type']
        email_addresses = data['email_addresses']
        created_by      = data['created_by']
        last_updated_by = data['last_updated_by']
        tenant_id       = data['tenant_id']
        # Extract person fields
        first_name      = data.get('first_name')
        middle_name     = data.get('middle_name')
        last_name       = data.get('last_name')
        job_title       = data.get('job_title')
        # Extract credentials
        password        = data['password']

        # Set default profile picture if not provided
        profile_picture = data.get('profile_picture') or {
            "original": "uploads/profiles/default/profile.jpg",
            "thumbnail": "uploads/profiles/default/thumbnail.jpg"
        }

        # Check for existing user/email
        if DefUser.query.filter_by(user_name=user_name).first():
            return jsonify({"message": "Username already exists"}), 409
        for email in email_addresses:
            if DefUser.query.filter(DefUser.email_addresses.contains    ([email])).first():
                return jsonify({"message": "Email already exists"}), 409

        # Create user
        new_user = DefUser(
            user_id         = user_id,
            user_name       = user_name,
            user_type       = user_type,
            email_addresses = email_addresses,
            created_by      = created_by,
            created_on      = current_timestamp(),
            last_updated_by = last_updated_by,
            last_updated_on = current_timestamp(),
            tenant_id       = tenant_id,
            profile_picture = profile_picture
        )
        db.session.add(new_user)

        # Create person if user_type is person
        if user_type.lower() == "person":
            new_person = DefPerson(
                user_id     = user_id,
                first_name  = first_name,
                middle_name = middle_name,
                last_name   = last_name,
                job_title   = job_title
            )
            db.session.add(new_person)

        # Create credentials
        hashed_password = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)
        new_cred = DefUserCredential(
            user_id  = user_id,
            password = hashed_password
        )
        db.session.add(new_cred)

        db.session.commit()
        return jsonify({"message": "User registered successfully", "user_id": user_id}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Registration failed", "error": str(e)}), 500

@users_bp.route('/users', methods=['GET'])
# @jwt_required()
def defusers():
    try:
        defusers = DefUsersView.query.order_by(DefUsersView.user_id.desc()).all()
        return make_response(jsonify([defuser.json() for defuser in defusers]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting users', 'error': str(e)}), 500)
    
    
@users_bp.route('/users/<int:user_id>', methods=['GET'])
# @jwt_required()
def get_specific_user(user_id):
    try:
        user = DefUsersView.query.filter_by(user_id=user_id).first()
        if user:
            return make_response(jsonify({'user': user.json()}), 200)
        return make_response(jsonify({'message': 'User not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error getting User', 'error': str(e)}), 500)  
    

@users_bp.route('/users/<int:user_id>', methods=['PUT'])
def update_specific_user(user_id):
    try:
        data = request.get_json()
        if not data:
            return make_response(jsonify({'message': 'No input data provided'}), 400)

        user = DefUser.query.filter_by(user_id=user_id).first()
        if not user:
            return make_response(jsonify({'message': 'User not found'}), 404)

        # Update DefUser fields
        user.user_name = data.get('user_name', user.user_name)
        user.email_addresses = data.get('email_addresses', user.email_addresses)
        user.last_updated_on = current_timestamp()

        # Update DefPerson fields if user_type is "person"
        if user.user_type and user.user_type.lower() == "person":
            person = DefPerson.query.filter_by(user_id=user_id).first()
            if not person:
                return make_response(jsonify({'message': 'Person not found'}), 404)

            person.first_name = data.get('first_name', person.first_name)
            person.middle_name = data.get('middle_name', person.middle_name)
            person.last_name = data.get('last_name', person.last_name)
            person.job_title = data.get('job_title', person.job_title)

        # Password update logic
        password = data.get('password')
        if password:
            user_cred = DefUserCredential.query.filter_by(user_id=user_id).first()
            if not user_cred:
                return make_response(jsonify({'message': 'User credentials not found'}), 404)

            user_cred.password = generate_password_hash(password, method='pbkdf2:sha256', salt_length=16)

        db.session.commit()
        return make_response(jsonify({'message': 'User updated successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error updating user', 'error': str(e)}), 500)



@users_bp.route('/users/<int:user_id>', methods=['DELETE'])
def delete_specific_user(user_id):
    try:
        # Find the user record in the DefUser table
        user = DefUser.query.filter_by(user_id=user_id).first()
        if user:
            # Delete the DefPerson record if it exists and user_type is "person"
            if user.user_type.lower() == "person":
                person = DefPerson.query.filter_by(user_id=user_id).first()
                if person:
                    db.session.delete(person)

            # Delete the DefUserCredential record if it exists
            user_credential = DefUserCredential.query.filter_by(user_id=user_id).first()
            if user_credential:
                db.session.delete(user_credential)

            # Delete the DefUser record
            db.session.delete(user)
            db.session.commit()

            return make_response(jsonify({'message': 'User and related records deleted successfully'}), 200)

        return make_response(jsonify({'message': 'User not found'}), 404)
    
    except Exception as e:
        db.session.rollback()  # Rollback in case of any error
        return make_response(jsonify({'message': 'Error deleting user', 'error': str(e)}), 500)




@users_bp.route('/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        email_or_username = data['email_or_username']
        password          = data['password']
        
        if not email_or_username or not password:
            return make_response(jsonify({"message": "Invalid request. Please provide both email/username and password."}), 400)

        # Set a default value for user_profile
        user_profile = None

        # Check if the input is an email address or a username
        # if '@' in email_or_username:
        # # Cast JSON column to TEXT and use LIKE
        #     user_profile = DefUser.query.filter(cast(DefUser.email_addresses, Text).ilike(f"%{email_or_username}%")).first()
        # else:
        #     user_profile = DefUser.query.filter_by(user_name = email_or_username).first()

        # Use JSONB contains for email lookup
        if '@' in email_or_username:
            user_profile = DefUser.query.filter(
                DefUser.email_addresses.contains([email_or_username])
            ).first()
        else:
            user_profile = DefUser.query.filter_by(user_name=email_or_username).first()



        if user_profile and user_profile.user_id:
            user_credentials = DefUserCredential.query.filter_by(user_id = user_profile.user_id ).first()

            if user_credentials and check_password_hash(user_credentials.password, password):
                access_token = create_access_token(
                    identity = str(user_profile.user_id),
                    additional_claims={"username": user_profile.user_name}
                )
                return make_response(jsonify({"access_token": access_token}), 200)
            else:
                return make_response(jsonify({"message": "Invalid email/username or password"}), 401)
        else:
            return make_response(jsonify({"message": "User not found"}), 404)

    except Exception as e:
        return make_response(jsonify({"message": str(e)}), 500)

