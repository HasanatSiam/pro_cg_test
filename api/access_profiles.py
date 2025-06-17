
from flask import Blueprint, request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity, get_jwt


from sqlalchemy.exc import IntegrityError

from executors.extensions import db
from executors.models import DefAccessProfile


access_profiles_bp = Blueprint('access_profiles', __name__)

@access_profiles_bp.route('/access_profiles/<int:user_id>', methods=['POST'])
def create_access_profiles(user_id):
    try:
        profile_type = request.json.get('profile_type')  # Fixed incorrect key
        profile_id = request.json.get('profile_id')
        primary_yn = request.json.get('primary_yn', 'N')  # Default to 'N' if not provided

        if not profile_type or not profile_id:
            return make_response(jsonify({"message": "Missing required fields"}), 400)

        new_profile = DefAccessProfile(
            user_id=user_id,
            profile_type=profile_type,
            profile_id=profile_id,
            primary_yn=primary_yn
        )

        db.session.add(new_profile)  # Fixed: Corrected session operation
        db.session.commit()
        return make_response(jsonify({"message": "Access profiles created successfully"}), 201)

    except IntegrityError as e:
        db.session.rollback()  
        print("IntegrityError:", str(e))  
        return make_response(jsonify({"message": "Error creating Access Profiles", "error": str(e)}), 409)

    except Exception as e:
        db.session.rollback()  
        print("General Exception:", str(e))  
        return make_response(jsonify({"message": "Error creating Access Profiles", "error": str(e)}), 500)


# Get all access profiles
@access_profiles_bp.route('/access_profiles', methods=['GET'])
def get_users_access_profiles():
    try:
        profiles = DefAccessProfile.query.all()
        return make_response(jsonify([profile.json() for profile in profiles]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting Access Profiles", "error": str(e)}), 500)


@access_profiles_bp.route('/access_profiles/<int:user_id>', methods=['GET'])
def get_user_access_profiles_(user_id):
    try:
        profiles = DefAccessProfile.query.filter_by(user_id=user_id).all()
        
        if profiles:
            return make_response(jsonify([profile.json() for profile in profiles]), 200)
        else:
            return make_response(jsonify({"message": "Access Profiles not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving Access Profiles", "error": str(e)}), 500)


@access_profiles_bp.route('/access_profiles/<int:user_id>/<int:serial_number>', methods=['PUT'])
def update_access_profile(user_id, serial_number):
    try:
        # Retrieve the existing access profile
        profile = DefAccessProfile.query.filter_by(user_id=user_id, serial_number=serial_number).first()
        if not profile:
            return make_response(jsonify({"message": "Access Profile not found"}), 404)

        data = request.get_json()

        # Update fields in DefAccessProfile table
        if 'profile_type' in data:
            profile.profile_type = data['profile_type']
        if 'profile_id' in data:
            profile.profile_id = data['profile_id']
        if 'primary_yn' in data:
            profile.primary_yn = data['primary_yn']

        # Commit changes to DefAccessProfile
        db.session.commit()

        return make_response(jsonify({"message": "Access Profile updated successfully"}), 200)

    except Exception as e:
        db.session.rollback()  # Rollback on error
        return make_response(jsonify({"message": "Error updating Access Profile", "error": str(e)}), 500)


# Delete an access profile
@access_profiles_bp.route('/access_profiles/<int:user_id>/<int:serial_number>', methods=['DELETE'])
def delete_access_profile(user_id, serial_number):
    try:
        profile = DefAccessProfile.query.filter_by(user_id=user_id, serial_number=serial_number).first()
        if profile:
            db.session.delete(profile)
            db.session.commit()
            return make_response(jsonify({"message": "Access Profile deleted successfully"}), 200)
        return make_response(jsonify({"message": "Access Profile not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error deleting Access Profile", "error": str(e)}), 500)

