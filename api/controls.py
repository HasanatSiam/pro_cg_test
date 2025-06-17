from flask import Blueprint, request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity, get_jwt
from sqlalchemy import or_
from datetime import datetime

from executors.extensions import db
from executors.models import DefControl



controls_bp = Blueprint('controls', __name__)

@controls_bp.route('/def_controls', methods=['GET'])
@jwt_required()
def get_all_controls():
    try:
        controls = DefControl.query.order_by(DefControl.def_control_id.desc()).all()
        return make_response(jsonify([c.json() for c in controls]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching controls', 'error': str(e)}), 500)


@controls_bp.route('/def_controls/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_controls(page, limit):
    try:
        paginated = DefControl.query.order_by(DefControl.def_control_id.desc()).paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            'items': [control.json() for control in paginated.items],
            'total': paginated.total,
            'pages': paginated.pages,
            'page': paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching paginated controls', 'error': str(e)}), 500)


@controls_bp.route('/def_controls/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_controls(page, limit):
    try:
        search_query = request.args.get('control_name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefControl.query

        if search_query:
            query = query.filter(
                or_(
                    DefControl.control_name.ilike(f'%{search_query}%'),
                    DefControl.control_name.ilike(f'%{search_underscore}%'),
                    DefControl.control_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefControl.def_control_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [control.json() for control in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error searching controls', 'error': str(e)}), 500)


@controls_bp.route('/def_controls/<int:control_id>', methods=['GET'])
@jwt_required()
def get_control_by_id(control_id):
    try:
        control = DefControl.query.filter_by(def_control_id=control_id).first()
        if control:
            return make_response(jsonify(control.json()), 200)
        return make_response(jsonify({'message': 'Control not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching control', 'error': str(e)}), 500)


@controls_bp.route('/def_controls', methods=['POST'])
@jwt_required()
def create_control():
    claims = get_jwt()
    username = claims.get("username")
    try:
        new_control = DefControl(
            control_name=request.json.get('control_name'),
            description=request.json.get('description'),
            pending_results_count=request.json.get('pending_results_count'),
            control_type=request.json.get('control_type'),
            priority=request.json.get('priority'),
            datasources=request.json.get('datasources'),
            last_run_date=request.json.get('last_run_date'),
            last_updated_date=request.json.get('last_updated_date'),
            status=request.json.get('status'),
            state=request.json.get('state'),
            result_investigator=request.json.get('result_investigator'),
            authorized_data=request.json.get('authorized_data'),
            revision=0,
            revision_date=datetime.utcnow().date(),
            created_by=username,
            created_date=datetime.utcnow().date()
        )
        db.session.add(new_control)
        db.session.commit()
        return make_response(jsonify({'message': 'Control created successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': 'Error creating control', 'error': str(e)}), 500)


@controls_bp.route('/def_controls/<int:control_id>', methods=['PUT'])
@jwt_required()
def update_control(control_id):
    claims = get_jwt()
    username = claims.get("username")
    try:
        control = DefControl.query.filter_by(def_control_id=control_id).first()
        if control:
            control.control_name = request.json.get('control_name', control.control_name)
            control.description = request.json.get('description', control.description)
            control.pending_results_count = request.json.get('pending_results_count', control.pending_results_count)
            control.control_type = request.json.get('control_type', control.control_type)
            control.priority = request.json.get('priority', control.priority)
            control.datasources = request.json.get('datasources', control.datasources)
            control.last_run_date = request.json.get('last_run_date', control.last_run_date)
            control.last_updated_date = request.json.get('last_updated_date', control.last_updated_date)
            control.status = request.json.get('status', control.status)
            control.state = request.json.get('state', control.state)
            control.result_investigator = request.json.get('result_investigator', control.result_investigator)
            control.authorized_data = request.json.get('authorized_data', control.authorized_data)
            control.revision += 1
            control.revision_date = datetime.utcnow().date()
            control.created_by = username
            control.created_date = request.json.get('created_date', control.created_date)

            db.session.commit()
            return make_response(jsonify({'message': 'Control updated successfully'}), 200)
        return make_response(jsonify({'message': 'Control not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating control', 'error': str(e)}), 500)


@controls_bp.route('/def_controls/<int:control_id>', methods=['DELETE'])
@jwt_required()
def delete_control(control_id):
    try:
        control = DefControl.query.filter_by(def_control_id=control_id).first()
        if control:
            db.session.delete(control)
            db.session.commit()
            return make_response(jsonify({'message': 'Control deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Control not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting control', 'error': str(e)}), 500)
