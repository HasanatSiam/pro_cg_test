from flask import Blueprint, request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy import or_
from datetime import datetime

from executors.extensions import db
from executors.models import DefAccessEntitlement

entitlements_bp = Blueprint('entitlements', __name__)
#def_access_entitlements
@entitlements_bp.route('/def_access_entitlements', methods=['GET'])
def get_all_entitlements():
    try:
        entitlements = DefAccessEntitlement.query.order_by(DefAccessEntitlement.def_entitlement_id.desc()).all()
        return make_response(jsonify([e.json() for e in entitlements]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching entitlements', 'error': str(e)}), 500)


@entitlements_bp.route('/def_access_entitlements/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_access_entitlements(page, limit):
    try:
        search_query = request.args.get('entitlement_name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAccessEntitlement.query

        if search_query:
            query = query.filter(
                or_(
                    DefAccessEntitlement.entitlement_name.ilike(f'%{search_query}%'),
                    DefAccessEntitlement.entitlement_name.ilike(f'%{search_underscore}%'),
                    DefAccessEntitlement.entitlement_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefAccessEntitlement.def_entitlement_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [e.json() for e in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error searching entitlements', 'error': str(e)}), 500)

@entitlements_bp.route('/def_access_entitlements/<int:page>/<int:limit>', methods=['GET'])
def get_paginated_entitlements(page, limit):
    try:
        paginated = DefAccessEntitlement.query.order_by(DefAccessEntitlement.def_entitlement_id.desc()).paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            'items': [e.json() for e in paginated.items],
            'total': paginated.total,
            'pages': paginated.pages,
            'page': paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching paginated entitlements', 'error': str(e)}), 500)


@entitlements_bp.route('/def_access_entitlements/<int:id>', methods=['GET'])
def get_entitlement_by_id(id):
    try:
        e = DefAccessEntitlement.query.filter_by(def_entitlement_id=id).first()
        if e:
            return make_response(jsonify(e.json()), 200)
        return make_response(jsonify({'message': 'Entitlement not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching entitlement', 'error': str(e)}), 500)


@entitlements_bp.route('/def_access_entitlements', methods=['POST'])
def create_entitlement():
    try:
        new_e = DefAccessEntitlement(
            entitlement_name=request.json.get('entitlement_name'),
            description=request.json.get('description'),
            comments=request.json.get('comments'),
            status=request.json.get('status'),
            effective_date= datetime.utcnow().date(),
            revision= 0,
            revision_date= datetime.utcnow().date(),
            created_by=request.json.get('created_by'),
            last_updated_by=request.json.get('last_updated_by')
        )
        db.session.add(new_e)
        db.session.commit()
        return make_response(jsonify({'message': 'Entitlement created successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': 'Error creating entitlement', 'error': str(e)}), 500)


@entitlements_bp.route('/def_access_entitlements/<int:id>', methods=['PUT'])
def update_entitlement(id):
    try:
        e = DefAccessEntitlement.query.filter_by(def_entitlement_id=id).first()
        if e:
            e.entitlement_name = request.json.get('entitlement_name', e.entitlement_name)
            e.description = request.json.get('description', e.description)
            e.comments = request.json.get('comments', e.comments)
            e.status = request.json.get('status', e.status)
            e.effective_date = datetime.utcnow().date()
            e.revision =  e.revision + 1
            e.revision_date = datetime.utcnow().date()
            e.created_by = request.json.get('created_by', e.created_by)
            e.last_updated_by = request.json.get('last_updated_by', e.last_updated_by)
            db.session.commit()
            return make_response(jsonify({'message': 'Entitlement updated successfully'}), 200)
        return make_response(jsonify({'message': 'Entitlement not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating entitlement', 'error': str(e)}), 500)


@entitlements_bp.route('/def_access_entitlements/<int:id>', methods=['DELETE'])
def delete_entitlement(id):
    try:
        e = DefAccessEntitlement.query.filter_by(def_entitlement_id=id).first()
        if e:
            db.session.delete(e)
            db.session.commit()
            return make_response(jsonify({'message': 'Entitlement deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Entitlement not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting entitlement', 'error': str(e)}), 500)


