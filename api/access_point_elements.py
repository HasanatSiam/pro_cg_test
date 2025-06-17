
from flask import Blueprint, request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity, get_jwt
from sqlalchemy import desc, or_

from executors.extensions import db
from executors.models import (
    DefDataSource,
    DefAccessPointElement
)



access_point_elements_bp = Blueprint('access_point_elements', __name__)



#Def_access_point_elements
@access_point_elements_bp.route('/def_access_point_elements', methods=['POST'])
def create_def_access_point_element():
    try:
        def_data_source_id = request.json.get('def_data_source_id')
        element_name = request.json.get('element_name')
        description = request.json.get('description')
        platform = request.json.get('platform')
        element_type = request.json.get('element_type')
        access_control = request.json.get('access_control')
        change_control = request.json.get('change_control')
        audit = request.json.get('audit')
        created_by = request.json.get('created_by')
        last_updated_by = request.json.get('last_updated_by')

        if not def_data_source_id:
            return make_response(jsonify({'message': 'def_data_source_id is required'}), 400)

        data_source = DefDataSource.query.get(def_data_source_id)
        if not data_source:
            return make_response(jsonify({'message': 'Invalid def_data_source_id — referenced source not found'}), 404)

        new_element = DefAccessPointElement(
            def_data_source_id=def_data_source_id,
            element_name=element_name,
            description=description,
            platform=platform,
            element_type=element_type,
            access_control=access_control,
            change_control=change_control,
            audit=audit,
            created_by=created_by,
            last_updated_by=last_updated_by
        )

        db.session.add(new_element)
        db.session.commit()
        return make_response(jsonify({"message": "DefAccessPointElement created successfully!"}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    

@access_point_elements_bp.route('/def_access_point_elements', methods=['GET'])
def get_all_def_access_point_elements():
    try:
        elements = DefAccessPointElement.query.order_by(
            desc(DefAccessPointElement.def_access_point_id)
        ).all()
        return jsonify([element.json() for element in elements])
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500
    
@access_point_elements_bp.route('/def_access_point_elements/<int:dap_id>', methods=['GET'])
def get_def_access_point_element_by_id(dap_id):
    try:
        element = DefAccessPointElement.query.get(dap_id)
        
        if element is None:
            return jsonify({"error": "Element not found"}), 404

        return jsonify(element.json())

    except ValueError:
        return jsonify({"error": "Invalid ID format. ID must be an integer."}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@access_point_elements_bp.route('/def_access_point_elements/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_access_point_elements(page, limit):
    try:
        search_query = request.args.get('element_name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAccessPointElement.query

        if search_query:
            query = query.filter(
                or_(
                    DefAccessPointElement.element_name.ilike(f'%{search_query}%'),
                    DefAccessPointElement.element_name.ilike(f'%{search_underscore}%'),
                    DefAccessPointElement.element_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefAccessPointElement.def_access_point_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [element.json() for element in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error searching access point elements', 'error': str(e)}), 500)



@access_point_elements_bp.route('/def_access_point_elements/<int:page>/<int:limit>', methods=['GET'])
def get_paginated_elements(page, limit):
    try:
        query = DefAccessPointElement.query.order_by(DefAccessPointElement.def_access_point_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        # Return paginated data
        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching elements', 'error': str(e)}), 500)

    
@access_point_elements_bp.route('/def_access_point_elements/<int:dap_id>', methods=['PUT'])
def update_def_access_point_element(dap_id):
    try:
        element = DefAccessPointElement.query.filter_by(def_access_point_id=dap_id).first()
        if not element:
            return make_response(jsonify({'message': 'DefAccessPointElement not found'}), 404)

        new_data_source_id = request.json.get('def_data_source_id')

        # If a new def_data_source_id is provided, verify that it exists
        if new_data_source_id is not None:
            data_source_exists = DefDataSource.query.filter_by(def_data_source_id=new_data_source_id).first()
            if not data_source_exists:
                return make_response(jsonify({'message': 'Invalid def_data_source_id – no such data source exists'}), 400)
            element.def_data_source_id = new_data_source_id

        
        element.element_name = request.json.get('element_name', element.element_name)
        element.description = request.json.get('description', element.description)
        element.platform = request.json.get('platform', element.platform)
        element.element_type = request.json.get('element_type', element.element_type)
        element.access_control = request.json.get('access_control', element.access_control)
        element.change_control = request.json.get('change_control', element.change_control)
        element.audit = request.json.get('audit', element.audit)
        element.created_by = request.json.get('created_by', element.created_by)
        element.last_updated_by = request.json.get('last_updated_by', element.last_updated_by)

        db.session.commit()
        return make_response(jsonify({'message': 'DefAccessPointElement updated successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error updating DefAccessPointElement', 'error': str(e)}), 500)


@access_point_elements_bp.route('/def_access_point_elements/<int:dap_id>', methods=['DELETE'])
def delete_element(dap_id):
    try:
        element = DefAccessPointElement.query.filter_by(def_access_point_id=dap_id).first()

        if element:
            db.session.delete(element)
            db.session.commit()
            return make_response(jsonify({'message': 'DefAccessPointElement deleted successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'DefAccessPointElement not found'}), 404)

    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting DefAccessPointElement', 'error': str(e)}), 500)


