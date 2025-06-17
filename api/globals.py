from flask import Blueprint, request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity

from sqlalchemy.exc import IntegrityError
from sqlalchemy import or_

from executors.extensions import db
from executors.models import (
    DefGlobalCondition,
    DefGlobalConditionLogic,
    DefGlobalConditionLogicAttribute
)

globals_bp = Blueprint('globals', __name__)

# def_global_conditions
globals_bp.route('/def_global_conditions', methods=['POST'])
def create_def_global_condition():
    try:
        name        = request.json.get('name')
        datasource  = request.json.get('datasource')
        description = request.json.get('description')
        status      = request.json.get('status')

        new_condition = DefGlobalCondition(
            name        = name,
            datasource  = datasource,
            description = description,
            status      = status
        )

        db.session.add(new_condition)
        db.session.commit()

        return make_response(jsonify({"message": "DefGlobalCondition created successfully!"}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)

@globals_bp.route('/def_global_conditions', methods=['GET'])
def get_def_global_conditions():
    try:
        conditions = DefGlobalCondition.query.order_by(DefGlobalCondition.def_global_condition_id.desc()).all()
        return make_response(jsonify([condition.json() for condition in conditions]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving DefGlobalConditions", "error": str(e)}), 500)


@globals_bp.route('/def_global_conditions/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_global_conditions(page, limit):
    try:
        search_query = request.args.get('name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefGlobalCondition.query

        if search_query:
            query = query.filter(
                or_(
                    DefGlobalCondition.name.ilike(f'%{search_query}%'),
                    DefGlobalCondition.name.ilike(f'%{search_underscore}%'),
                    DefGlobalCondition.name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefGlobalCondition.def_global_condition_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({
            "message": "Error searching DefGlobalConditions",
            "error": str(e)
        }), 500)


@globals_bp.route('/def_global_conditions/<int:def_global_condition_id>', methods=['GET'])
def get_def_global_condition(def_global_condition_id):
    try:
        condition = DefGlobalCondition.query.filter_by(def_global_condition_id=def_global_condition_id).first()
        if condition:
            return make_response(jsonify(condition.json()), 200)
        return make_response(jsonify({"message": "DefGlobalCondition not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving DefGlobalCondition", "error": str(e)}), 500)


@globals_bp.route('/def_global_conditions/<int:page>/<int:limit>', methods=['GET'])
def get_paginated_def_global_conditions(page, limit):
    try:
        query = DefGlobalCondition.query.order_by(DefGlobalCondition.def_global_condition_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            "message": "Error retrieving DefGlobalConditions",
            "error": str(e)
        }), 500)


@globals_bp.route('/def_global_conditions/<int:def_global_condition_id>', methods=['PUT'])
def update_def_global_condition(def_global_condition_id):
    try:
        condition = DefGlobalCondition.query.filter_by(def_global_condition_id=def_global_condition_id).first()
        if condition:
            condition.name        = request.json.get('name', condition.name)
            condition.datasource  = request.json.get('datasource', condition.datasource)
            condition.description = request.json.get('description', condition.description)
            condition.status      = request.json.get('status', condition.status)

            db.session.commit()
            return make_response(jsonify({'message': 'DefGlobalCondition updated successfully'}), 200)
        return make_response(jsonify({'message': 'DefGlobalCondition not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating DefGlobalCondition', 'error': str(e)}), 500)

@globals_bp.route('/def_global_conditions/<int:def_global_condition_id>', methods=['DELETE'])
def delete_def_global_condition(def_global_condition_id):
    try:
        condition = DefGlobalCondition.query.filter_by(def_global_condition_id=def_global_condition_id).first()
        if condition:
            db.session.delete(condition)
            db.session.commit()
            return make_response(jsonify({'message': 'DefGlobalCondition deleted successfully'}), 200)
        return make_response(jsonify({'message': 'DefGlobalCondition not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting DefGlobalCondition', 'error': str(e)}), 500)




# def_global_condition_logics
@globals_bp.route('/def_global_condition_logics', methods=['POST'])
def create_def_global_condition_logic():
    try:
        def_global_condition_logic_id = request.json.get('def_global_condition_logic_id')

        if def_global_condition_logic_id is None:
            return make_response(jsonify({"message": "Missing 'def_global_condition_logic_id'"}), 400)

        # Check if ID already exists
        existing = DefGlobalConditionLogic.query.get(def_global_condition_logic_id)
        if existing:
            return make_response(jsonify({
                "message": f"DefGlobalConditionLogic ID {def_global_condition_logic_id} already exists."
            }), 409)
        
        def_global_condition_id = request.json.get('def_global_condition_id')
        object = request.json.get('object')
        attribute = request.json.get('attribute')
        condition = request.json.get('condition')
        value = request.json.get('value')

    
        new_logic = DefGlobalConditionLogic(
            def_global_condition_logic_id = def_global_condition_logic_id,
            def_global_condition_id = def_global_condition_id,
            object = object,
            attribute = attribute,
            condition = condition,
            value = value
        )
        db.session.add(new_logic)
        db.session.commit()
        return make_response(jsonify({'def_global_condition_logic_id' : new_logic.def_global_condition_logic_id,
                                      'message': 'DefGlobalConditionLogic created successfully!'}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)

@globals_bp.route('/def_global_condition_logics/upsert', methods=['POST'])
def upsert_def_global_condition_logics():
    try:
        data_list = request.get_json()

        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a list of objects'}), 400)

        response = []

        for data in data_list:
            def_global_condition_logic_id = data.get('def_global_condition_logic_id')
            def_global_condition_id = data.get('def_global_condition_id')
            object_text = data.get('object')
            attribute = data.get('attribute')
            condition = data.get('condition')
            value = data.get('value')

            existing_logic = DefGlobalConditionLogic.query.filter_by(def_global_condition_logic_id=def_global_condition_logic_id).first()

            if existing_logic:
                # Prevent changing foreign key
                if def_global_condition_id and def_global_condition_id != existing_logic.def_global_condition_id:
                    response.append({
                        'def_global_condition_logic_id': def_global_condition_logic_id,
                        'status': 'error',
                        'message': 'Updating def_global_condition_id is not allowed'
                    })
                    continue

                existing_logic.object = object_text
                existing_logic.attribute = attribute
                existing_logic.condition = condition
                existing_logic.value = value
                db.session.add(existing_logic)

                response.append({
                    'def_global_condition_logic_id': existing_logic.def_global_condition_logic_id,
                    'status': 'updated',
                    'message': 'Logic updated successfully'
                })

            else:
                if not def_global_condition_id:
                    response.append({
                        'status': 'error',
                        'message': 'def_global_condition_id is required for new records'
                    })
                    continue

                # Validate foreign key existence (optional; depends on enforcement at DB)
                condition_exists = db.session.query(
                    db.exists().where(DefGlobalCondition.def_global_condition_id == def_global_condition_id)
                ).scalar()

                if not condition_exists:
                    response.append({
                        'status': 'error',
                        'message': f'def_global_condition_id {def_global_condition_id} does not exist'
                    })
                    continue

                new_logic = DefGlobalConditionLogic(
                    def_global_condition_logic_id=def_global_condition_logic_id,
                    def_global_condition_id=def_global_condition_id,
                    object=object_text,
                    attribute=attribute,
                    condition=condition,
                    value=value
                )
                db.session.add(new_logic)
                db.session.flush()

                response.append({
                    'def_global_condition_logic_id': new_logic.def_global_condition_logic_id,
                    'status': 'created',
                    'message': 'Logic created successfully'
                })

        db.session.commit()
        return make_response(jsonify(response), 200)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({'message': 'Integrity error during upsert'}), 409)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error during upsert',
            'error': str(e)
        }), 500)


@globals_bp.route('/def_global_condition_logics', methods=['GET'])
def get_def_global_condition_logics():
    try:
        logics = DefGlobalConditionLogic.query.order_by(DefGlobalConditionLogic.def_global_condition_logic_id.desc()).all()
        return make_response(jsonify([logic.json() for logic in logics]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving DefGlobalConditionLogics", "error": str(e)}), 500)



@globals_bp.route('/def_global_condition_logics/<int:def_global_condition_logic_id>', methods=['GET'])
def get_def_global_condition_logic(def_global_condition_logic_id):
    try:
        logic = DefGlobalConditionLogic.query.filter_by(def_global_condition_logic_id=def_global_condition_logic_id).first()
        if logic:
            return make_response(jsonify(logic.json()), 200)
        return make_response(jsonify({"message": "DefGlobalConditionLogic not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving DefGlobalConditionLogic", "error": str(e)}), 500)


@globals_bp.route('/def_global_condition_logics/<int:def_global_condition_logic_id>', methods=['PUT'])
def update_def_global_condition_logic(def_global_condition_logic_id):
    try:
        logic = DefGlobalConditionLogic.query.filter_by(def_global_condition_logic_id=def_global_condition_logic_id).first()
        if logic:
            logic.def_global_condition_id = request.json.get('def_global_condition_id', logic.def_global_condition_id)
            logic.object                  = request.json.get('object', logic.object)
            logic.attribute               = request.json.get('attribute', logic.attribute)
            logic.condition               = request.json.get('condition', logic.condition)
            logic.value                   = request.json.get('value', logic.value)

            db.session.commit()
            return make_response(jsonify({'message': 'DefGlobalConditionLogic updated successfully'}), 200)
        return make_response(jsonify({'message': 'DefGlobalConditionLogic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating DefGlobalConditionLogic', 'error': str(e)}), 500)


@globals_bp.route('/def_global_condition_logics/<int:def_global_condition_logic_id>', methods=['DELETE'])
def delete_def_global_condition_logic(def_global_condition_logic_id):
    try:
        logic = DefGlobalConditionLogic.query.filter_by(def_global_condition_logic_id=def_global_condition_logic_id).first()
        if logic:
            db.session.delete(logic)
            db.session.commit()
            return make_response(jsonify({'message': 'DefGlobalConditionLogic deleted successfully'}), 200)
        return make_response(jsonify({'message': 'DefGlobalConditionLogic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting DefGlobalConditionLogic', 'error': str(e)}), 500)





# def_global_condition_logics_attributes
@globals_bp.route('/def_global_condition_logic_attributes', methods=['POST'])
def create_def_global_condition_logic_attribute():
    try:
        id = request.json.get('id')
        def_global_condition_logic_id = request.json.get('def_global_condition_logic_id')
        widget_position = request.json.get('widget_position')
        widget_state = request.json.get('widget_state')

        if not all([id, def_global_condition_logic_id]):
            return make_response(jsonify({'message': 'Both id and def_global_condition_logic_id are required'}), 400)

        # Check if the ID already exists
        existing = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()
        if existing:
            return make_response(jsonify({'message': f'Attribute with id {id} already exists'}), 409)

        # Check if foreign key exists
        logic_exists = db.session.query(
            db.exists().where(DefGlobalConditionLogic.def_global_condition_logic_id == def_global_condition_logic_id)
        ).scalar()

        if not logic_exists:
            return make_response(jsonify({
                'message': f'def_global_condition_logic_id {def_global_condition_logic_id} does not exist'
            }), 404)

        # Create new record
        new_attr = DefGlobalConditionLogicAttribute(
            id=id,
            def_global_condition_logic_id=def_global_condition_logic_id,
            widget_position=widget_position,
            widget_state=widget_state
        )

        db.session.add(new_attr)
        db.session.commit()

        return make_response(jsonify({
            'id': new_attr.id,
            'message': 'Attribute created successfully'
        }), 201)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({'message': 'Integrity error (possibly duplicate key)'}), 409)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({'message': 'Error creating attribute', 'error': str(e)}), 500)



@globals_bp.route('/def_global_condition_logic_attributes', methods=['GET'])
def get_all_def_global_condition_logic_attributes():
    try:
        attributes = DefGlobalConditionLogicAttribute.query.order_by(DefGlobalConditionLogicAttribute.id.desc()).all()
        return make_response(jsonify([attribute.json() for attribute in attributes]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving condition logic attributes", "error": str(e)}), 500)


@globals_bp.route('/def_global_condition_logic_attributes/<int:id>', methods=['GET'])
def get_def_global_condition_logic_attribute(id):
    try:
        attribute = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()
        if attribute:
            return make_response(jsonify(attribute.json()), 200)
        return make_response(jsonify({"message": "DefGlobalConditionLogicAttribute not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving condition logic attribute", "error": str(e)}), 500)
    

@globals_bp.route('/def_global_condition_logic_attributes/<int:page>/<int:limit>', methods=['GET'])
# @jwt_required()
def get_paginated_def_global_condition_logic_attributes(page, limit):
    try:
        query = DefGlobalConditionLogicAttribute.query.order_by(DefGlobalConditionLogicAttribute.id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [item.json() for item in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({
            'message': 'Error fetching global condition logic attributes',
            'error': str(e)
        }), 500)


@globals_bp.route('/def_global_condition_logic_attributes/upsert', methods=['POST'])
def upsert_def_global_condition_logic_attributes():
    try:
        data_list = request.get_json()

        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a list of objects'}), 400)

        response = []

        for data in data_list:
            id = data.get('id')
            def_global_condition_logic_id = data.get('def_global_condition_logic_id')
            widget_position = data.get('widget_position')
            widget_state = data.get('widget_state')

            existing_attr = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()

            if existing_attr:
                # Prevent changing foreign key
                if def_global_condition_logic_id and def_global_condition_logic_id != existing_attr.def_global_condition_logic_id:
                    response.append({
                        'id': id,
                        'status': 'error',
                        'message': 'Updating def_global_condition_logic_id is not allowed'
                    })
                    continue

                existing_attr.widget_position = widget_position
                existing_attr.widget_state = widget_state
                db.session.add(existing_attr)

                response.append({
                    'id': existing_attr.id,
                    'status': 'updated',
                    'message': 'Attribute updated successfully'
                })

            else:
                # Validate required FK
                if not def_global_condition_logic_id:
                    response.append({
                        'status': 'error',
                        'message': 'def_global_condition_logic_id is required for new records'
                    })
                    continue

                # Check foreign key existence
                logic_exists = db.session.query(
                    db.exists().where(DefGlobalConditionLogic.def_global_condition_logic_id == def_global_condition_logic_id)
                ).scalar()

                if not logic_exists:
                    response.append({
                        'status': 'error',
                        'message': f'def_global_condition_logic_id {def_global_condition_logic_id} does not exist'
                    })
                    continue

                if not id:
                    response.append({
                        'status': 'error',
                        'message': 'id is required for new records'
                    })
                    continue

                new_attr = DefGlobalConditionLogicAttribute(
                    id=id,
                    def_global_condition_logic_id=def_global_condition_logic_id,
                    widget_position=widget_position,
                    widget_state=widget_state
                )
                db.session.add(new_attr)
                db.session.flush()

                response.append({
                    'id': new_attr.id,
                    'status': 'created',
                    'message': 'Attribute created successfully'
                })

        db.session.commit()
        return make_response(jsonify(response), 200)

    except IntegrityError:
        db.session.rollback()
        return make_response(jsonify({'message': 'Integrity error during upsert'}), 409)
    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error during upsert',
            'error': str(e)
        }), 500)


@globals_bp.route('/def_global_condition_logic_attributes/<int:id>', methods=['PUT'])
def update_def_global_condition_logic_attribute(id):
    try:
        data = request.get_json()
        attribute = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()

        if not attribute:
            return make_response(jsonify({'message': 'DefGlobalConditionLogicAttribute not found'}), 404)

        # Update allowed fields
        attribute.widget_position = data.get('widget_position', attribute.widget_position)
        attribute.widget_state = data.get('widget_state', attribute.widget_state)

        db.session.commit()

        return make_response(jsonify({'message': 'DefGlobalConditionLogicAttribute updated successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error updating DefGlobalConditionLogicAttribute',
            'error': str(e)
        }), 500)




@globals_bp.route('/def_global_condition_logic_attributes/<int:id>', methods=['DELETE'])
def delete_def_global_condition_logic_attribute(id):
    try:
        attribute = DefGlobalConditionLogicAttribute.query.filter_by(id=id).first()

        if not attribute:
            return make_response(jsonify({'message': 'DefGlobalConditionLogicAttribute not found'}), 404)

        db.session.delete(attribute)
        db.session.commit()

        return make_response(jsonify({'message': 'DefGlobalConditionLogicAttribute deleted successfully'}), 200)

    except Exception as e:
        db.session.rollback()
        return make_response(jsonify({
            'message': 'Error deleting DefGlobalConditionLogicAttribute',
            'error': str(e)
        }), 500)


