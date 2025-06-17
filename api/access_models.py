from flask import Blueprint, request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func, or_
from datetime import datetime

from executors.extensions import db
from executors.models import (
    DefAccessModel,
    DefAccessModelLogic,
    DefAccessModelLogicAttribute,
    DefDataSource
)


access_models_bp= Blueprint('access_models', __name__)
#def_access_models
@access_models_bp.route('/def_access_models', methods=['POST'])
def create_def_access_models():
    try:
        datasource_name = request.json.get('datasource_name', None)
        # Only validate foreign key if datasource_name is provided and not null/empty
        if datasource_name:
            datasource = DefDataSource.query.filter_by(datasource_name=datasource_name).first()
            if not datasource:
                return make_response(jsonify({"message": f"Datasource '{datasource_name}' does not exist"}), 400)

        new_def_access_model = DefAccessModel(
            model_name = request.json.get('model_name'),
            description = request.json.get('description'),
            type = request.json.get('type'),
            run_status = request.json.get('run_status'),
            state = request.json.get('state'),
            last_run_date = datetime.utcnow(),
            created_by = request.json.get('created_by'),
            last_updated_by = request.json.get('last_updated_by'),
            last_updated_date = datetime.utcnow(),
            revision = 0,
            revision_date = datetime.utcnow(),
            datasource_name = datasource_name  # FK assignment
        )
        db.session.add(new_def_access_model)
        db.session.commit()
        return make_response(jsonify({"message": "DefAccessModel created successfully!"}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)
    
@access_models_bp.route('/def_access_models', methods=['GET'])
def get_def_access_models():
    try:
        models = DefAccessModel.query.order_by(DefAccessModel.def_access_model_id.desc()).all()
        return make_response(jsonify([model.json() for model in models]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving access models", "error": str(e)}), 500)

@access_models_bp.route('/def_access_models/<int:page>/<int:limit>', methods=['GET'])
def get_paginated_def_access_models(page, limit):
    try:
        query = DefAccessModel.query.order_by(DefAccessModel.def_access_model_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [model.json() for model in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)

    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving access models", "error": str(e)}), 500)


@access_models_bp.route('/def_access_models/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_access_models(page, limit):
    try:
        search_query = request.args.get('model_name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefAccessModel.query

        if search_query:
            query = query.filter(
                or_(
                    DefAccessModel.model_name.ilike(f'%{search_query}%'),
                    DefAccessModel.model_name.ilike(f'%{search_underscore}%'),
                    DefAccessModel.model_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefAccessModel.def_access_model_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [model.json() for model in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching access models", "error": str(e)}), 500)


@access_models_bp.route('/def_access_models/<int:model_id>', methods=['GET'])
def get_def_access_model(model_id):
    try:
        model = DefAccessModel.query.filter_by(def_access_model_id=model_id).first()
        return make_response(jsonify(model.json()), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving access models", "error": str(e)}), 500)


@access_models_bp.route('/def_access_models/<int:model_id>', methods=['PUT'])
def update_def_access_model(model_id):
    try:
        model = DefAccessModel.query.filter_by(def_access_model_id=model_id).first()
        if model:
            data = request.get_json()
            # Handle datasource_name FK update with case-insensitive and space-insensitive matching
            if 'datasource_name' in data:
                # Normalize input: strip, lower, remove underscores and spaces for matching
                input_ds = data['datasource_name'].strip().lower().replace('_', '').replace(' ', '')
                datasource = DefDataSource.query.filter(
                    func.replace(func.replace(func.lower(DefDataSource.datasource_name), '_', ''), ' ', '') == input_ds
                ).first()
                if not datasource:
                    return make_response(jsonify({"message": f"Datasource '{data['datasource_name']}' does not exist"}), 404)
                model.datasource_name = datasource.datasource_name  # Use the canonical name from DB

            model.model_name        = data.get('model_name', model.model_name)
            model.description       = data.get('description', model.description)
            model.type              = data.get('type', model.type)
            model.run_status        = data.get('run_status', model.run_status)
            model.state             = data.get('state', model.state)
            model.last_run_date     = datetime.utcnow()
            model.last_updated_by   = data.get('last_updated_by', model.last_updated_by)
            model.last_updated_date = datetime.utcnow()
            model.revision          = model.revision + 1
            model.revision_date     = datetime.utcnow()

            db.session.commit()
            return make_response(jsonify({'message': 'DefAccessModel updated successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'DefAccessModel not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating DefAccessModel', 'error': str(e)}), 500)

@access_models_bp.route('/def_access_models/<int:model_id>', methods=['DELETE'])
def delete_def_access_model(model_id):
    try:
        model = DefAccessModel.query.filter_by(def_access_model_id=model_id).first()
        if model:
            db.session.delete(model)
            db.session.commit()
            return make_response(jsonify({'message': 'DefAccessModel deleted successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'DefAccessModel not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting DefAccessModel', 'error': str(e)}), 500)




#def_access_model_logics
@access_models_bp.route('/def_access_model_logics', methods=['POST'])
def create_def_access_model_logic():
    try:
        def_access_model_logic_id = request.json.get('def_access_model_logic_id')
        def_access_model_id = request.json.get('def_access_model_id')
        filter_text = request.json.get('filter')
        object_text = request.json.get('object')
        attribute = request.json.get('attribute')
        condition = request.json.get('condition')
        value = request.json.get('value')

        if not def_access_model_id:
            return make_response(jsonify({'message': 'def_access_model_id is required'}), 400)
        
        if DefAccessModelLogic.query.filter_by(def_access_model_logic_id=def_access_model_logic_id).first():
            return make_response(jsonify({'message': f'def_access_model_logic_id {def_access_model_logic_id} already exists'}), 409)

        # Check if def_access_model_id exists in DefAccessModel table
        model_id_exists = db.session.query(
            db.exists().where(DefAccessModel.def_access_model_id == def_access_model_id)
        ).scalar()
        if not model_id_exists:
            return make_response(jsonify({'message': f'def_access_model_id {def_access_model_id} does not exist'}), 400)

        new_logic = DefAccessModelLogic(
            def_access_model_logic_id=def_access_model_logic_id,
            def_access_model_id=def_access_model_id,
            filter=filter_text,
            object=object_text,
            attribute=attribute,
            condition=condition,
            value=value
        )
        db.session.add(new_logic)
        db.session.commit()
        return make_response(jsonify({'message': 'DefAccessModelLogic created successfully!'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': f'Error: {str(e)}'}), 500)

@access_models_bp.route('/def_access_model_logics/upsert', methods=['POST'])
def upsert_def_access_model_logics():
    try:
        data_list = request.get_json()

        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a list of objects'}), 400)

        response = []

        for data in data_list:
            def_access_model_logic_id = data.get('def_access_model_logic_id')
            model_id = data.get('def_access_model_id')
            filter_text = data.get('filter')
            object_text = data.get('object')
            attribute = data.get('attribute')
            condition = data.get('condition')
            value = data.get('value')

            existing_logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=def_access_model_logic_id).first()

            if existing_logic:
                # if not logic:
                #     response.append({
                #         'def_access_model_logic_id': logic_id,
                #         'status': 'error',
                #         'message': f'DefAccessModelLogic with id {logic_id} not found'
                #     })
                #     continue

                # Prevent changing foreign key
                if model_id and model_id != existing_logic.def_access_model_id:
                    response.append({
                        'def_access_model_logic_id': def_access_model_logic_id,
                        'status': 'error',
                        'message': 'Updating def_access_model_id is not allowed'
                    })
                    continue

               
                existing_logic.filter = filter_text
                existing_logic.object = object_text
                existing_logic.attribute = attribute
                existing_logic.condition = condition
                existing_logic.value = value
                db.session.add(existing_logic)

                response.append({
                    'def_access_model_logic_id': existing_logic.def_access_model_logic_id,
                    'status': 'updated',
                    'message': 'Logic updated successfully'
                })

            else:
                if not model_id:
                    response.append({
                        'status': 'error',
                        'message': 'def_access_model_id is required for new records'
                    })
                    continue

                # Validate foreign key existence (optional; depends on enforcement at DB)
                model_exists = db.session.query(
                    db.exists().where(DefAccessModel.def_access_model_id == model_id)
                ).scalar()

                if not model_exists:
                    response.append({
                        'status': 'error',
                        'message': f'def_access_model_id {model_id} does not exist'
                    })
                    continue

                new_logic = DefAccessModelLogic(
                    def_access_model_logic_id = def_access_model_logic_id,
                    def_access_model_id=model_id,
                    filter=filter_text,
                    object=object_text,
                    attribute=attribute,
                    condition=condition,
                    value=value
                )
                db.session.add(new_logic)
                db.session.flush()

                response.append({
                    'def_access_model_logic_id': new_logic.def_access_model_logic_id,
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



@access_models_bp.route('/def_access_model_logics', methods=['GET'])
def get_def_access_model_logics():
    try:
        logics = DefAccessModelLogic.query.order_by(DefAccessModelLogic.def_access_model_logic_id.desc()).all()
        return make_response(jsonify([logic.json() for logic in logics]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error retrieving logics', 'error': str(e)}), 500)


@access_models_bp.route('/def_access_model_logics/<int:logic_id>', methods=['GET'])
def get_def_access_model_logic(logic_id):
    try:
        logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=logic_id).first()
        if logic:
            return make_response(jsonify(logic.json()), 200)
        else:
            return make_response(jsonify({'message': 'Logic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error retrieving logic', 'error': str(e)}), 500)


@access_models_bp.route('/def_access_model_logics/<int:logic_id>', methods=['PUT'])
def update_def_access_model_logic(logic_id):
    try:
        logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=logic_id).first()
        if logic:
            # logic.def_access_model_id = request.json.get('def_access_model_id', logic.def_access_model_id)
            logic.filter = request.json.get('filter', logic.filter)
            logic.object = request.json.get('object', logic.object)
            logic.attribute = request.json.get('attribute', logic.attribute)
            logic.condition = request.json.get('condition', logic.condition)
            logic.value = request.json.get('value', logic.value)

            db.session.commit()
            return make_response(jsonify({'message': 'Logic updated successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Logic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating logic', 'error': str(e)}), 500)


@access_models_bp.route('/def_access_model_logics/<int:logic_id>', methods=['DELETE'])
def delete_def_access_model_logic(logic_id):
    try:
        logic = DefAccessModelLogic.query.filter_by(def_access_model_logic_id=logic_id).first()
        if logic:
            db.session.delete(logic)
            db.session.commit()
            return make_response(jsonify({'message': 'Logic deleted successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Logic not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting logic', 'error': str(e)}), 500)





#def_access_model_logic_attributes
@access_models_bp.route('/def_access_model_logic_attributes', methods=['POST'])
def create_def_access_model_logic_attribute():
    try:
        id = request.json.get('id')
        def_access_model_logic_id = request.json.get('def_access_model_logic_id')
        widget_position = request.json.get('widget_position')
        widget_state = request.json.get('widget_state')

        if not def_access_model_logic_id:
            return make_response(jsonify({'message': 'def_access_model_logic_id is required'}), 400)
        if DefAccessModelLogicAttribute.query.filter_by(id=id).first():
            return make_response(jsonify({'message': f'id {id} already exists'}), 409)
        # Check if def_access_model_logic_id exists in DefAccessModelLogic table
        logic_id_exists = db.session.query(
            db.exists().where(DefAccessModelLogic.def_access_model_logic_id == def_access_model_logic_id)
        ).scalar()
        if not logic_id_exists:
            return make_response(jsonify({'message': f'def_access_model_logic_id {def_access_model_logic_id} does not exist'}), 400)
        
        
        new_attribute = DefAccessModelLogicAttribute(
            id = id,
            def_access_model_logic_id=def_access_model_logic_id,
            widget_position=widget_position,
            widget_state=widget_state
        )
        db.session.add(new_attribute)
        db.session.commit()
        return make_response(jsonify({"message": "DefAccessModelLogicAttribute created successfully!"}), 201)
    except Exception as e:
        return make_response(jsonify({"message": f"Error: {str(e)}"}), 500)


@access_models_bp.route('/def_access_model_logic_attributes', methods=['GET'])
def get_def_access_model_logic_attributes():
    try:
        attributes = DefAccessModelLogicAttribute.query.order_by(DefAccessModelLogicAttribute.id.desc()).all()
        return make_response(jsonify([attr.json() for attr in attributes]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving attributes", "error": str(e)}), 500)



@access_models_bp.route('/def_access_model_logic_attributes/upsert', methods=['POST'])
def upsert_def_access_model_logic_attributes():
    try:
        data_list = request.get_json()

        # Enforce list-only payload
        if not isinstance(data_list, list):
            return make_response(jsonify({'message': 'Payload must be a list of objects'}), 400)

        response = []

        for data in data_list:
            id = data.get('id')
            def_access_model_logic_id = data.get('def_access_model_logic_id')
            widget_position = data.get('widget_position')
            widget_state = data.get('widget_state')

            existing_attribute = DefAccessModelLogicAttribute.query.filter_by(id=id).first()
            if existing_attribute:
                # if not attribute:
                #     response.append({
                #         'id': attribute_id,
                #         'status': 'error',
                #         'message': f'Attribute with id {attribute_id} not found'
                #     })
                #     continue

                # Disallow updating def_access_model_logic_id
                if def_access_model_logic_id and def_access_model_logic_id != existing_attribute.def_access_model_logic_id:
                    response.append({
                        'id': id,
                        'status': 'error',
                        'message': 'Updating def_access_model_logic_id is not allowed'
                    })
                    continue

                existing_attribute.widget_position = widget_position
                existing_attribute.widget_state = widget_state
                db.session.add(existing_attribute)

                response.append({
                    'id': existing_attribute.id,
                    'status': 'updated',
                    'message': 'Attribute updated successfully'
                })

            else:
                # Take the maximum data of foreign-key from foreign table
                # def_access_model_logic_id = db.session.query(
                #     func.max(DefAccessModelLogic.def_access_model_logic_id)
                # ).scalar()

                # if def_access_model_logic_id is None:
                #     response.append({
                #         'status': 'error',
                #         'message': 'No DefAccessModelLogic entries exist to assign logic ID'
                #     })
                #     continue

                # Validate def_access_model_logic_id exists
                logic_exists = db.session.query(
                    db.exists().where(DefAccessModelLogic.def_access_model_logic_id == def_access_model_logic_id)
                    ).scalar()

                if not logic_exists:
                    response.append({
                        'status': 'error',
                        'message': f'def_access_model_logic_id {def_access_model_logic_id} does not exist'
                    })
                    continue

                new_attribute = DefAccessModelLogicAttribute(
                    id = id,
                    def_access_model_logic_id=def_access_model_logic_id,
                    widget_position=widget_position,
                    widget_state=widget_state
                )
                db.session.add(new_attribute)
                db.session.flush()

                response.append({
                    'id': new_attribute.id,
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



@access_models_bp.route('/def_access_model_logic_attributes/<int:attr_id>', methods=['GET'])
def get_def_access_model_logic_attribute(attr_id):
    try:
        attribute = DefAccessModelLogicAttribute.query.filter_by(id=attr_id).first()
        if attribute:
            return make_response(jsonify(attribute.json()), 200)
        else:
            return make_response(jsonify({'message': 'Attribute not found'}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving attribute", "error": str(e)}), 500)


@access_models_bp.route('/def_access_model_logic_attributes/<int:attr_id>', methods=['PUT'])
def update_def_access_model_logic_attribute(attr_id):
    try:
        attribute = DefAccessModelLogicAttribute.query.filter_by(id=attr_id).first()
        if attribute:
            # attribute.def_access_model_logic_id = request.json.get('def_access_model_logic_id', attribute.def_access_model_logic_id)
            attribute.widget_position = request.json.get('widget_position', attribute.widget_position)
            attribute.widget_state = request.json.get('widget_state', attribute.widget_state)

            db.session.commit()
            return make_response(jsonify({'message': 'Attribute updated successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Attribute not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating attribute', 'error': str(e)}), 500)


@access_models_bp.route('/def_access_model_logic_attributes/<int:attr_id>', methods=['DELETE'])
def delete_def_access_model_logic_attribute(attr_id):
    try:
        attribute = DefAccessModelLogicAttribute.query.filter_by(id=attr_id).first()
        if attribute:
            db.session.delete(attribute)
            db.session.commit()
            return make_response(jsonify({'message': 'Attribute deleted successfully'}), 200)
        else:
            return make_response(jsonify({'message': 'Attribute not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting attribute', 'error': str(e)}), 500)


