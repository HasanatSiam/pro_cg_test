from flask import Blueprint, request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity

from sqlalchemy import or_


from executors.extensions import db
from executors.models import DefDataSource

data_sources_bp = Blueprint('data_sources', __name__)



#Def_Data_Sources
@data_sources_bp.route('/def_data_sources', methods=['POST'])
def create_def_data_source():
    try:
        new_ds = DefDataSource(
            datasource_name=request.json.get('datasource_name'),
            description=request.json.get('description'),
            application_type=request.json.get('application_type'),
            application_type_version=request.json.get('application_type_version'),
            last_access_synchronization_date=request.json.get('last_access_synchronization_date'),
            last_access_synchronization_status=request.json.get('last_access_synchronization_status'),
            last_transaction_synchronization_date=request.json.get('last_transaction_synchronization_date'),
            last_transaction_synchronization_status=request.json.get('last_transaction_synchronization_status'),
            default_datasource=request.json.get('default_datasource'),
            created_by=request.json.get('created_by'),
            last_updated_by=request.json.get('last_updated_by')
        )
        db.session.add(new_ds)
        db.session.commit()
        return make_response(jsonify({'message': 'Data source created successfully'}), 201)
    except Exception as e:
        return make_response(jsonify({'message': 'Error creating data source', 'error': str(e)}), 500)


@data_sources_bp.route('/def_data_sources', methods=['GET'])
@jwt_required()
def get_all_def_data_sources():
    try:
        data_sources = DefDataSource.query.order_by(DefDataSource.def_data_source_id.desc()).all()
        return make_response(jsonify([ds.json() for ds in data_sources]), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching data sources', 'error': str(e)}), 500)

@data_sources_bp.route('/def_data_sources/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_def_data_sources(page, limit):
    try:
        search_query = request.args.get('datasource_name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefDataSource.query

        if search_query:
            query = query.filter(
                or_(
                    DefDataSource.datasource_name.ilike(f'%{search_query}%'),
                    DefDataSource.datasource_name.ilike(f'%{search_underscore}%'),
                    DefDataSource.datasource_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefDataSource.def_data_source_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [ds.json() for ds in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error searching data sources', 'error': str(e)}), 500)

@data_sources_bp.route('/def_data_sources/<int:page>/<int:limit>', methods=['GET'])
def get_paginated_def_data_sources(page, limit):
    try:
        paginated = DefDataSource.query.order_by(DefDataSource.def_data_source_id.desc()).paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            'items': [ds.json() for ds in paginated.items],
            'total': paginated.total,
            'pages': paginated.pages,
            'page': paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching paginated data sources', 'error': str(e)}), 500)

@data_sources_bp.route('/def_data_sources/<int:id>', methods=['GET'])
def get_def_data_source_by_id(id):
    try:
        ds = DefDataSource.query.filter_by(def_data_source_id=id).first()
        if ds:
            return make_response(jsonify(ds.json()), 200)
        return make_response(jsonify({'message': 'Data source not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error fetching data source', 'error': str(e)}), 500)

@data_sources_bp.route('/def_data_sources/<int:id>', methods=['PUT'])
def update_def_data_source(id):
    try:
        ds = DefDataSource.query.filter_by(def_data_source_id=id).first()
        if ds:
            ds.datasource_name = request.json.get('datasource_name', ds.datasource_name)
            ds.description = request.json.get('description', ds.description)
            ds.application_type = request.json.get('application_type', ds.application_type)
            ds.application_type_version = request.json.get('application_type_version', ds.application_type_version)
            ds.last_access_synchronization_date = request.json.get('last_access_synchronization_date', ds.last_access_synchronization_date)
            ds.last_access_synchronization_status = request.json.get('last_access_synchronization_status', ds.last_access_synchronization_status)
            ds.last_transaction_synchronization_date = request.json.get('last_transaction_synchronization_date', ds.last_transaction_synchronization_date)
            ds.last_transaction_synchronization_status = request.json.get('last_transaction_synchronization_status', ds.last_transaction_synchronization_status)
            ds.default_datasource = request.json.get('default_datasource', ds.default_datasource)
            ds.created_by = request.json.get('created_by', ds.created_by)
            ds.last_updated_by = request.json.get('last_updated_by', ds.last_updated_by)
            db.session.commit()
            return make_response(jsonify({'message': 'Data source updated successfully'}), 200)
        return make_response(jsonify({'message': 'Data source not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error updating data source', 'error': str(e)}), 500)


@data_sources_bp.route('/def_data_sources/<int:id>', methods=['DELETE'])
def delete_def_data_source(id):
    try:
        ds = DefDataSource.query.filter_by(def_data_source_id=id).first()
        if ds:
            db.session.delete(ds)
            db.session.commit()
            return make_response(jsonify({'message': 'Data source deleted successfully'}), 200)
        return make_response(jsonify({'message': 'Data source not found'}), 404)
    except Exception as e:
        return make_response(jsonify({'message': 'Error deleting data source', 'error': str(e)}), 500)

