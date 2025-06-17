from flask import Blueprint, request, jsonify, make_response
from flask_jwt_extended import jwt_required, get_jwt_identity

from sqlalchemy.exc import IntegrityError
from sqlalchemy import or_

from executors.extensions import db
from executors.models import (
    DefTenant,
    DefTenantEnterpriseSetup,
    DefTenantEnterpriseSetupV
)

enterprises_bp = Blueprint('enterprises', __name__)


# Create a tenant
@enterprises_bp.route('/tenants', methods=['POST'])
@jwt_required()
def create_tenant():
    try:
       data = request.get_json()
    #    tenant_id   = generate_tenant_id()  # Call the function to get the result
       tenant_name = data['tenant_name']
       new_tenant  = DefTenant(tenant_name = tenant_name)
       db.session.add(new_tenant)
       db.session.commit()
       return make_response(jsonify({"message": "Tenant created successfully"}), 201)
   
    except IntegrityError as e:
        return make_response(jsonify({"message": "Error creating Tenant", "error": "Tenant already exists"}), 409)
    except Exception as e:
        return make_response(jsonify({"message": "Error creating Tenant", "error": str(e)}), 500)

       

# Get all tenants
@enterprises_bp.route('/tenants', methods=['GET'])
@jwt_required()
def get_tenants():
    try:
        tenants = DefTenant.query.order_by(DefTenant.tenant_id.desc()).all()
        return make_response(jsonify([tenant.json() for tenant in tenants]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting Tenants", "error": str(e)}), 500)

@enterprises_bp.route('/tenants/v1', methods=['GET'])
def get_tenants_v1():
    try:
        tenants = DefTenant.query.order_by(DefTenant.tenant_id.desc()).all()
        return make_response(jsonify([tenant.json() for tenant in tenants]))
    except Exception as e:
        return make_response(jsonify({"message": "Error getting Tenants", "error": str(e)}), 500)



@enterprises_bp.route('/def_tenants/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_tenants(page, limit):
    try:
        query = DefTenant.query.order_by(DefTenant.tenant_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return make_response(jsonify({
            "items": [tenant.json() for tenant in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error fetching paginated tenants", "error": str(e)}), 500)


@enterprises_bp.route('/def_tenants/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_tenants(page, limit):
    try:
        search_query = request.args.get('tenant_name', '').strip()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = DefTenant.query

        if search_query:
            query = query.filter(
                or_(
                    DefTenant.tenant_name.ilike(f'%{search_query}%'),
                    DefTenant.tenant_name.ilike(f'%{search_underscore}%'),
                    DefTenant.tenant_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefTenant.tenant_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [tenant.json() for tenant in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching tenants", "error": str(e)}), 500)



@enterprises_bp.route('/tenants/<int:tenant_id>', methods=['GET'])
@jwt_required()
def get_tenant(tenant_id):
    try:
        tenant = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if tenant:
            return make_response(jsonify(tenant.json()),200)
        else:
            return make_response(jsonify({"message": "Tenant not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving tenant", "error": str(e)}), 500)


# Update a tenant
@enterprises_bp.route('/tenants/<int:tenant_id>', methods=['PUT'])
@jwt_required()
def update_tenant(tenant_id):
    try:
        tenant = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if tenant:
            data = request.get_json()
            tenant.tenant_name  = data['tenant_name']
            db.session.commit()
            return make_response(jsonify({"message": "Tenant updated successfully"}), 200)
        return make_response(jsonify({"message": "Tenant not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error updating Tenant", "error": str(e)}), 500)


# Delete a tenant
@enterprises_bp.route('/tenants/<int:tenant_id>', methods=['DELETE'])
@jwt_required()
def delete_tenant(tenant_id):
    try:
        user = DefTenant.query.filter_by(tenant_id=tenant_id).first()
        if user:
            db.session.delete(user)
            db.session.commit()
            return make_response(jsonify({"message": "Tenant deleted successfully"}), 200)
        return make_response(jsonify({"message": "Tenant not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error deleting tenant", "error": str(e)}), 500)


# Create enterprise setup
@enterprises_bp.route('/create_enterpriseV1/<int:tenant_id>', methods=['POST'])
@jwt_required()
def create_enterprise(tenant_id):
    try:
        data = request.get_json()
        tenant_id       = tenant_id
        enterprise_name = data['enterprise_name']
        enterprise_type = data['enterprise_type']

        new_enterprise = DefTenantEnterpriseSetup(
            tenant_id=tenant_id,
            enterprise_name=enterprise_name,
            enterprise_type=enterprise_type
        )

        db.session.add(new_enterprise)
        db.session.commit()
        return make_response(jsonify({"message": "Enterprise setup created successfully"}), 201)

    except IntegrityError:
        return make_response(jsonify({"message": "Error creating enterprise setup", "error": "Setup already exists"}), 409)
    except Exception as e:
        return make_response(jsonify({"message": "Error creating enterprise setup", "error": str(e)}), 500)

# Create or update enterprise setup
@enterprises_bp.route('/create_enterprise/<int:tenant_id>', methods=['POST'])
@jwt_required()
def create_update_enterprise(tenant_id):
    try:
        data = request.get_json()
        tenant_id       = tenant_id
        enterprise_name = data['enterprise_name']
        enterprise_type = data['enterprise_type']

        existing_enterprise = DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).first()

        if existing_enterprise:
            existing_enterprise.enterprise_name = enterprise_name
            existing_enterprise.enterprise_type = enterprise_type
            message = "Enterprise setup updated successfully"

        else:
            new_enterprise = DefTenantEnterpriseSetup(
                tenant_id=tenant_id,
                enterprise_name=enterprise_name,
                enterprise_type=enterprise_type
            )

            db.session.add(new_enterprise)
            message = "Enterprise setup created successfully"

        db.session.commit()
        return make_response(jsonify({"message": message}), 200)

    except IntegrityError:
        return make_response(jsonify({"message": "Error creating or updating enterprise setup", "error": "Integrity error"}), 409)
    except Exception as e:
        return make_response(jsonify({"message": "Error creating or updating enterprise setup", "error": str(e)}), 500)



#Get all enterprise setups
@enterprises_bp.route('/get_enterprises', methods=['GET'])
@jwt_required()
def get_enterprises():
    try:
        setups = DefTenantEnterpriseSetup.query.order_by(DefTenantEnterpriseSetup.tenant_id.desc()).all()
        return make_response(jsonify([setup.json() for setup in setups]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving enterprise setups", "error": str(e)}), 500)

@enterprises_bp.route('/get_enterprises/v1', methods=['GET'])
def get_enterprises_v1():
    try:
        setups = DefTenantEnterpriseSetup.query.order_by(DefTenantEnterpriseSetup.tenant_id.desc()).all()
        return make_response(jsonify([setup.json() for setup in setups]), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving enterprise setups", "error": str(e)}), 500)


# Get one enterprise setup by tenant_id
@enterprises_bp.route('/get_enterprise/<int:tenant_id>', methods=['GET'])
@jwt_required()
def get_enterprise(tenant_id):
    try:
        setup = DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).first()
        if setup:
            return make_response(jsonify(setup.json()), 200)
        return make_response(jsonify({"message": "Enterprise setup not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error retrieving enterprise setup", "error": str(e)}), 500)


@enterprises_bp.route('/def_tenant_enterprise_setup/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def get_paginated_enterprises(page, limit):
    try:
        query = db.session.query(DefTenantEnterpriseSetupV).order_by(DefTenantEnterpriseSetupV.tenant_id.desc())
        paginated = query.paginate(page=page, per_page=limit, error_out=False)
        return jsonify({
            "items": [row.json() for row in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200
    except Exception as e:
        return jsonify({"message": "Error fetching paginated enterprises", "error": str(e)}), 500

# Update enterprise setup
@enterprises_bp.route('/update_enterprise/<int:tenant_id>', methods=['PUT'])
@jwt_required()
def update_enterprise(tenant_id):
    try:
        setup = DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).first()
        if setup:
            data = request.get_json()
            setup.enterprise_name = data.get('enterprise_name', setup.enterprise_name)
            setup.enterprise_type = data.get('enterprise_type', setup.enterprise_type)
            db.session.commit()
            return make_response(jsonify({"message": "Enterprise setup updated successfully"}), 200)
        return make_response(jsonify({"message": "Enterprise setup not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error updating enterprise setup", "error": str(e)}), 500)


# Delete enterprise setup
@enterprises_bp.route('/delete_enterprise/<int:tenant_id>', methods=['DELETE'])
@jwt_required()
def delete_enterprise(tenant_id):
    try:
        setup = DefTenantEnterpriseSetup.query.filter_by(tenant_id=tenant_id).first()
        if setup:
            db.session.delete(setup)
            db.session.commit()
            return make_response(jsonify({"message": "Enterprise setup deleted successfully"}), 200)
        return make_response(jsonify({"message": "Enterprise setup not found"}), 404)
    except Exception as e:
        return make_response(jsonify({"message": "Error deleting enterprise setup", "error": str(e)}), 500)

 

 



#get all tenants enterprise setups
@enterprises_bp.route('/enterprises', methods=['GET'])
@jwt_required()
def enterprises():
    try:
        # results = db.session.query(DefTenantEnterpriseSetupV).all()
        results = db.session.query(DefTenantEnterpriseSetupV).order_by(
            DefTenantEnterpriseSetupV.tenant_id.desc()
        ).all()
        
        if not results:
            return jsonify({'data': [], 'message': 'No records found'}), 200

        data = [row.json() for row in results]
        
        return jsonify(data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500



@enterprises_bp.route('/def_tenant_enterprise_setup/search/<int:page>/<int:limit>', methods=['GET'])
@jwt_required()
def search_enterprises(page, limit):
    try:
        search_query = request.args.get('enterprise_name', '').strip().lower()
        search_underscore = search_query.replace(' ', '_')
        search_space = search_query.replace('_', ' ')
        query = db.session.query(DefTenantEnterpriseSetupV)

        if search_query:
            query = query.filter(
                or_(
                    DefTenantEnterpriseSetupV.enterprise_name.ilike(f'%{search_query}%'),
                    DefTenantEnterpriseSetupV.enterprise_name.ilike(f'%{search_underscore}%'),
                    DefTenantEnterpriseSetupV.enterprise_name.ilike(f'%{search_space}%')
                )
            )

        paginated = query.order_by(DefTenantEnterpriseSetupV.tenant_id.desc()).paginate(page=page, per_page=limit, error_out=False)

        return make_response(jsonify({
            "items": [row.json() for row in paginated.items],
            "total": paginated.total,
            "pages": paginated.pages,
            "page": paginated.page
        }), 200)
    except Exception as e:
        return make_response(jsonify({"message": "Error searching enterprises", "error": str(e)}), 500)

