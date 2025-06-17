from .jwt import jwt  # ensures JWT is initialized
from .controls import controls_bp
from .users import users_bp
from .access_models import access_models_bp
from .entitlements import entitlements_bp
from .globals import globals_bp
from .data_sources import data_sources_bp
from .notification import notification_bp
from .enterprises import enterprises_bp
from .access_profiles import access_profiles_bp
from .tasks import tasks_bp
from .access_point_elements import access_point_elements_bp
from .redis import reidis_bp

def register_blueprints(app):
    app.register_blueprint(controls_bp)
    app.register_blueprint(users_bp)
    app.register_blueprint(tasks_bp)
    app.register_blueprint(access_models_bp)
    app.register_blueprint(entitlements_bp)
    app.register_blueprint(globals_bp)
    app.register_blueprint(reidis_bp)
    app.register_blueprint(data_sources_bp)
    app.register_blueprint(notification_bp)
    app.register_blueprint(enterprises_bp)
    app.register_blueprint(access_profiles_bp)
    app.register_blueprint(access_point_elements_bp)
