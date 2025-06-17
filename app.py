from executors import flask_app
from flask_cors import CORS
from executors import flask_app
from api import register_blueprints



# Setup CORS
CORS(
    flask_app,
    resources={r"/*": {"origins": "http://localhost:5173"}},
    supports_credentials=True,
    allow_headers=["Content-Type", "Authorization"],
    expose_headers=["Content-Type", "Authorization"],
    methods=["GET", "POST", "PUT", "DELETE"]
)

# Register blueprints
register_blueprints(flask_app)
