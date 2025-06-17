# api/jwt.py
from flask_jwt_extended import JWTManager
from executors import flask_app

jwt = JWTManager(flask_app)
