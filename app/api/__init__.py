from flask import Flask
from .algorithm_api import api


def register_api(app: Flask):
    app.register_blueprint(api)
