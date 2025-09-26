from datetime import datetime, date
from flask_cors import CORS
from flask import Flask, request
from flask.json import JSONEncoder    #flask<=2.2
# from flask.json.provider import DefaultJSONProvider #JSONEncoder #flask>=2.3

from app.api import register_api

from main_job import schedule
from multiprocessing import Process
# from app.utils import globalVariable




app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
app.config['COMPRESS_LEVEL'] = 7
app.config['COMPRESS_MIN_SIZE'] = 5000
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['JSON_SORT_KEYS'] = False
# 注册蓝图
register_api(app)
CORS(app, resources=r'/*')


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return JSONEncoder.default(self, obj)

# class CustomJSONEncoder(DefaultJSONProvider):
#     def default(self, obj):
#         if isinstance(obj, datetime):
#             return obj.strftime('%Y-%m-%d %H:%M:%S')
#         elif isinstance(obj, date):
#             return obj.strftime('%Y-%m-%d')
#         else:
#             return DefaultJSONProvider.default(self, obj)

app.json_encoder = CustomJSONEncoder


process = Process(target=schedule)
process.start()

