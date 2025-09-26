import flask
from flask import Blueprint, request, jsonify, json
import app.utils.data_util as data_util
import pandas as pd

import importlib
import time
import numpy as np

from app.utils.rest_response import success, error
import logging

api = Blueprint('api', __name__, url_prefix='/wind-algorithm-model')


data_cache = {}


@api.route('/')
def index():
    return {
        "msg": "success",
        "data": "welcome to use flask."
    }


@api.get('/test/execute')
def execute():
    try:
        from main_job import execute
        model_name = request.args.get('name')
        execute(model_name)
    except Exception as e:
        logging.getLogger().error(f'接口执行报错，错误信息：{str(e)}')


@api.get('/test/pushAlarm')
def pushAlarm():
    from alarms.alarm import push_alarm_v2
    assetId = request.args.get('assetId')
    alarmName = request.args.get('alarmName')
    alarmTime = request.args.get('alarmTime')
    error_start_time = request.args.get('error_start_time')
    error_end_time = request.args.get('error_end_time')

    push_alarm_v2(assetId, alarmName, alarmTime, error_start_time, error_end_time)


@api.get("/analyse")
def analyse():
    try:
        print('--------------------------------------------')
        modelCode = request.args.get('modelCode')
        print(modelCode)
        assetId = 'BYA2LVsH'
        data_df = pd.read_csv(f'sample/{modelCode}.csv')
        data_df.set_index('localtime',inplace=True)
        if modelCode == 'blade_angle_not_balance':
            curves = []
            curves.append({'name': '叶片1桨距角', 'data': list(data_df['WROT.Blade1Position'])})
            curves.append({'name': '叶片2桨距角', 'data': list(data_df['WROT.Blade2Position'])})
            curves.append({'name': '叶片3桨距角', 'data': list(data_df['WROT.Blade3Position'])})
            result = {
                'startTime': data_df.index.min(),
                'endTime': data_df.index.max(),
                'timeInterval': 'm',
                'timeIntervalValue': 10,
                'measurement': '角度',
                'multiDimensionData': curves
            }
            return success(result)
        elif modelCode == 'generator_temperature':
            curves = []
            curves.append({'name': '发电机定子U相线圈温度', 'data': list(data_df['WGEN.TemGenStaU'])})
            curves.append({'name': '发电机定子V相线圈温度', 'data': list(data_df['WGEN.TemGenStaV'])})
            curves.append({'name': '发电机定子W相线圈温度', 'data': list(data_df['WGEN.TemGenStaW'])})
            
            algorithm = importlib.import_module('algorithms.' + modelCode)
            predict_function = getattr(algorithm, 'predict_result')
            # FIXME
            if not modelCode in data_cache:
                predict_data = predict_function(data_df[['WGEN.GenActivePW','WNAC.TemOut','WNAC.TemNacelle','WGEN.LHDLGENAI31','WGEN.GenSpd','WNAC.WindSpeed','WGEN.LHDLGENAI103']].copy())
                data_cache[modelCode] = predict_data
                curves.append({'name': '健康温度', 'data': list(np.round(predict_data,2))})
            else:
                predict_data = data_cache[modelCode]
                curves.append({'name': '健康温度', 'data': list(np.round(predict_data,2))})
            result = {
                'startTime': data_df.index.min(),
                'endTime': data_df.index.max(),
                'timeInterval': 'm',
                'timeIntervalValue': 10,
                'measurement': '温度',
                'multiDimensionData': curves
            }
            return success(result)
        else:
            # 实际数据
            algorithm = importlib.import_module('algorithms.' + modelCode)
            target_column_name = algorithm.ai_points[len(algorithm.ai_points)-1]
            real_data = data_df[[target_column_name,'assetId']]
            print(real_data)
            real_data = real_data[real_data['assetId']==assetId]
            real_data.drop('assetId', inplace=True, axis=1)
            real_data = real_data[target_column_name]
            
            # 预测数据
            # TODO 需不需要清洗数据
            predict_function = getattr(algorithm, 'predict_result')
            # predict_data = predict_function(data_df)
            # print(type(predict_data))
            
            curves = []
            curves.append({'name': '实际温度', 'data': real_data.to_list()})
            if not modelCode in data_cache:
                predict_data = predict_function(data_df)
                data_cache[modelCode] = predict_data
                curves.append({'name': '健康温度', 'data': list(np.round(predict_data,2))})
            else:
                predict_data = data_cache[modelCode]
                curves.append({'name': '健康温度', 'data': list(np.round(predict_data,2))})
            result = {
                'startTime': data_df.index.min(),
                'endTime': data_df.index.max(),
                'timeInterval': 'm',
                'timeIntervalValue': 10,
                'measurement': '温度',
                'multiDimensionData': curves
            }
            return success(result)
    except Exception as e:
        print(e)
        return error(str(e))