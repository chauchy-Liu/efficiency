# -*- coding: utf-8 -*-
"""
Created on Fri May 26 09:26:57 2023

@author: sunyan
"""
import pandas as pd
import datetime
from data.get_data import getData, getWindTurbines, wash_data_for_train
import importlib
from utils import time_util
from logging_config import init_loggers
# from app.utils import globalVariable

windFarm = 'xrHXLWFF' # 四期三一双馈
# windFarm = 'Mr78LFhE' # 二期联合动力 有发电机三相绕组   

algorithms = [
                # 'weathercock_freeze'
                # 'wind_speed_fault'
                # 'engine_env_temperature'
                # 'oar_machine_temperature'
                # 'oar_engine_temperature'
                # 'oar_electric_capacity_temperature'
               # 'blade_angle_not_balance'
              # 'chilunxiang_sanre'
              # 'weathercock_freeze'
               # 'chilunxiang_gaosu_zhoucheng_temperature'
                # 'engine_cabinet_temperature'
                # 'generator_temperature',
                # 'generator_zhuanju_kongzhi'
                # 'pianhang_duifeng_buzheng'
                # 'blade_freeze',
                # 'capacity_reduction'
                'pianhang_duifeng_buzheng'
              ]
    
def execute(name, assetId, assetIds, ratedPower):
    '''
    执行模型
    Parameters
    ----------
    name : str
        模型编码
    assetId : number
        目标风机
    assetIds : list
        全场风机
    Returns
    -------
    None.

    '''
    algorithm = importlib.import_module('algorithms.' + name)
    # 取数
    # startTime = datetime.datetime.strptime('2023-06-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    # endTime = datetime.datetime.strptime('2023-06-30 00:00:00', '%Y-%m-%d %H:%M:%S')
    
    endTime = datetime.datetime.now()
    if not time_util.is_lower_than_day(algorithm.time_duration):
        endTime = datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d 00:00:00'), '%Y-%m-%d %H:%M:%S')
    startTime = endTime - pd.to_timedelta(algorithm.time_duration)
    # 判断是否需要全场数据
    param_assetIds = [assetId]
    if algorithm.need_all_turbines:
        param_assetIds = assetIds
    data_df = getData(startTime, endTime, param_assetIds, algorithm)
    # data_df.to_csv(f'sample/{name}.csv')
    # data_df = wash_data_for_train(data_df, ratedPower, algorithm)
    # 清洗数据
    if not data_df.empty:
        final_data = algorithm.wash_data(data_df, ratedPower)
        # 跑模型
        abnormal_data = algorithm.judge_model(final_data, assetId)
        print(abnormal_data)
    else:
        print('数据为空')


if __name__ == '__main__':
    # # 初始化日志
    # init_loggers()
    
    # df_wind_turbine = getWindTurbines(windFarm)
    # assetIds = df_wind_turbine['mdmId']
    # for index, row in df_wind_turbine.iterrows():
    #     assetId = row['mdmId']
    #     ratedPower = row['ratedPower']
    #     print(assetId)
    #     for name in algorithms:
    #         execute(name, assetId, assetIds, ratedPower) # FIXME
    #     break
    # from flask import Flask
    # from app.api.algorithm_api import api
    # globalVariable._init()
    import asyncio
    import app
    # from uvicorn import run
    # app.debug = False
    # app.run(host='0.0.0.0', port=8889)
    # app = Flask(__name__)
    # app.register_blueprint(api)
    # app.run(host='172.17.11.119', port=8888, debug=True)
    # app.app.run(host='172.17.11.50', port=8889, debug=True) #172.17.11.119  127.0.0.1
    app.app.run(host='127.0.0.1', port=8889, debug=True) #172.17.11.119  127.0.0.
    # app.app.run(host='10.191.65.77', port=8889, debug=True) #172.17.11.119  127.0.0.
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(run(app.app, host='172.17.11.119', port=8888))