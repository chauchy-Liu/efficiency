# -*- coding: utf-8 -*-
import logging.handlers
from apscheduler.schedulers.background import BackgroundScheduler,BlockingScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import algorithms
# from data.get_data import getData, getWindTurbines, getDataForMultiAlgorithms
from data.get_data_async import getWindTurbines, getDataForMultiAlgorithms, getWindTurbinesNode, getWindFarm
from configs.config import Wind_Farm, EXCEPT_MODLES, extraModelName, scheduleConfig, turbineConfig, algConfig, Wind_Farm_Name
from utils import time_util, display_util
import datetime
import importlib
import pandas as pd
import logging
from logging_config import init_loggers
import asyncio
from datetime import datetime
from db.db import UpdateTubineNum, UpdateAlgorithmInfo, UpdateResult, get_connection, InsertAlgorithmHead, InsertAlgorithmDetail, UpdateAlgorithmHead, ResetTubineNum, ReviewTubineNum, CheckThreshold
import os
import traceback
from collections import ChainMap
import configs.config as config
from copy import deepcopy
import warnings
import gc
import numpy as np
import faultcode.faultcode_SANY as faultcode_SANY
import faultcode.faultcode_GW as faultcode_GW
import faultcode.faultcode_MINYANG_GANSU_QINGSHUI as faultcode_MINYANG_GANSU_QINGSHUI 
import faultcode.faultcode_MINYANG_GANSU_TONGWEI as faultcode_MINYANG_GANSU_TONGWEI
import faultcode.faultcode_MINYANG as faultcode_MINYANG
import faultcode.faultcode_ENOS as faultcode_ENOS
import faultcode.faultcode_ZHONGCHE as faultcode_ZHONGCHE
import faultcode.faultcode_hadian as faultcode_hadian
import faultcode.faultcode_YUNDA as faultcode_YUNDA
import faultcode.faultcode_HZ as faultcode_HZ
import faultcode.faultcode_FD as faultcode_FD
import faultcode.faultcode_UP as faultcode_UP
import faultcode.faultcode_TZ as faultcode_TZ
import faultcode.faultcode_XUJI as faultcode_XUJI
# from data.get_data_async import Df_all_all_alltype, Df_all_m_all_alltype, zuobiao_all, Df_all_all, Df_all_m_all, yaw_time, turbine_err_all, turbine_param_all
import time
import data.efficiency_function as turbine_efficiency_function
import data.get_data_async as get_data_async
import pickle, gzip


warnings.simplefilter(action='ignore')
# warnings.simplefilter(action='ignore', category=FutureWarning)

# mysqlClient = get_connection()

# 多模型执行，保证执行频次相同、数据范围相同、依赖测点相似
async def execute_multi_algorithms(names: list, execute_time=''):
    # 0. 排除不执行的模型
    final_names = list(set(names) - set(EXCEPT_MODLES))
    # 检查model表里的sum>0时，只重置current
    # resetNames = ReviewTubineNum(final_names, 0)#mysqlClient, 
    # 1. 获取算法信息
    multi_algorithms = []
    multi_algorithms_config = {
        'farmId': None,
        'farmName': None,
        'typeName': None,
        'typeProcess': None,
        'Pwrat_Rate' : None,
        'rotor_radius' : None,
        'hub_high' : None,
        'ManufacturerID' : None,
        'state' : None,
        'fault_code' : None,
        'Turbine_attr': None,
        "Df_all_all_alltype" : pd.DataFrame(),
        "Df_all_m_all_alltype" : pd.DataFrame(),
        "zuobiao_all" : pd.DataFrame(),
        "Df_all_all" : pd.DataFrame(),
        "Df_all_m_all" : pd.DataFrame(),
        "yaw_time" : pd.DataFrame(),
        "turbine_err_all" : pd.DataFrame(),
        "turbine_param_all" : pd.DataFrame(),
        "zuobiao" : pd.DataFrame(),
        "pw_df_alltime" : pd.DataFrame(),
        "pw_turbine_all" : pd.DataFrame(),
        "windbinreg" : np.arange(1.75,25.25,0.5),
        "windbin" : np.arange(2.0,25.0,0.5),
        "wind_ti_all" : pd.DataFrame(),
        "wtids": None,
        'threshold': None,
        "neighbors_num" : None,
        "Turbine_attr_type" : None,
        "path": None,
        "minio_dir": str(time.time()).replace(".", "_"),
        "jobTime": datetime.now().strftime('%Y-%m-%d 00:00:00'),
        "algName": None
    }
    try:
        #日志设置
        #日志
        # logging.basicConfig(filemode='w')
        if len(execute_time) > 0:
            prefix = "当前任务调度周期量级"
            prefix = prefix + execute_time
        else:
            prefix = "当前任务为网页请求"
        if len(execute_time) == 0:
            #主日志
            mainLog = logging.getLogger("http-main")
        else:
            #主日志
            mainLog = logging.getLogger("job-main")
        if not mainLog.handlers:
            mainLog.setLevel(level=logging.INFO)
            mainsh = logging.StreamHandler()
            mainLog.addHandler(mainsh)
            director = os.path.dirname(os.path.abspath(__file__))
            # mainfh = logging.FileHandler(filename=os.path.join(director, "logs","main"+".log"), mode='w')
            # mainfh.setLevel(level=logging.INFO)
            # mainLog.addHandler(mainfh)
            if len(execute_time) == 0:
                #主日志
                mainrfh = logging.handlers.RotatingFileHandler(filename=os.path.join(director, "logs","http-main"+".log"), mode='a', maxBytes=5*1024**2, backupCount=3)
            else:
                #主日志
                mainrfh = logging.handlers.RotatingFileHandler(filename=os.path.join(director, "logs","job-main"+".log"), mode='a', maxBytes=5*1024**2, backupCount=3)
            mainrfh.setLevel(level=logging.INFO)
            mainLog.addHandler(mainrfh)
            format = f'[{prefix}]: - %(asctime)s - %(levelname)s - %(message)s' 
            log_format = logging.Formatter(fmt=format)
            mainsh.setFormatter(log_format)
            # mainfh.setFormatter(log_format)
            mainrfh.setFormatter(log_format)
        mainLog.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        mainLog.info("+++++++++++++++++++++++++++++++++++++++++++新任务++++++++++++++++++++++++++++++++++++++++++")
        mainLog.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

        for name in final_names:
            multi_algorithms.append(importlib.import_module('.' + name, package='algorithms'))
            name_convice = multi_algorithms[-1].__name__.split('.')[-1]
            if "pianhang_duifeng_buzheng" == name_convice:
                endTime = datetime.strptime(datetime.now().strftime('%Y-%m-%d 00:00:00'), '%Y-%m-%d %H:%M:%S')
            else:
                endTime = datetime.now()#datetime.strptime(datetime.now().strftime('%Y-%m-%d 00:00:00'), '%Y-%m-%d %H:%M:%S')
            # endTime = datetime.strptime('2024-6-24 03:31:48', '%Y-%m-%d %H:%M:%S')#datetime.now()#.strftime('%Y-%m-%d %H:%M:%S')
            startTime = endTime - pd.to_timedelta(multi_algorithms[-1].time_duration)
            multi_algorithms_config[name_convice] = {
                'startTime':startTime, 
                'endTime': endTime, 
                #'resampleTime':multi_algorithms[-1].resample_interval, 
                'aiPoints':deepcopy(multi_algorithms[-1].ai_points), 
                "diPoints":deepcopy(multi_algorithms[-1].di_points), 
                "cjDiPoints":deepcopy(multi_algorithms[-1].cj_di_points),
                "tyDiPoints":deepcopy(multi_algorithms[-1].ty_di_points),
                "generalPoints":deepcopy(multi_algorithms[-1].general_points), 
                "privatePoints":deepcopy(multi_algorithms[-1].private_points),
                "executeTime": execute_time
            }
        
        # 多模型执行
        # await _do_execute(multi_algorithms, startTime, endTime)
        await _do_execute(multi_algorithms, multi_algorithms_config, mainLog)
    except Exception as e:
        errorInfomation = traceback.format_exc()
        mainLog.info(f'\033[31m{errorInfomation}\033[0m')
        mainLog.info(f'\033[33m顶层函数execute_multi_algorithms补获异常：{e}\033[0m')
    
    


async def execute(name):
    logging.getLogger(name).info(f'开始执行模型:{name}')
    # 1. 获取算法信息
    algorithm = importlib.import_module('.' + name, package='algorithms')
    # 开始结束时间 FIXME 拿到外边去
    endTime = datetime.now()
    if not time_util.is_lower_than_day(algorithm.time_duration):
        endTime = datetime.strptime(datetime.now().strftime('%Y-%m-%d 00:00:00'), '%Y-%m-%d %H:%M:%S')
    startTime = endTime - pd.to_timedelta(algorithm.time_duration)
    await _do_execute([algorithm], startTime, endTime)
    logging.getLogger(name).info(f'结束执行模型:{name}')
    


    

# 每个算法执行函数 算法维度
async def _do_execute(multi_algorithms, algorithms_configs, mainLog): #startTime, endTime
    #日志配置
    director = os.path.dirname(os.path.abspath(__file__))
    if len(algorithms_configs[list(algorithms_configs.keys())[-1]]["executeTime"]) > 0:
        prefix = "当前任务调度周期量级"
        prefix = prefix + algorithms_configs[list(algorithms_configs.keys())[-1]]["executeTime"]
    else:
        prefix = "当前任务为网页请求"
    format = f'[{prefix}]: - %(asctime)s - %(levelname)s - %(message)s' 
    log_format = logging.Formatter(fmt=format)
    #算法日志
    algorithmLogs = {}
    algorithmshs = {} #控制台打印
    algorithmfhs = {} #输出到文件
    algorithmrfhs = {} #输出到文件，且文件可以根据大小分割
    # modelIds = []
    for algorithm in multi_algorithms:
        name = algorithm.__name__.split('.')[-1]
        # if hasattr(algorithm, 'modelId'):
        #     modelIds.append(algorithm.modelId)
        if len(algorithms_configs[list(algorithms_configs.keys())[-1]]["executeTime"]) == 0:
            algorithmLogs[name] = logging.getLogger('http-'+name)
        else:
            algorithmLogs[name] = logging.getLogger('job-'+name)
        if not algorithmLogs[name].handlers:
            algorithmLogs[name].setLevel(level=logging.INFO)
            # algorithmshs[name] = logging.StreamHandler()
            # algorithmLogs[name].addHandler(algorithmshs[name])
            # algorithmfhs[name] = logging.FileHandler(filename=os.path.join(director, "logs",name+".log"), mode='w')
            # algorithmfhs[name].setLevel(level=logging.INFO)
            if len(algorithms_configs[list(algorithms_configs.keys())[-1]]["executeTime"]) == 0:
                algorithmrfhs[name] = logging.handlers.RotatingFileHandler(filename=os.path.join(director, "logs",'http-'+name+".log"), mode='a', maxBytes=2*1024**2, backupCount=3)
            else:
                algorithmrfhs[name] = logging.handlers.RotatingFileHandler(filename=os.path.join(director, "logs",'job-'+name+".log"), mode='a', maxBytes=2*1024**2, backupCount=3)
            algorithmrfhs[name].setLevel(level=logging.INFO)
            # algorithmLogs[name].addHandler(algorithmfhs[name])
            algorithmLogs[name].addHandler(algorithmrfhs[name])
            # algorithmshs[name].setFormatter(log_format)
            # algorithmfhs[name].setFormatter(log_format)
            algorithmrfhs[name].setFormatter(log_format)
        algorithmLogs[name].info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        algorithmLogs[name].info("+++++++++++++++++++++++++++++++++++++++++++新任务++++++++++++++++++++++++++++++++++++++++++")
        algorithmLogs[name].info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    # 获取风场
    algorithms_configs['farmId'] = Wind_Farm
    algorithms_configs['farmName'] = Wind_Farm_Name
    WindFarm_attr = await getWindFarm(Wind_Farm)
    path_farm = str('result/')+str(WindFarm_attr.loc[0,'二级公司'])+'/'+str(WindFarm_attr.loc[0,'风电场名'])
    if os.path.exists(path_farm)==False:
        os.makedirs(path_farm)
    WindFarm_attr.to_csv(str(path_farm+'/WindFarm.csv'),index=True, encoding='utf-8') 
    algorithms_configs['farmName'] = WindFarm_attr.loc[0,'风电场名']
    #  获取所有风机
    Turbine_attr = await getWindTurbines(Wind_Farm)
    algorithms_configs['Turbine_attr'] = Turbine_attr
    algorithms_configs['Df_all_all_alltype'] = pd.DataFrame()
    algorithms_configs['Df_all_m_all_alltype'] = pd.DataFrame()
    algorithms_configs['zuobiao_all'] = pd.DataFrame()
    for i_type in range(len(np.unique(Turbine_attr['turbineTypeID']))):
        algorithms_configs['Turbine_attr_type'] = Turbine_attr[Turbine_attr['turbineTypeID'] == np.unique(Turbine_attr['turbineTypeID'])[i_type]]
        algorithms_configs['typeProcess'] = i_type
        algorithms_configs['Turbine_attr_type'] = algorithms_configs['Turbine_attr_type'].reset_index()
        algorithms_configs['path'] = str(str(path_farm)+'/'+str(np.unique(Turbine_attr['turbineTypeID'])[i_type]))
        if os.path.exists(algorithms_configs['path'])==False:
            os.makedirs(algorithms_configs['path'])
        algorithms_configs['Pwrat_Rate'] = algorithms_configs['Turbine_attr_type'].loc[0,'ratedPower']#['ratedPower']
        if('rotorDiameter' in algorithms_configs['Turbine_attr_type']): #.loc[0,'attributes']
            algorithms_configs['rotor_radius'] = algorithms_configs['Turbine_attr_type'].loc[0,'rotorDiameter']*0.5#['rotorDiameter']*0.5
        else:
            algorithms_configs['rotor_radius'] = 131*0.5
        algorithms_configs['hub_high'] = algorithms_configs['Turbine_attr_type'].loc[0,'hubHeight']#['hubHeight']

        
        algorithms_configs['ManufacturerID'] = str(np.unique(Turbine_attr['turbineTypeID'])[i_type])[:2]
        algorithms_configs['typeName'] = str(np.unique(Turbine_attr['turbineTypeID'])[i_type])
        if algorithms_configs['ManufacturerID']=='GW':#金风5，哈电6，三一64，远景5，明阳3,海装32,运达6,中车0
            algorithms_configs['state'] = 5 
            algorithms_configs['fault_code'] = faultcode_GW.fault
        elif algorithms_configs['ManufacturerID']=='XE':#哈电6
            algorithms_configs['state'] = 6
            algorithms_configs['fault_code'] = faultcode_hadian.fault
        elif (algorithms_configs['ManufacturerID']=='SE'):#三一64,吉电三骏120
            algorithms_configs['state'] = 64
            algorithms_configs['fault_code'] = faultcode_SANY.fault
        elif (algorithms_configs['ManufacturerID']=='SI'):#三一64,吉电三骏120
            algorithms_configs['state'] = 120
            algorithms_configs['fault_code'] = faultcode_SANY.fault
        elif algorithms_configs['ManufacturerID']=='EN':#远景5,21
            algorithms_configs['state'] = 5
            algorithms_configs['fault_code'] = faultcode_ENOS.fault
        elif (algorithms_configs['ManufacturerID']=='MY'):
            algorithms_configs['state'] = 14
            algorithms_configs['fault_code'] = faultcode_MINYANG.fault
        elif (algorithms_configs['ManufacturerID']=='My'):#明阳3,MySE8.5-230为14
            #'''
            algorithms_configs['state'] = 3
            algorithms_configs['fault_code'] = faultcode_MINYANG_GANSU_QINGSHUI.fault
            algorithms_configs['state_code'] = faultcode_MINYANG_GANSU_QINGSHUI.state
            '''
            algorithms_configs['state'] = 3
            algorithms_configs['fault_code'] = faultcode_MINYANG_GANSU_TONGWEI.fault
            algorithms_configs['state_code'] = faultcode_MINYANG_GANSU_TONGWEI.state
            '''
            algorithms_configs['wspd'] = faultcode_MINYANG_GANSU_QINGSHUI.wspd
            algorithms_configs['pwrat'] = faultcode_MINYANG_GANSU_QINGSHUI.pwrat
            algorithms_configs['Rotspd_Connect'] = faultcode_MINYANG_GANSU_QINGSHUI.Rotspd_Connect
            algorithms_configs['Rotspd_Rate'] = faultcode_MINYANG_GANSU_QINGSHUI.Rotspd_Rate
            algorithms_configs['Pitch_Min'] = faultcode_MINYANG_GANSU_QINGSHUI.Pitch_Min
        elif (algorithms_configs['ManufacturerID']=='H1')|(algorithms_configs['ManufacturerID']=='HZ'):#海装32
            algorithms_configs['state'] = 32
            algorithms_configs['fault_code'] = faultcode_HZ.fault
        elif algorithms_configs['ManufacturerID']=='WD':#运达6
            algorithms_configs['state'] = 6
            algorithms_configs['fault_code'] = faultcode_YUNDA.fault
        elif algorithms_configs['ManufacturerID']=='WT':#中车0，10MW为8
            #state = 1
            #fault_code = faultcode_ZHONGCHE.fault
            algorithms_configs['state'] = 14##许继机组
            algorithms_configs['fault_code'] = faultcode_XUJI.fault
        elif algorithms_configs['ManufacturerID']=='FD':#中车0
            algorithms_configs['state'] = 2
            algorithms_configs['fault_code'] = faultcode_FD.fault
        elif algorithms_configs['ManufacturerID']=='UP':#中车0
            algorithms_configs['state'] = 5
            algorithms_configs['fault_code'] = faultcode_UP.fault
        elif (algorithms_configs['ManufacturerID']=='W2')|(algorithms_configs['ManufacturerID']=='W3')|(algorithms_configs['ManufacturerID']=='W4')|(algorithms_configs['ManufacturerID']=='EW'):#上气
            algorithms_configs['state'] = 8
            algorithms_configs['fault_code'] = faultcode_HZ.fault
        elif algorithms_configs['ManufacturerID']=='XW':#中车0
            algorithms_configs['state'] = 14
            algorithms_configs['fault_code'] = faultcode_UP.fault
        elif algorithms_configs['ManufacturerID']=='TZ':#中车0
            algorithms_configs['state'] = 13
            algorithms_configs['fault_code'] = faultcode_TZ.fault
        else:
            pass
            
         
        #pw_df_hadian = pd.DataFrame()
        #state_all_all = pd.DataFrame()
        #fault_all_all = pd.DataFrame()
        algorithms_configs['Df_all_all'] = pd.DataFrame()
        algorithms_configs['Df_all_m_all'] = pd.DataFrame()
        algorithms_configs['yaw_time'] = pd.DataFrame()
        
        
        
        ####修改计算机组台数
        #Turbine_attr_type = Turbine_attr_type.iloc[[21],:]
        #Turbine_attr_type = Turbine_attr_type.iloc[0:1,:]
        #####
        algorithms_configs['Turbine_attr_type'] = algorithms_configs['Turbine_attr_type'].reset_index()
        algorithms_configs['turbine_err_all'] = pd.DataFrame()
        algorithms_configs['turbine_err_all']['wtid'] = algorithms_configs['Turbine_attr_type'].loc[:,'name']
        algorithms_configs['turbine_err_all']['power_rate_err'] = 0  #额定功率异常
        algorithms_configs['turbine_err_all']['wspd_power_err'] = 0  #风速功率散点异常
        algorithms_configs['turbine_err_all']['torque_kopt_err'] = 0 #最佳Cp段转矩控制异常
        algorithms_configs['turbine_err_all']['torque_rate_err'] = 0 #额定转速段转矩控制异常
        algorithms_configs['turbine_err_all']['yaw_duifeng_err'] = 0 #对风偏航角度过大
        algorithms_configs['turbine_err_all']['yaw_leiji_err'] = 0 #偏航控制误差过大
        algorithms_configs['turbine_err_all']['pitch_min_err'] = 0 #最小桨距角异常
        algorithms_configs['turbine_err_all']['pitch_control_err'] = 0 #变桨控制异常
        algorithms_configs['turbine_err_all']['pitch_balance_err'] = 0 #三叶片变桨不平衡
        
        algorithms_configs['turbine_err_all']['power_rate_loss'] = 0
        algorithms_configs['turbine_err_all']['wspd_power_loss'] = 0
        algorithms_configs['turbine_err_all']['torque_kopt_loss'] = 0
        algorithms_configs['turbine_err_all']['torque_rate_loss'] = 0
        algorithms_configs['turbine_err_all']['yaw_duifeng_loss'] = 0
        algorithms_configs['turbine_err_all']['yaw_leiji_loss'] = 0
        algorithms_configs['turbine_err_all']['pitch_min_loss'] = 0
        algorithms_configs['turbine_err_all']['pitch_control_loss'] = 0
        algorithms_configs['turbine_err_all']['pitch_balance_loss'] = 0
        algorithms_configs['turbine_err_all']['pwrat_order'] = 1
        
        algorithms_configs['turbine_param_all'] = pd.DataFrame()

        # 运行单机型预警项目的主体逻辑函数
        await algorithms_turbines_stream(mainLog, algorithmLogs, multi_algorithms, algorithms_configs)#algorithms_configs['Turbine_attr_type']

        
    gc.collect()
    mainLog.info(f'##############################################\!\!\!本次请求的所有模型已跑完,注：本次请求的mainlog日志会覆盖上次请求的日志!\!\!################################################')


async def algorithms_turbines_stream(mainLog, algorithmLogs, multi_algorithms, algorithms_configs): #df_wind_turbine, 
    # 控制风机数量
    if 'changeMeasurePointQueue' in algorithms_configs[multi_algorithms[-1].__name__.split('.')[-1]]:
        turbineNameList = []
        for key, value in algorithms_configs[multi_algorithms[-1].__name__.split('.')[-1]]['changeMeasurePointQueue'].items():
            turbineNameList.append(key)
    else:
        turbineNameList = turbineConfig['turbineNameList'] #["16#"]#
    if turbineNameList != None and len(turbineNameList) > 0:
        algorithms_configs['Turbine_attr_type'] = algorithms_configs['Turbine_attr_type'][algorithms_configs['Turbine_attr_type']["name"].isin(turbineNameList)]
    # df_wind_turbine = df_wind_turbine.iloc[0:3]
    # df_wind_turbine = df_wind_turbine.iloc[[2]]

    #将model表里上次和这次执行对应算法的sum_num和current_num置回初始位
    # ResetTubineNum(list_names, df_wind_turbine.shape[0],0)#mysqlClient,
    mainLog.info(f'每次请求时重置algorithm_model表里的对应算法的风机总数和当前执行完算法台数')
    for algorithm_i in multi_algorithms:
        name_i = algorithm_i.__name__.split('.')[-1]
        algorithmLogs[name_i].info(f'每次请求时重置algorithm_model表里的对应算法的风机总数和当前执行完算法台数')

    
    mainLog.info(f'开始执行算法{multi_algorithms}') #，执行时间范围{startTime}-{endTime}
    mainLog.info(f'获取风场{Wind_Farm}的风机数量为'+str(algorithms_configs['Turbine_attr_type'].shape[0]))
    assetIds = algorithms_configs['Turbine_attr_type']['mdmId']
    #获取每个风机私有节点id
    # try:
    #     multiModelAssetIds = await getWindTurbinesNode(assetIds, algorithms_configs, nameConstrain=extraModelName) #一个风机可能会有多个模型资产Id
    # except Exception as e:
    #     errorInfomation = traceback.format_exc()
    #     mainLog.info(f'\033[31m{errorInfomation}\033[0m')
    #     mainLog.info(f'\033[33m获取私有测点时发生异常：{e}\033[0m')
    #     # endAlgorithmTime = datetime.now()
    #     multiModelAssetIds = [[]]*df_wind_turbine.shape[0]

    # 2. 获取每个算法的阈值, 并调整不同周期的调度适配的算法参数
    mainLog.info(f'获取每个算法的阈值, 并调整不同周期的调度适配的算法参数')
    try:
        # algorithms_configs = CheckThreshold(multi_algorithms, algorithms_configs)#mysqlClient, 
        for algorithm_i in multi_algorithms:
            name_i = algorithm_i.__name__.split('.')[-1]
            threshold = algConfig[name_i]['threshold']
            if type(threshold) == type(None) or len(threshold) == 0:
                algorithms_configs[name_i]['threshold'] = {}
            else:
                algorithms_configs[name_i]['threshold'] = threshold
            algorithmLogs[name_i].info(f'获取每个算法的阈值, 并调整不同周期的调度适配的算法参数')
            if len(algorithms_configs[name_i]["threshold"]) > 0:
                if len(algorithms_configs[name_i]['executeTime']) > 0:
                    if algorithms_configs[name_i]['executeTime'] == 'static':
                        pass
                    else:
                        algorithm_i.resample_interval = algorithms_configs[name_i]["threshold"]["executeTime"][algorithms_configs[name_i]['executeTime']]['kelidu']
                        algorithm_i.time_duration = algorithms_configs[name_i]["threshold"]["executeTime"][algorithms_configs[name_i]['executeTime']]['duration']
                        algorithm_i.error_data_time_duration = algorithms_configs[name_i]["threshold"]["executeTime"][algorithms_configs[name_i]['executeTime']]['continue']
                        algorithms_configs[name_i]["threshold"]["levelline"] = algorithms_configs[name_i]["threshold"]["executeTime"][algorithms_configs[name_i]['executeTime']]['levelline']
                        algorithms_configs[name_i]["threshold"]["executeTimeValue"] = algorithms_configs[name_i]['executeTime']
                else:
                    algorithms_configs[name_i]["threshold"]["executeTimeValue"] = 'day'
            else:
                pass
            if 'changeMeasurePointQueue' in algorithms_configs[name_i]:
                algorithm_i.resample_interval = algConfig[name_i]['changeDateFreq']
                algorithm_i.time_duration = algConfig[name_i]['changeDateRange']
                algorithm_i.error_data_time_duration = algConfig[name_i]['changeErrorContinue']
            if 'changeMeasurePointQueue' in algConfig[name_i] and algConfig[name_i]['changeDateRange'] != algorithm_i.time_duration:
                algorithms_configs[name_i]['changeMeasurePointQueue'] = {}
            algorithms_configs[name_i]['resampleTime'] = algorithm_i.resample_interval
            algorithms_configs[name_i]['startTime'] = algorithms_configs[name_i]['endTime'] - pd.to_timedelta(algorithm_i.time_duration)
    except Exception as e:
        errorInfomation = traceback.format_exc()
        mainLog.info(f'\033[31m{errorInfomation}\033[0m')
        mainLog.info(f'\033[33m获取私有测点时发生异常：{e}\033[0m')
        # endAlgorithmTime = datetime.now()
        # multiModelAssetIds = [[]]*df_wind_turbine.shape[0]
    
    # 3. 串行执行模型
    all_total_no_alarm_models = []    # 所有算法,所有台正常执行、有数据、无告警模型
    all_total_alarm_models = []       # 所有算法,所有台正常执行、有数据、有告警模型
    all_total_exception_models = []   # 所有算法,所有台执行发生异常模型
    all_total_data_empty_models = []  # 所有算法,所有台正常执行、无数据的模型
    algorithm_index = 0
    
    for algorithm in multi_algorithms:
        total_no_alarm_models = []    # 所有台正常执行、有数据、无告警模型
        total_alarm_models = []       # 所有台正常执行、有数据、有告警模型
        total_exception_models = []   # 所有台执行发生异常模型
        total_data_empty_models = []  # 所有台正常执行、无数据的模型
        total_no_alarm_turbines = []    # 所有台正常执行、有数据、无告警模型
        total_alarm_turbines = []       # 所有台正常执行、有数据、有告警模型
        total_exception_turbines = []   # 所有台执行发生异常模型
        total_data_empty_turbines = []  # 所有台正常执行、无数据的模型
        turbineIndex = 0             #风机计数序号
        idMaps = {}                   #算法名和任务id映射表

        #算法名
        name = algorithm.__name__.split('.')[-1]
        algorithms_configs['algName'] = name
        algorithm_index += 1
        mainLog.info(f'===================================开始准备{name}预警模型，模型已完成的数量进度{algorithm_index}/{len(multi_algorithms)}==============================================')
        data_df = {'data_df':pd.DataFrame()}
        # 2. 每台风机执行算法
        for index, row in algorithms_configs['Turbine_attr_type'].iterrows():
            no_alarm_models = []    # 正常执行、有数据、无告警模型
            alarm_models = []       # 正常执行、有数据、有告警模型
            exception_models = []   # 执行发生异常模型
            data_empty_models = []  # 正常执行、无数据的模型
            startTurbineTime = datetime.now()

            turbineIndex += 1
            assetId = row['mdmId'] #风机id
            turbineName = row['name']
            ratedPower = row['ratedPower']
            mainLog.info(f'-----------------------------------开始执行风机{assetId}预警模型{name}: {turbineIndex}/'+str(algorithms_configs['Turbine_attr_type'].shape[0])+'----------------------------------------------')
            # 判断是否需要全场数据
            # if algConfig[name]['need_all_turbines'] == False:
            data_df['data_df'] = pd.DataFrame()
            for algorithm_i in [algorithm]:
                name_i = algorithm_i.__name__.split('.')[-1]
                algorithmLogs[name_i].info(f'判断{name_i}算法是否需要全风机数据还是单风机数据')
                if algorithm_i.need_all_turbines:
                # param_assetIds = assetIds
                # param_private_assetIds = multiModelAssetIds#[index]
                    #添加任务
                    mainLog.info(f"需要全场风机：算法名>{name_i},风机名>{turbineName}")
                    algorithmLogs[name].info(f"需要全场风机：算法名>{name_i},风机名>{turbineName}")
                    if turbineIndex == algorithms_configs['Turbine_attr_type'].shape[0]:
                        algorithms_configs[name_i]['PrepareTurbines'] = True
                        # algorithms_configs[name_i]['param_assetIds']  += [assetId] #= assetIds.tolist() #[assetId]
                        # algorithms_configs[name_i]['param_turbine_num'] += [turbineName]
                        
                    elif turbineIndex == 1:
                        algorithms_configs[name_i]['PrepareTurbines'] = False
                        # algorithms_configs[name_i]['param_private_assetIds'] = [multiModelAssetIds[turbineIndex-1][name_i]]
                        # algorithms_configs[name_i]['param_turbine_num'] = [turbineName]
                        # algorithms_configs[name_i]['param_assetIds'] = [assetId]
                    else:
                        algorithms_configs[name_i]['PrepareTurbines'] = False
                        # algorithms_configs[name_i]['param_private_assetIds'] += [multiModelAssetIds[turbineIndex-1][name_i]]#[multiModelAssetIds[index]]
                        # algorithms_configs[name_i]['param_assetIds'] += [assetId]
                        # algorithms_configs[name_i]['param_turbine_num'] += [turbineName]
                else:
                    algorithms_configs[name_i]['PrepareTurbines'] = True
                algorithms_configs[name_i]['param_private_assetIds'] = [[]]*algorithms_configs['Turbine_attr_type'].shape[0]#[multiModelAssetIds[turbineIndex-1][name_i]]
                algorithms_configs[name_i]['param_assetIds'] = [assetId]
                algorithms_configs[name_i]['param_turbine_num'] = [turbineName]
            ################################
            #设置单台风机需要重新引入新的数据范围的测点都有哪些
            if 'changeMeasurePointQueue' in algConfig[name] and algConfig[name]['changeDateRange'] == algorithm.time_duration:
                if len(algorithm.private_points) > 0:
                    for modelKey, pointValue in algorithm.private_points.items():
                        algorithm.private_points[modelKey] = [i for i in algorithms_configs[name]['changeMeasurePointQueue'][turbineName]]
            ################################
            #测点分批次处理，每批测点有相同的数据时间范围和采样频率
            if 'changeMeasurePointQueue' in algConfig[name] and algConfig[name]['changeDateRange'] != algorithm.time_duration:
                #对当前风机初始化为空列表
                algorithms_configs[name]['changeMeasurePointQueue'][turbineName] = deepcopy(algConfig[name]['changeMeasurePointQueue'])
                await single_measure_point_batch(mainLog, turbineName, algorithms_configs, name, algorithm, assetId, algorithmLogs, data_df, idMaps, turbineIndex, algorithms_configs['Turbine_attr_type'], startTurbineTime, ratedPower, row, data_empty_models, no_alarm_models, exception_models, alarm_models, total_data_empty_models,total_alarm_models,total_no_alarm_models,total_exception_models,total_data_empty_turbines, total_no_alarm_turbines, total_alarm_turbines, total_exception_turbines)
            else:
                await single_measure_point_batch(mainLog, turbineName, algorithms_configs, name, algorithm, assetId, algorithmLogs, data_df, idMaps, turbineIndex, algorithms_configs['Turbine_attr_type'], startTurbineTime, ratedPower, row, data_empty_models, no_alarm_models, exception_models, alarm_models, total_data_empty_models,total_alarm_models,total_no_alarm_models,total_exception_models,total_data_empty_turbines, total_no_alarm_turbines, total_alarm_turbines, total_exception_turbines)

        all_total_data_empty_models += total_data_empty_models
        all_total_alarm_models += total_alarm_models
        all_total_no_alarm_models += total_no_alarm_models
        all_total_exception_models += total_exception_models

        
        mainLog.info(f'预警模型<{name}>执行完成所有风机<'+str(algorithms_configs['Turbine_attr_type'].shape[0])+f'>：\n\
                                正常执行、有数据、无告警模型有{total_no_alarm_turbines},\n\
                                正常执行、有数据、有告警模型有{total_alarm_turbines},\n\
                                正常执行、无数据的模型有{total_data_empty_turbines},\n\
                                执行发生异常模型有{total_exception_turbines}')
        
        #更新algorithm_model表部分字段
        mainLog.info(f'model表，模型{[algorithm]}: 更新algorithm_model模型状态字段')
        # UpdateAlgorithmInfo([algorithm], total_no_alarm_models, total_alarm_models, total_exception_models+total_data_empty_models)#mysqlClient, 

        #更新algorithm_execute_head表
        # UpdateAlgorithmHead(idMaps)#mysqlClient, 
        
    #更新algorithm_model表部分字段
    mainLog.info(f'model表，所有模型{multi_algorithms}: 更新algorithm_model表content字段')
    # UpdateResult(multi_algorithms, all_total_no_alarm_models, all_total_alarm_models, all_total_exception_models, all_total_data_empty_models)#mysqlClient, 

async def single_measure_point_batch(mainLog, turbineName, algorithms_configs, name, algorithm, assetId, algorithmLogs, data_df, idMaps, turbineIndex, df_wind_turbine, startTurbineTime, ratedPower, row, data_empty_models, no_alarm_models, exception_models, alarm_models, total_data_empty_models,total_alarm_models,total_no_alarm_models,total_exception_models,total_data_empty_turbines, total_no_alarm_turbines, total_alarm_turbines, total_exception_turbines):
    '''
    mainlog:主程序日志
    turbineName: 风机号
    algorithms_configs: 主程序中的算法配置文件
    name: 算法名
    algorithm: 算法对象
    assetId: 风机ID
    algorithmLogs: 算法日志
    idMaps: 任务Id映射表
    turbineIndex: 风机进度号
    df_wind_turbine: 风机属性
    startTurbineTime: 开始执行本风机时间
    ratedPower: 额定功率
    row: 一条风机属性
    data_empty_models: 空数据列表
    no_alarm_models: 无告警列表
    exception_models: 异常列表
    alarm_models: 告警列表
    total_data_empty_models: 所有风机的空数据模型列表
    total_alarm_models: 所有风机的告警模型列表
    total_no_alarm_models: 所有风机的无告警模型列表
    total_exception_models: 所有风机的异常报错模型列表
    total_data_empty_turbines: 所有风机的空数据的风机号列表
    total_no_alarm_turbines: 所有风机的无告警的风机号列表
    total_alarm_turbines: 所有风机的有告警的风机号列表
    total_exception_turbines: 所有风机的异常报错的风机号列表
    '''
    # 3. 每个算法统一获取单台或多台数据
    # data_df =  await getDataForMultiAlgorithms(startTime, endTime, param_assetIds, param_private_assetIds, multi_algorithms)#await
    mainLog.info(f'获取{turbineName}风机单算法需要的数据:')
    try:
        endTime = algorithms_configs[name]['endTime']#datetime.now()
        date = endTime.date()
        days = str(date).split('-')[2]
        if days[0] == '0':
            days = days[1]
        # if name != 'Efficiency_ana_V3' or (name == 'Efficiency_ana_V3' and days == '1') or (name == 'Efficiency_ana_V3' and not os.path.exists('Efficiency_ana_V3.pkl.gz')):
        algorithmData = await getDataForMultiAlgorithms({name:algorithms_configs[name]}) #mainLog, algorithmLogs, 
        # else:
        #     if algorithms_configs[name]['PrepareTurbines'] == True:
        #         algorithmData = {name: pd.read_pickle('Efficiency_ana_V3.pkl.gz')}
        #     else:
        #         algorithmData = {name: pd.DataFrame()}
    except Exception as e:
        errorInfomation = traceback.format_exc()
        mainLog.info(f'\033[31m{errorInfomation}\033[0m')
        mainLog.info(f'\033[33m风机{assetId}在执行模型{name}时发生异常：{e}\033[0m')
        algorithmLogs[name].info(f'\033[31m{errorInfomation}\033[0m')
        algorithmLogs[name].info(f'\033[33m风机{assetId}在执行模型{name}时发生异常：{e}\033[0m')

    #算法名1, 算法名2, ... 
    #数据1, 数据2, ...
    
    # # 4. 串行执行模型
    # no_alarm_models = []    # 正常执行、有数据、无告警模型
    # alarm_models = []       # 正常执行、有数据、有告警模型
    # exception_models = []   # 执行发生异常模型
    # data_empty_models = []  # 正常执行、无数据的模型
    # startTurbineTime = datetime.now()
# for algorithm in multi_algorithms:
    startAlgorithmTime = datetime.now()
    mainLog.info(f'~~~~~~~~~~~~~~~~~~~~~~~~~~~~{turbineName}台风机执行算法{name}~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    algorithmLogs[name].info(f'{name}开始执行风机{assetId}')
    # if name not in idMaps.keys():
        #添加任务
        # mainLog.info(f"head表，添加任务algorithm_execute_head：算法名>{name},风机名>{turbineName}")
        # algorithmLogs[name].info(f"head表，添加任务algorithm_execute_head：算法名>{name},风机名>{turbineName}")
        # taskId = InsertAlgorithmHead(name, startAlgorithmTime, algorithms_configs[name]['startTime'], algorithms_configs[name]['endTime'])#
        #任务id映射
        # idMaps[name] = str(taskId)
    #提取算法对应的数据
    if name in algorithmData.keys():
        # 清洗数据
        get_data_async.wash_data(algorithmData[name], algorithms_configs)
        # data_df['data_df'] = pd.concat([data_df['data_df'], algorithmData[name]])
    # else:
        # data_df['data_df'] = pd.concat([data_df['data_df'], pd.DataFrame()])
    # if data_df['data_df'].empty:
    #     data_empty_models.append(name)
    #     total_data_empty_models += data_empty_models
    #     total_data_empty_turbines += [algorithms_configs[name]['param_assetIds'][-1]+'/'+algorithms_configs[name]['param_turbine_num'][-1]]
    #     #更新algorithm_model表,部分字段
    #     mainLog.info(f'model表，模型{name}更新algorithm_model，时间，进度字段{turbineIndex}')
    #     algorithmLogs[name].info(f'model表，模型{name}更新algorithm_model，时间，进度字段{turbineIndex}')
    #     endAlgorithmTime = datetime.now()
    #     # UpdateTubineNum(name, startAlgorithmTime, endAlgorithmTime, df_wind_turbine.shape[0], turbineIndex)#mysqlClient, 
    #     #更新AlgorithmDetail
    #     mainLog.info(f"detail表，风机名>{turbineName}, 算法名>{name}, 数据为空")
    #     algorithmLogs[name].info(f"detail表，风机名>{turbineName}, 算法名>{name}, 数据为空")
    #     # InsertAlgorithmDetail(idMaps[name], turbineName, startTurbineTime, 2, assetId, "从中台没有提取到数据")#mysqlClient, 
    #     #撤销重命名
    #     if len(algorithm.ai_rename) != 0:
    #         for key, evalue in algorithm.ai_rename.items():
    #             if evalue in algorithm.ai_points:
    #                 index_key = algorithm.ai_points.index(evalue)
    #                 algorithm.ai_points[index_key] = key
    #     return None# continue
    if algorithms_configs[name]['PrepareTurbines'] == False:
        #更新algorithm_model表,部分字段
        mainLog.info(f'模型{name}需要全场风机数据来分析，当前风机数据融合进度字段{turbineIndex}')
        algorithmLogs[name].info(f'模型{name}需要全场风机数据来分析，当前风机数据融合进度字段{turbineIndex}')
        
        return None# continue
    # if (algorithms_configs[name]['PrepareTurbines'] == True and days==1 and name == 'Efficiency_ana_V3') or (algorithms_configs[name]['PrepareTurbines'] == True and not os.path.exists('Efficiency_ana_V3.pkl.gz') and name == 'Efficiency_ana_V3'):
        # data_df.to_pickle('Efficiency_ana_V3.pkl.gz', compression='gzip')
    try:
        #检查某算法测点数据范围是否二次调整
        remove_points = []
        if 'changeMeasurePointQueue' in algorithms_configs[name]:
            data_df['data_df'] = algorithm.cleanData(data_df['data_df'], algorithms_configs[name])
            remove_points = algorithms_configs[name]['changeMeasurePointQueue']
        #测点检测是否获取的测点和需要的测点数量一致
        if len(algorithm.private_points) > 0:
            private_points = []
            for modelKey, pointValue in algorithm.private_points.items():
                private_points += pointValue
        else:
            private_points = []
        #参数确定
        get_data_async.define_parameters(algorithms_configs, name)
        #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        #调试专用语句，其他情况注释该语句为了调试记录和加载数据
        if not os.path.exists('Efficiency_ana_V3.pkl.gz'):
            # algorithms_configs.to_pickle('algorithms_configs.pkl.gz', compression='gzip')
            with open('Efficiency_ana_V3.pkl.gz', 'wb') as f:
                f.write(gzip.compress(pickle.dumps(algorithms_configs)))
                # pickle.dump(algorithms_configs, f)
        else:
            with open('Efficiency_ana_V3.pkl.gz', 'rb') as f:
                algorithms_configs = pickle.loads(gzip.decompress(f.read()))
            # algorithms_configs = pd.read_pickle('algorithms_configs.pkl.gz')
        #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        # 跑模型
        if name == "Efficiency_ana_V3":
            timeOut = 7200
            judge_model_task = [algorithm.judge_model(algorithms_configs)]
            done, pending = await asyncio.wait(judge_model_task, timeout=timeOut) 
        else:
            timeOut = 7200
            judge_model_task = [algorithm.judge_model(algorithms_configs)]
            done, pending = await asyncio.wait(judge_model_task, timeout=timeOut)  
        if len(done) > 0:
            judge_model_result = [task.result() for task in done]
            # abnormal_data, statement, display, alert, warning = judge_model_result[0]
            tmp = [task.cancel() for task in pending]
        else:
            tmp = [task.cancel() for task in pending]
            assert False, ValueError("模型执行时长超过10分钟")

        endAlgorithmTime = datetime.now()
        #更新algorithm_model表,部分字段
        mainLog.info(f'model表，模型{name}更新algorithm_model，时间，进度字段字段{turbineIndex}')
        algorithmLogs[name].info(f'model表，模型{name}更新algorithm_model，时间，进度字段字段{turbineIndex}')
        # UpdateTubineNum(name, startAlgorithmTime, endAlgorithmTime, df_wind_turbine.shape[0], turbineIndex)#mysqlClient, 
    except Exception as e:
        exception_models.append(name)
        total_exception_turbines += [algorithms_configs[name]['param_assetIds'][-1]+'/'+algorithms_configs[name]['param_turbine_num'][-1]]
        # algorithmLogs[name].info(f'\033[31merror file: {e.__traceback__.tb_frame.f_globals["__file__"]}\033[0m')
        # algorithmLogs[name].info(f'\033[31merror line:{e.__traceback__.tb_lineno}\033[0m')
        errorInfomation = traceback.format_exc()
        mainLog.info(f'\033[31m{errorInfomation}\033[0m')
        mainLog.info(f'\033[33m风机{assetId}在执行模型{name}时发生异常：{e}\033[0m')
        algorithmLogs[name].info(f'\033[31m{errorInfomation}\033[0m')
        algorithmLogs[name].info(f'\033[33m风机{assetId}在执行模型{name}时发生异常：{e}\033[0m')
        endAlgorithmTime = datetime.now()
        #更新algorithm_model表,部分字段
        mainLog.info(f'model表，模型{name}更新algorithm_model，时间，进度字段字段{turbineIndex}')
        algorithmLogs[name].info(f'model表，模型{name}更新algorithm_model，时间，进度字段字段{turbineIndex}')
        # UpdateTubineNum(name, startAlgorithmTime, endAlgorithmTime, df_wind_turbine.shape[0], turbineIndex)#mysqlClient, 
        #更新AlgorithmDetail
        mainLog.info(f"detail表，风机名>{turbineName}, 算法名>{name}, 运行报错")
        algorithmLogs[name].info(f"detail表，风机名>{turbineName}, 算法名>{name}, 运行报错")
        str_e_index = f'{errorInfomation}'.rfind("Traceback")*-1
        str_e = f'{errorInfomation}'[str_e_index:]
        # if "wash_data" in errorInfomation and "wash_data_for_train" in errorInfomation:
        #     InsertAlgorithmDetail(idMaps[name], turbineName, startTurbineTime, 2, assetId, f'可能风机停机时造成数据异常，清洗后不满足算法计算条件。')#mysqlClient, 
        # else:
        #     InsertAlgorithmDetail(idMaps[name], turbineName, startTurbineTime, 2, assetId, f'{e}；详细情况：\n {str_e}')#mysqlClient, 
    # #撤销重命名
    # if len(algorithm.ai_rename) != 0:
    #     for key, evalue in algorithm.ai_rename.items():
    #         if evalue in data_df['data_df'].columns.tolist():
    #             if evalue in algorithm.ai_points:
    #                 index_key = algorithm.ai_points.index(evalue)
    #                 algorithm.ai_points[index_key] = key
    # endTurbineTime = datetime.now()
    total_data_empty_models += data_empty_models
    total_alarm_models += alarm_models
    total_no_alarm_models += no_alarm_models
    total_exception_models += exception_models

    #检查该风机是否需要二次变动测点的数据范围
    if 'changeMeasurePointQueue' in algConfig[name]:
        #删除空列表
        if len(algorithms_configs[name]['changeMeasurePointQueue'][turbineName]) == 0:
            del algorithms_configs[name]['changeMeasurePointQueue'][turbineName] 

    return None


def schedule():
    # while(True):
    #     try:
    init_loggers()
    
    scheduler = AsyncIOScheduler()

    # 具有相同时间范围、相同测点的算法合并一起，可以不用重复取数，数据缓存，执行完后销毁，提高运行效率
    
    #“hour”,"minute","day","halfday", "static"
    #超短期
    # scheduler.add_job(execute_multi_algorithms, 'interval', minutes=30, args=(scheduleConfig['minunte-1'], "minute")) # 每隔半个月执行一次
    # scheduler.add_job(execute_multi_algorithms, 'interval', hours=1, args=(scheduleConfig['minunte-2'], "minute")) # 每隔半个月执行一次
    # scheduler.add_job(execute_multi_algorithms, 'interval', hours=4, args=(scheduleConfig['hour'], "hour")) # 每隔四小时执行一次
    #短期
    # scheduler.add_job(execute_multi_algorithms, 'cron', hour=1, args=(['blade_angle_not_balance','wind_speed_fault','capacity_reduction','chilunxiang_disu_zhoucheng_temperature','chilunxiang_gaosu_zhoucheng_temperature','chilunxiang_sanre','engine_cabinet_temperature','engine_env_temperature','generator_houzhoucheng_temperature','generator_qianzhoucheng_temperature','generator_raozu_not_balance','generator_temperature','generator_zhuzhou_rpm_not_balance','oar_electric_capacity_temperature','oar_engine_performance','oar_engine_temperature','oar_machine_temperature', 'yepian_kailie', 'jiegou_sunshang'], "hour")) # 每天1点执行
    # scheduler.add_job(execute_multi_algorithms, 'interval', hours=6, args=(scheduleConfig['clock-1'], "hour")) # 每天1点执行
    # scheduler.add_job(execute_multi_algorithms, 'cron', hour=3, args=(scheduleConfig['clock-2'], "hour")) # 每天1点执行

    # 中期
    scheduler.add_job(execute_multi_algorithms, 'interval', days=5, args=(scheduleConfig['day-1'], "day")) # 每隔半个月执行一次
    scheduler.add_job(execute_multi_algorithms, 'interval', days=1, args=(scheduleConfig['day-2'], "day")) # 每隔半个月执行一次
    # 长期
    
    # scheduler.add_job(execute_multi_algorithms, 'interval', seconds=30, args=(['blade_freeze'],))
    # 调试
    # scheduler.add_job(execute_multi_algorithms, 'cron', hour=10, minute=49, args=(['blade_angle_not_balance','wind_speed_fault','capacity_reduction','chilunxiang_disu_zhoucheng_temperature','chilunxiang_gaosu_zhoucheng_temperature','chilunxiang_sanre','engine_cabinet_temperature','engine_env_temperature','generator_houzhoucheng_temperature','generator_qianzhoucheng_temperature','generator_raozu_not_balance','generator_temperature','generator_zhuzhou_rpm_not_balance','oar_electric_capacity_temperature','oar_engine_performance','oar_engine_temperature','oar_machine_temperature', 'yepian_kailie', 'jiegou_sunshang', 'tatong_qingjiao', 'hongwaicewen', 'luoshuansongdong', 'weathercock_freeze', 'blade_freeze','pianhang_duifeng_buzheng','generator_zhuanju_kongzhi'], "halfday"))
    
    scheduler.start()

    loop = asyncio.get_event_loop()
    loop.run_forever()
        # except Exception as e:
        #     errorInfomation = traceback.format_exc()
        #     logging.getLogger().error(f'\033[31m{errorInfomation}\033[0m')
        #     logging.getLogger().error(f'\033[33m任务调度报错，错误信息：{e}\033[0m')
        #     scheduler.shutdown()
        #     loop.close()


# 算法->风机维度
if __name__ == '__main__':
    init_loggers()
    policy = asyncio.get_event_loop_policy()
    policy.get_event_loop().set_debug(True)
    asyncio.run(execute_multi_algorithms(['capacity_reduction']))#'tatong_qingjiao', 'hongwaicewen'
    # asyncio.run(execute_multi_algorithms(['blade_angle_not_balance', 'blade_freeze', 'capacity_reduction', 'chilunxiang_disu_zhoucheng_temperature', 'chilunxiang_gaosu_zhoucheng_temperature', 'chilunxiang_sanre', 'engine_cabinet_temperature', 'engine_env_temperature', 'generator_houzhoucheng_temperature', 'generator_qianzhoucheng_temperature', 'generator_raozu_not_balance', 'generator_temperature', 'generator_zhuanju_kongzhi', 'generator_zhuzhou_rpm_not_balance', 'oar_electric_capacity_temperature', 'oar_engine_performance', 'oar_engine_temperature', 'pianhang_duifeng_buzheng', 'weathercock_freeze', 'wind_speed_fault']))
 
    
