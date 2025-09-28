# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 11:00:00 2023

@author: sunyan
"""

from poseidon import poseidon  # 打包时注意
import pandas as pd
from functools import partial  # 偏函数，用于map传递多个参数
from multiprocessing import Pool  # 计算密集型采用该并行
import multiprocessing as mp
import numpy as np
from sklearn.neighbors import LocalOutlierFactor
from itertools import product
import utils.time_util as time_util
from utils.time_util import timestamp_to_localtime
from datetime import timedelta
import logging
from configs.config import AccessKey, SecretKey, GW_Url, OrgId, algConfig, Data_Url, token, MainIP, MainPort, Platform, farmOrIds, serviceKey, Token_Url, OosUrl
import asyncio
from concurrent.futures import ThreadPoolExecutor
import time
import traceback
from datetime import datetime, timedelta
import importlib
import os
from logging.handlers import RotatingFileHandler
from data.efficiency_function import mymode
import configs.config as config
# from flask import request, jsonify, json, make_response
import requests
import json
import dask.dataframe as dd
from data.efficiency_function import turbineTypeNameFormat
import io
from requests_toolbelt.multipart.encoder import MultipartEncoder

#能效分析中需要在算法中用到的输入变量量
# Pwrat_Rate = None
# rotor_radius = None
# hub_high = None
# ManufacturerID = None
# state = None
# fault_code = None

# Df_all_all_alltype = pd.DataFrame()
# Df_all_m_all_alltype = pd.DataFrame()
# zuobiao_all = pd.DataFrame()
# Df_all_all = pd.DataFrame()
# Df_all_m_all = pd.DataFrame()
# yaw_time = pd.DataFrame()
# turbine_err_all = pd.DataFrame()
# turbine_err_all['wtid'] = None
# turbine_err_all['power_rate_err'] = 0  #额定功率异常
# turbine_err_all['wspd_power_err'] = 0  #风速功率散点异常
# turbine_err_all['torque_kopt_err'] = 0 #最佳Cp段转矩控制异常
# turbine_err_all['torque_rate_err'] = 0 #额定转速段转矩控制异常
# turbine_err_all['yaw_duifeng_err'] = 0 #对风偏航角度过大
# turbine_err_all['yaw_leiji_err'] = 0 #偏航控制误差过大
# turbine_err_all['pitch_min_err'] = 0 #最小桨距角异常
# turbine_err_all['pitch_control_err'] = 0 #变桨控制异常
# turbine_err_all['pitch_balance_err'] = 0 #三叶片变桨不平衡

# turbine_err_all['power_rate_loss'] = 0
# turbine_err_all['wspd_power_loss'] = 0
# turbine_err_all['torque_kopt_loss'] = 0
# turbine_err_all['torque_rate_loss'] = 0
# turbine_err_all['yaw_duifeng_loss'] = 0
# turbine_err_all['yaw_leiji_loss'] = 0
# turbine_err_all['pitch_min_loss'] = 0
# turbine_err_all['pitch_control_loss'] = 0
# turbine_err_all['pitch_balance_loss'] = 0
# turbine_err_all['pwrat_order'] = 1

# turbine_param_all = pd.DataFrame()

# zuobiao = pd.DataFrame()
# pw_df_alltime = pd.DataFrame()
# pw_turbine_all = pd.DataFrame()
# windbinreg = np.arange(1.75,25.25,0.5)
# windbin = np.arange(2.0,25.0,0.5)
# pw_df_alltime['windbin'] = windbin
# wind_ti_all = pd.DataFrame()
# wind_ti_all['windbin'] = windbin

######################################################

data_logger = logging.getLogger('get_data')
if not data_logger.handlers:
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(process)d - %(threadName)s - %(message)s')
    # console_handler = logging.StreamHandler()
    # console_handler.setFormatter(formatter)
    # alarm_file_handler = TimedRotatingFileHandler('logs/alarm.log', when='midnight', interval=1, backupCount=30)
    data_file_handler = logging.handlers.RotatingFileHandler(filename=os.path.join("logs","data"+".log"), mode='a', maxBytes=5*1024**2, backupCount=3)
    data_file_handler.setFormatter(formatter)
    data_logger.setLevel(logging.INFO)
    # data_logger.addHandler(console_handler)
    data_logger.addHandler(data_file_handler)

time_logger = logging.getLogger('get_time')
if not time_logger.handlers:
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(process)d - %(threadName)s - %(message)s')
    # console_handler = logging.StreamHandler()
    # console_handler.setFormatter(formatter)
    # alarm_file_handler = TimedRotatingFileHandler('logs/alarm.log', when='midnight', interval=1, backupCount=30)
    time_file_handler = logging.handlers.RotatingFileHandler(filename=os.path.join("logs","time"+".log"), mode='a', maxBytes=5*1024**2, backupCount=3)
    time_file_handler.setFormatter(formatter)
    time_logger.setLevel(logging.INFO)
    # data_logger.addHandler(console_handler)
    time_logger.addHandler(time_file_handler)

#################################################################
#中台
Url_asset = GW_Url + '/cds-asset-service/v1.0/hierarchy?orgId=' + OrgId  # 这个api哪里来的？
Url_ai_normalized = GW_Url + '/tsdb-service/v2.1/ai-normalized?orgId=' + OrgId
Url_ai = GW_Url + '/tsdb-service/v2.1/ai?orgId=' + OrgId
Url_raw = GW_Url + '/tsdb-service/v2.1/raw?orgId=' + OrgId
Url_di = GW_Url + '/tsdb-service/v2.1/di?orgId=' + OrgId
Url_node = GW_Url + '/asset-tree-service/v2.1/asset-nodes?action=searchRelatedAsset&orgId=' + OrgId
#######################################################################
#智慧场站
# Url_prefix = str(MainIP)+':'+str(MainPort)
Url_farm = Data_Url + '/wind/farm/list' #Data_Url "http://173.17.4.11:9100"
Url_turbine = Data_Url + '/wind/turbine/listByOrgId/%s'
Url_point = Data_Url + '/iot/device/search/point/history'
head = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer '+ token
}
#########################################################################
#内蒙请求服务获取token
Url_token = Token_Url + "/auth/login/externalApp"
headToken = {'Content-Type': 'application/json'}
def getToken():
    data_logger.info(f"#######################Url_token：{Url_token}###############")
    data_logger.info(f"#######################headToken:{headToken}###############")
    data_logger.info(f"#######################serviceKey:{serviceKey}###############")
    response = requests.post(Url_token, headers=headToken, data=json.dumps(serviceKey))
    if response != None:
        response = json.loads(response.text)
    else:
        data_logger.info(f"#######################access_token: None ###############")
        return None
    if response != None and len(response["data"]) > 0:
        data_logger.info(f"#######################access_token: {response['data']['access_token']}###############")
        return response["data"]["access_token"]
    else:
        data_logger.info(f"#######################access_token: None ###############")
        return None

########################################################################
#内蒙古请求java上传图片
def javaUploadImage(execute_time:str, token:str):
    headImage = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer '+ token
    }
    Url_image = OosUrl + "analysis/result/file/image/upload"
    data = {"executeTime": execute_time}
    data_logger.info(f"#######################Url_image: {Url_image}###############")
    data_logger.info(f"#######################headImage: {headImage}###############")
    data_logger.info(f"#######################params: {data}###############")
    requests.post(Url_image, headers=headImage, data=json.dumps(data))

########################################################################
#内蒙古请求java上传word
def javaUploadWord(execute_time:str, token:str):
    headWord = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer '+ token
    }
    Url_word = OosUrl + "analysis/result/file/word/upload"
    data = {"executeTime": execute_time}
    data_logger.info(f"#######################Url_image: {Url_word}###############")
    data_logger.info(f"#######################headWord: {headWord}###############")
    data_logger.info(f"#######################params: {data}###############")
    requests.post(Url_word, headers=headWord, data=json.dumps(data))

########################################################################
def get_content_type(filename):
    extension = filename.lower().split('.')[-1]
    content_types = {
        'jpg': 'image/jpeg',
        'jpeg': 'image/jpeg',
        'png': 'image/png',
        'gif': 'image/gif',
        'bmp': 'image/bmp',
        'webp': 'image/webp',
        'svg': 'image/svg+xml',
        'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'doc': 'application/msword',
        'pdf': 'application/pdf'
    }
    return content_types.get(extension, 'application/octet-stream')

#内蒙古请求java上传oos
def javaUploadOOS(filename:str, token:str):
    contentType = get_content_type(filename)
    # headOOS = {
    #     # 'Content-Type': contentType,
    #     'Authorization': 'Bearer '+ token
    # }
    # files = {"file": open(filename, 'rb')}
    #########################################3
    headerOOS = {
        # 'Content-Type': contentType,
        'Authorization': 'Bearer '+ token
    }
    files = {"file": (os.path.basename(filename), open(filename, 'rb'), contentType)}

    #########################################3
    # 精确控制每个部分的 Content-Type
    # multipart_data = MultipartEncoder(
    #     fields={
    #         'image': (os.path.basename(filename), open(filename, 'rb'), contentType),
    #         'contentType': contentType  # 单独字段
    #     }
    # )

    # headerOOS = {'Content-Type': multipart_data.content_type}
    Url_OOS = OosUrl + "analysis/result/file/upload"
    # file = {"file": files}
    data_logger.info(f"#######################Url_OOS: {Url_OOS}###############")
    data_logger.info(f"#######################headOOS: {headerOOS}###############")
    data_logger.info(f"#######################file: {files}###############")
    response = requests.post(Url_OOS, headers=headerOOS, files=files)
    if response != None:
        response = json.loads(response.text)
    if response != None and len(response["data"]) > 0:
        data_logger.info(f"#######################uuid: {response['data']['id']}###############")
        return response["data"]["id"]
    else:
        data_logger.info(f"#######################uuid: None ###############")
        return None


#######################################################################
#智慧场站
#发送数据请求请求
#超时
requestTime = 5
#风场信息
async def getWindFarmIntel():
    funStartTime= time.time()
    global Url_farm
    WindFarm_attr = pd.DataFrame()
    data_logger.info(f"#############################获取风场信息############################")
    data_logger.info(f"数据接口url：{Url_farm}")
    data_logger.info(f"数据接口head：{head}")
    data_logger.info(f"数据接口参数：无")
    try:
        ResponsePoint = await asyncio.to_thread(requests.post, Url_farm, headers=head, timeout=requestTime)
        data_logger.info(f"接口响应：{ResponsePoint}")
        ResponsePoint = json.loads(ResponsePoint.text)
        #print(ResponsePoint)
        if ResponsePoint and len(ResponsePoint['data']) > 0:
            for i, wind_farm in enumerate(ResponsePoint['data']):
                # WindFarm_attr = pd.DataFrame(wind_farm['mdmObjects']['EnOS_Wind_Farm'][:])
                if wind_farm["orgId"]:
                    if farmOrIds:
                        if wind_farm["orgId"] in farmOrIds:
                            orgId = wind_farm["orgId"]
                        else:
                            continue
                    else:
                        orgId = wind_farm["orgId"]
                else:
                    continue
                if wind_farm["orgName"]:
                    orgName = wind_farm["orgName"]#list(map(lambda x: x['name'], WindFarm_attr['attributes']))
                else:
                    continue
                # WindFarm_attr.loc[i, 'mdmId'] = list(map(lambda x: x['mdmId'], WindFarm_attr['attributes']))
                # WindFarm_attr['二级公司'] = list(map(lambda x: x['companyID'], WindFarm_attr['attributes']))
                if wind_farm['capacity']:
                    capacity = wind_farm['capacity']#list(map(lambda x: x['operativeCapacity'], WindFarm_attr['attributes']))
                else:
                    continue
                if wind_farm['machineType'] and "," in wind_farm['machineType']:
                    machineType = wind_farm['machineType'].split(",")
                elif wind_farm['machineType'] and "," not in wind_farm['machineType']:
                    machineType = [wind_farm['machineType']]
                else:
                    continue
                # WindFarm_attr['风资源'] = list(map(lambda x: x['resourceType'], WindFarm_attr['attributes']))
                if wind_farm["turbineNum"]:
                    turbineNum = wind_farm["turbineNum"]#list(map(lambda x: x['equipmentAmount'], WindFarm_attr['attributes']))
                else:
                    continue
                if wind_farm["createTime"]:
                    createTime = wind_farm["createTime"]#list(map(lambda x: x['operativeDate'], WindFarm_attr['attributes']))
                else:
                    continue
                if wind_farm["address"]:
                    address = wind_farm["address"]#list(map(lambda x: x['address'], WindFarm_attr['attributes']))
                else:
                    continue
                # if 'address' in WindFarm_attr['attributes'].keys():
                #     WindFarm_attr['地址'] = list(map(lambda x: x['address'], WindFarm_attr['attributes']))
                # elif 'county' in WindFarm_attr['attributes'].keys():
                #     WindFarm_attr['地址'] = list(map(lambda x: x['county'], WindFarm_attr['attributes']))
                WindFarm_attr.loc[i, 'orgId'] = orgId
                WindFarm_attr.loc[i,'风电场名'] = orgName
                WindFarm_attr.loc[i, '容量'] = capacity
                WindFarm_attr.loc[i,'机型'] = machineType
                WindFarm_attr.loc[i,'风机台数'] = turbineNum
                WindFarm_attr.loc[i,'并网日期'] = createTime
                WindFarm_attr.loc[i, '地址'] = address
    except Exception as e:
        # 处理异常
        errorInfomation = traceback.format_exc()
        data_logger.info(f'###############################风场请求抛错######################')
        data_logger.info(f'\033[31m{errorInfomation}\033[0m')
        data_logger.info(f'\033[33m发生异常：{e}\033[0m')
        WindFarm_attr = pd.DataFrame()
    time_logger.info(f'###########################执行一次 getWindFarmIntel 时间{time.time()-funStartTime}秒###################')
    return WindFarm_attr
#风机信息
def selectCoreDeviceAttributes(coreDeviceAttributesList, attribute):
    funStartTime= time.time()
    attributeList = []
    for turbine in coreDeviceAttributesList:
        for item in turbine:
            if item['key'] == attribute:
                if attribute == 'WeiDu' or attribute == 'JingDu':
                    value = float(item['value'].split("°")[0])
                    value += float(item['value'].split("°")[-1].split("'")[0])
                    value += float(item['value'].split("°")[-1].split("'")[-1].split("\"")[0])
                elif attribute == "HaiBa" or attribute == "QieRuFengSu" or attribute == "QieChuFengSu" or attribute == "LunGuGaoDu" or attribute == "YeLunZhiJing" or attribute == "EDingRongLiang":
                    value = float(item['value'])
                else:
                    value = item['value']
                attributeList.append(value)
    time_logger.info(f'###########################执行一次 selectCoreDeviceAttributes 时间{time.time()-funStartTime}秒###################')
    return attributeList

async def getWindTurbinesIntel(wind_farm):
    '''
    获取风场下的风机
    '''
    
    global Url_turbine
    if "%s" in Url_turbine:
        Url_turbine = Url_turbine%wind_farm
    data_logger.info(f"#############################获取风机信息############################")
    data_logger.info(f"数据接口url：{Url_turbine}")
    data_logger.info(f"数据接口head：{head}")
    data_logger.info(f"数据接口参数：无")
    try:
        ResponsePoint = await asyncio.to_thread(requests.post, Url_turbine, headers=head, timeout=requestTime)
        data_logger.info(f"接口响应：{ResponsePoint}")
        ResponsePoint = json.loads(ResponsePoint.text)
        if ResponsePoint and len(ResponsePoint['data']) > 0:
            # wind_turbine_df = pd.DataFrame(
                # ResponsePoint['data'][wind_farm]['mdmObjects']['EnOS_Wind_Turbine'][:])
            wind_turbine_df = pd.DataFrame()
            wind_turbine_df['name'] = list(
                map(lambda x: x['deviceName'],  ResponsePoint['data']))
            wind_turbine_df['assetId'] = list(
                map(lambda x: x['deviceCode'],  ResponsePoint['data']))
            coreDeviceAttributesList =  list(
                map(lambda x: x['coreDeviceAttributes'],  ResponsePoint['data']))
            wind_turbine_df['ratedPower'] = selectCoreDeviceAttributes(coreDeviceAttributesList, "EDingRongLiang")
            wind_turbine_df['rotorDiameter'] = selectCoreDeviceAttributes(coreDeviceAttributesList, "YeLunZhiJing")
            wind_turbine_df['altitude'] = selectCoreDeviceAttributes(coreDeviceAttributesList, "HaiBa")
            wind_turbine_df['hubHeight'] = selectCoreDeviceAttributes(coreDeviceAttributesList, "LunGuGaoDu")
            wind_turbine_df['turbineTypeID'] = selectCoreDeviceAttributes(coreDeviceAttributesList, "JiXing")
            wind_turbine_df['cutInwindSpeed'] = selectCoreDeviceAttributes(coreDeviceAttributesList, "QieRuFengSu")
            wind_turbine_df['cutOutwindSpeed'] = selectCoreDeviceAttributes(coreDeviceAttributesList, "QieChuFengSu")
            wind_turbine_df['longitude'] = selectCoreDeviceAttributes(coreDeviceAttributesList, "JingDu")
            wind_turbine_df['latitude'] = selectCoreDeviceAttributes(coreDeviceAttributesList, "WeiDu")
            # wind_turbine_df.drop('attributes', axis=1, inplace=True)
            return wind_turbine_df 
        else:
            return pd.DataFrame()
    except Exception as e:
        # 处理异常
        errorInfomation = traceback.format_exc()
        data_logger.info(f'###############################风机信息请求抛错######################')
        data_logger.info(f'\033[31m{errorInfomation}\033[0m')
        data_logger.info(f'\033[33m发生异常：{e}\033[0m')
        return pd.DataFrame()
#测点信息

#格式转化
def FormatConvert(oldFormat:list):
    funStartTime = time.time()
    try:
        newFormat = pd.DataFrame()
        for obj1 in oldFormat:
            pointName = [point for point in obj1.keys()][0]
            if len(obj1[pointName]) == 0:
                newFormat[pointName] = pd.Series(dtype='float32')
            elif isinstance(obj1[pointName], str) and "不存在" in obj1[pointName]:
                continue
            elif isinstance(obj1[pointName], str) and "[]" in obj1[pointName]:
                continue
            for obj2 in obj1[pointName]:
                newFormat.loc[obj2["localtime"], pointName] = obj2["value"]
                newFormat.loc[obj2["localtime"], "timestamp"] = obj2["localtime"] #timestamp_to_localtime(obj2["localtime"])
        if "timestamp" not in newFormat.columns.to_list():
            newFormat = pd.DataFrame()
    except Exception as e:
        errorInfomation = traceback.format_exc()
        data_logger.info(f'\033[31m{errorInfomation}\033[0m')
        data_logger.info(f'\033[33m发生异常：{e}\033[0m')
        data_logger.info(f'###############################formatConvert######################')
        data_logger.info(f'{pointName}:{obj2}, localtime type:{type(obj2["localtime"])}')
    time_logger.info(f'###########################执行一次 FormatConvert 时间{time.time()-funStartTime}秒###################')
    return newFormat


async def getGeneralDataIntel(algorithmName: str, startTime, endTime, assetId: str, points, resample_interval, algorithms_configs):
    funStartTime = time.time()
    data_logger.info(f"#############################获取通用故障测点信息############################")
    global Url_point
    turbinenames = dict(zip(algorithms_configs[algorithmName]['param_assetIds'],algorithms_configs[algorithmName]['param_turbine_num']))
    ResponsePoint = None
    pointIds = ','.join(points)
    params = {
        "assetIds": assetId,
        "pointIds": pointIds,
        "startTime": startTime,
        "endTime": endTime,
        "interval": "60",
        "type": 1
    }
    params = json.dumps(params)
    data_logger.info(f"#####################当前算法：{algorithmName}=>风机ID/机号:{algorithms_configs[algorithmName]['param_assetIds']}/{algorithms_configs[algorithmName]['param_turbine_num']}=>设备ID:{assetId}=>数据时间范围{startTime}到{endTime}#############################")
    data_logger.info(f"数据接口url：{Url_point}")
    data_logger.info(f"数据接口head：{head}")
    data_logger.info(f"数据接口参数：{params}")
    try:
        ResponsePoint = await asyncio.to_thread(requests.post, Url_point, headers=head, data=params, timeout=requestTime)
        funStartTime1 = time.time()
        time_logger.info(f'###########################执行一次 getGeneralDataIntel requests.post 时间{time.time()-funStartTime1}秒, 设定的超时时间{requestTime}秒###################')
        # ResponsePoint = await asyncio.to_thread(requests.post, Url_point, headers=head, data=params)
        # data_logger.info(f"接口响应：{ResponsePoint.text}")
        ResponsePoint = json.loads(ResponsePoint.text)
        DfTemp = pd.DataFrame()
        if ResponsePoint and ResponsePoint['data'] and ResponsePoint['data'][0]['data'] and len(ResponsePoint['data'][0]['data']) > 0:
            temp = ResponsePoint['data'][0]['data']
            temp = json.loads(temp) #字符串转json
            DfTemp = FormatConvert(temp)
            if DfTemp.empty == False:
                DfTemp['localtime'] = pd.to_datetime(DfTemp['timestamp'],unit = "ms",errors='coerce',origin = "1970-01-01 08:00:00")
    
                DfTemp.set_index('localtime', inplace=True)
                #重命名
                #算法名
                DfTemp['algorithm'] = algorithmName
                DfTemp['assetId'] = assetId
                DfTemp['wtid'] = turbinenames[assetId]
                data_logger.info(f"提取的测点有：{list(DfTemp.columns)}")
                data_logger.info(f"前10行数据：{DfTemp.iloc[:10]}")
            else:
                data_logger.info(f"没有提取到测点")
        else:
            data_logger.info(f"没有提取到测点")
    except Exception as e:
        # 处理异常
        errorInfomation = traceback.format_exc()
        data_logger.info(f'###############################测点信息请求抛错######################')
        data_logger.info(f'\033[31m{errorInfomation}\033[0m')
        data_logger.info(f'\033[33m发生异常：{e}\033[0m')
        DfTemp = pd.DataFrame()
    time_logger.info(f'###########################执行一次 getGeneralDataIntel 时间{time.time()-funStartTime}秒###################')
    return DfTemp

async def getDiDataIntel(algorithmName: str, startTime, endTime, assetId: str, points, resample_interval, algorithms_configs):
    funStartTime = time.time()
    data_logger.info(f"#############################获取状态测点信息############################")
    global Url_point
    turbinenames = dict(zip(algorithms_configs[algorithmName]['param_assetIds'],algorithms_configs[algorithmName]['param_turbine_num']))
    ResponsePoint = None
    pointIds = ','.join(points)
    params = {
        "assetIds": assetId,
        "pointIds": pointIds,
        "startTime": startTime,
        "endTime": endTime,
        "interval": "60",
        "type": 1
    }
    params = json.dumps(params)
    data_logger.info(f"#####################当前算法：{algorithmName}=>风机ID/机号:{algorithms_configs[algorithmName]['param_assetIds']}/{algorithms_configs[algorithmName]['param_turbine_num']}=>设备ID:{assetId}=>数据时间范围{startTime}到{endTime}#############################")
    data_logger.info(f"数据接口url：{Url_point}")
    data_logger.info(f"数据接口head：{head}")
    data_logger.info(f"数据接口参数：{params}")
    try:
        ResponsePoint = await asyncio.to_thread(requests.post, Url_point, headers=head, data=params, timeout=requestTime)
        funStartTime1 = time.time()
        time_logger.info(f'###########################执行一次 getDiDataIntel requests.post 时间{time.time()-funStartTime1}秒, 设定的超时时间{requestTime}秒###################')
        # data_logger.info(f"接口响应：{ResponsePoint.text}")
        ResponsePoint = json.loads(ResponsePoint.text)
        DfTemp = pd.DataFrame()
        if ResponsePoint and ResponsePoint['data'] and ResponsePoint['data'][0]['data'] and  len(ResponsePoint['data'][0]['data']) > 0:
            temp = ResponsePoint['data'][0]['data']
            temp = json.loads(temp)
            DfTemp = FormatConvert(temp)
            if DfTemp.empty == False:
                DfTemp['localtime'] = pd.to_datetime(DfTemp['timestamp'],unit = "ms",errors='coerce',origin = "1970-01-01 08:00:00")
                DfTemp.set_index('localtime', inplace=True)
                #算法名
                DfTemp['algorithm'] = algorithmName
                DfTemp['assetId'] = assetId
                DfTemp['wtid'] = turbinenames[assetId]
                data_logger.info(f"提取的测点有：{list(DfTemp.columns)}")
                data_logger.info(f"前10行数据：{DfTemp.iloc[:10]}")
            else:
                data_logger.info(f"没有提取到测点")
        else:
            data_logger.info(f"没有提取到测点")
    except Exception as e:
        # 处理异常
        errorInfomation = traceback.format_exc()
        data_logger.info(f'###############################测点信息请求抛错######################')
        data_logger.info(f'\033[31m{errorInfomation}\033[0m')
        data_logger.info(f'\033[33m发生异常：{e}\033[0m')
        DfTemp = pd.DataFrame()
    data_logger.info(f'###########################执行一次 getDiDataIntel 时间{time.time()-funStartTime}秒###################')
    return DfTemp


async def getAiDataIntel(algorithmName: str, startTime, endTime, assetId: str, points, resample_interval, algorithms_configs):
    funStartTime = time.time()
    data_logger.info(f"#############################获取数据测点信息############################")
    global Url_point
    turbinenames = dict(zip(algorithms_configs[algorithmName]['param_assetIds'],algorithms_configs[algorithmName]['param_turbine_num']))
    # print(startTime, endTime, assetId, points, resample_interval)
    ResponsePoint = None
    # time.sleep(5)
    pointsIds = ','.join(points)
    params = {
        "assetIds": assetId,
        "pointIds": pointsIds,
        "startTime": startTime,
        "endTime": endTime,
        "interval": "60",
        "type": 1
    }
    params = json.dumps(params)
    data_logger.info(f"#####################当前算法：{algorithmName}=>风机ID/机号:{algorithms_configs[algorithmName]['param_assetIds']}/{algorithms_configs[algorithmName]['param_turbine_num']}=>设备ID:{assetId}=>数据时间范围{startTime}到{endTime}#############################")
    data_logger.info(f"数据接口url：{Url_point}")
    data_logger.info(f"数据接口head：{head}")
    data_logger.info(f"数据接口参数：{params}")
    try:
        ResponsePoint = await asyncio.to_thread(requests.post, Url_point, headers=head, data=params, timeout=requestTime)
        funStartTime1 = time.time()
        time_logger.info(f'###########################执行一次 getAiDataIntel requests.post 时间{time.time()-funStartTime1}秒, 设定的超时时间{requestTime}秒###################')
        # ResponsePoint = requests.post(Url_point, headers=head, data=params)
        # data_logger.info(f"接口响应：{ResponsePoint.text}")
        ResponsePoint = json.loads(ResponsePoint.text)
        DfTemp = pd.DataFrame()
        if ResponsePoint and ResponsePoint['data'] and ResponsePoint['data'][0]['data'] and len(ResponsePoint['data'][0]['data']) > 0:
            temp = ResponsePoint['data'][0]['data']
            temp = json.loads(temp)
            DfTemp = FormatConvert(temp)
            if DfTemp.empty == False:
                DfTemp['localtime'] = pd.to_datetime(DfTemp['timestamp'],unit = "ms",errors='coerce',origin = "1970-01-01 08:00:00")
                DfTemp.set_index('localtime', inplace=True)
                #算法名
                DfTemp['algorithm'] = algorithmName
                DfTemp['assetId'] = assetId
                DfTemp['wtid'] = turbinenames[algorithms_configs[algorithmName]['param_assetIds'][-1]]
                data_logger.info(f"提取的测点有：{list(DfTemp.columns)}")
                data_logger.info(f"前10行数据：{DfTemp.iloc[:10]}")
            else:
                data_logger.info(f"没有提取到测点")
        else:
            data_logger.info(f"没有提取到测点")
    except Exception as e:
        # 处理异常
        errorInfomation = traceback.format_exc()
        data_logger.info(f'###############################测点信息请求抛错######################')
        data_logger.info(f'\033[31m{errorInfomation}\033[0m')
        data_logger.info(f'\033[33m发生异常：{e}\033[0m')
        DfTemp = pd.DataFrame()
    time_logger.info(f'###########################执行一次 getAiDataIntel 时间{time.time()-funStartTime}秒###################')
    return DfTemp
#智慧场站
############################

pd.options.mode.use_inf_as_na = True
lock = asyncio.Lock()

def urlRequest(url):
    response = requests.post(url, headers=head)
    return response



async def run_in_threadpool(func, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(ThreadPoolExecutor(), lambda: func(*args))

async def getWindFarm(wind_farms):
    WindFarm_attr = pd.DataFrame()
    for i, wind_farm in enumerate(wind_farms):
        url_asset = str(str(Url_asset) + '&mdmIds=' + str(wind_farm) + '&mdmTypes=EnOS_Wind_Farm&attributes=&locale=zh-CN')
        
        ResponsePoint = poseidon.urlopen(AccessKey, SecretKey, url_asset)
        #print(ResponsePoint)
        if ResponsePoint and ResponsePoint['pagination']['pageSize'] > 0:
            WindFarm = pd.DataFrame(ResponsePoint['data'][wind_farm]['mdmObjects']['EnOS_Wind_Farm'][:])
            if "fullName" in WindFarm['attributes'][0]:
                WindFarm_attr.loc[i,'风电场名'] = list(map(lambda x: x['fullName'], WindFarm['attributes']))[0]
            elif "name" in WindFarm['attributes'][0]:
                WindFarm_attr.loc[i,'风电场名'] = list(map(lambda x: x['name'], WindFarm['attributes']))[0]
            else:
                WindFarm_attr.loc[i,'风电场名'] = np.nan
            WindFarm_attr.loc[i,'mdmId'] = list(map(lambda x: x['mdmId'], WindFarm['attributes']))[0]
            if "companyID" in WindFarm['attributes'][0]:
                WindFarm_attr.loc[i,'二级公司'] = list(map(lambda x: x['companyID'], WindFarm['attributes']))[0]
            else:
                WindFarm_attr.loc[i,'二级公司'] = np.nan
            if "operativeCapacity" in WindFarm['attributes'][0]:
                WindFarm_attr.loc[i,'容量'] = list(map(lambda x: x['operativeCapacity'], WindFarm['attributes']))[0]
            elif "CleanEnergyCapacity" in WindFarm['attributes'][0]:
                WindFarm_attr.loc[i,'容量'] = list(map(lambda x: x['CleanEnergyCapacity'], WindFarm['attributes']))[0]
            else:
                WindFarm_attr.loc[i,'容量'] = np.nan
            if "wtgTypes" in WindFarm['attributes'][0]:
                WindFarm_attr.loc[i,'机型'] = list(map(lambda x: turbineTypeNameFormat(x['wtgTypes']), WindFarm['attributes']))[0]
            else:
                WindFarm_attr.loc[i,'机型'] = np.nan
            WindFarm_attr.loc[i,'风资源'] = list(map(lambda x: x['resourceType'], WindFarm['attributes']))[0]
            if "DevAmount" in WindFarm['attributes'][0]:
                WindFarm_attr.loc[i,'风机台数'] = list(map(lambda x: x['DevAmount'], WindFarm['attributes']))[0]
            elif "equipmentAmount" in WindFarm['attributes'][0]:
                WindFarm_attr.loc[i,'风机台数'] = list(map(lambda x: x['equipmentAmount'], WindFarm['attributes']))[0]
            else:
                WindFarm_attr.loc[i,'风机台数'] = np.nan
            if "operativeDate" in WindFarm['attributes'][0]:
                WindFarm_attr.loc[i,'并网日期'] = list(map(lambda x: x['operativeDate'], WindFarm['attributes']))[0]
            elif "allOngridDate" in WindFarm['attributes'][0]:
                WindFarm_attr.loc[i,'并网日期'] = list(map(lambda x: x['allOngridDate'], WindFarm['attributes']))[0]
            else:
                WindFarm_attr.loc[i,'并网日期'] = np.nan
            if 'address' in WindFarm['attributes'][0].keys():
                WindFarm_attr.loc[i,'地址'] = list(map(lambda x: x['address'], WindFarm['attributes']))[0]
            elif 'county' in WindFarm['attributes'][0].keys():
                WindFarm_attr.loc[i,'地址'] = list(map(lambda x: x['county'], WindFarm['attributes']))[0]
            else:
                WindFarm_attr.loc[i,'地址'] = np.nan
            if 'rccID' in WindFarm['attributes'][0].keys():
                WindFarm_attr.loc[i,'生产运营中心'] = list(map(lambda x: x['rccID'], WindFarm['attributes']))[0]
            else:
                WindFarm_attr.loc[i,'rccID'] = np.nan
        
    return WindFarm_attr

async def getWindTurbines(wind_farm):
    '''
    获取风场下的风机
    '''
    url_asset = Url_asset + '&mdmIds=' + wind_farm + \
        '&mdmTypes=EnOS_Wind_Turbine&attributes=mdmId,name,ratedPower,rotorDiameter,altitude,hubHeight,turbineTypeID,cutOutwindSpeed,longitude,latitude'
    # with ThreadPoolExecutor(max_workers=3) as executor:
    #获取每一个风机属性
    #python版本>3.8
    # ResponsePoint = await asyncio.to_thread(poseidon.urlopen, AccessKey, SecretKey, url_asset)#poseidon.urlopen(AccessKey, SecretKey, url_asset)#await asyncio.to_thread(poseidon.urlopen, AccessKey, SecretKey, url_asset)
    #python版本=3.8
    ResponsePoint = await asyncio.to_thread(poseidon.urlopen, AccessKey, SecretKey, url_asset)#await run_in_threadpool(poseidon.urlopen, AccessKey, SecretKey, url_asset)#poseidon.urlopen(AccessKey, SecretKey, url_asset)#await asyncio.to_thread(poseidon.urlopen, AccessKey, SecretKey, url_asset)
    if ResponsePoint and ResponsePoint['pagination']['pageSize'] > 0:
        wind_turbine_df = pd.DataFrame(
            ResponsePoint['data'][wind_farm]['mdmObjects']['EnOS_Wind_Turbine'][:])
        wind_turbine_df['name'] = list(
            map(lambda x: x['name'],  wind_turbine_df['attributes']))
        wind_turbine_df['ratedPower'] = list(
            map(lambda x: x['ratedPower'],  wind_turbine_df['attributes']))
        if 'rotorDiameter' in wind_turbine_df['attributes']:
            wind_turbine_df['rotorDiameter'] = list(
                map(lambda x: x['rotorDiameter'],  wind_turbine_df['attributes']))
        else:
            wind_turbine_df['rotorDiameter'] = [115] * len(wind_turbine_df['attributes'])
        wind_turbine_df['altitude'] = list(
            map(lambda x: x['altitude'],  wind_turbine_df['attributes']))
        wind_turbine_df['hubHeight'] = list(
            map(lambda x: x['hubHeight'],  wind_turbine_df['attributes']))
        wind_turbine_df['turbineTypeID'] = list(
            map(lambda x: turbineTypeNameFormat(x['turbineTypeID']),  wind_turbine_df['attributes']))
        wind_turbine_df['cutOutwindSpeed'] = list(
            map(lambda x: x['cutOutwindSpeed'],  wind_turbine_df['attributes']))
        wind_turbine_df['longitude'] = list(
            map(lambda x: x['longitude'],  wind_turbine_df['attributes']))
        wind_turbine_df['latitude'] = list(
            map(lambda x: x['latitude'],  wind_turbine_df['attributes']))
        wind_turbine_df.drop('attributes', axis=1, inplace=True)
        return wind_turbine_df 
    else:
        return pd.DataFrame()


async def getWindTurbinesNode(turbineIds, algorithms_configs, nameConstrain=None):
    '''
    获取风机下的模块信息
    '''
    url_node = Url_node + '&treeId=VsKgAT9I' #+ \
        #'&projection=attributes,assetId,name'
    # with ThreadPoolExecutor(max_workers=3) as executor:
    multiAssetIds = [] #[风机1[算法1[模型1(id),模型2(id),...], [算法2],...], [风机2],...]
    for turbineId in turbineIds: #[turbine1:(modelid1, modelid2,...), turbine2:(modelid1, modelid2,..), ...]
        params = {
            "filter": {
                "isChildOfAssetId": turbineId
            },
            "action": "searchRelatedAsset"
        }
        #获取每一个风机属性
        ResponsePoint = await asyncio.to_thread(poseidon.urlopen,AccessKey, SecretKey, url_node, params)
        # ResponsePoint = await run_in_threadpool(poseidon.urlopen,AccessKey, SecretKey, url_node, params)
        if ResponsePoint and len(ResponsePoint['data']) > 0:
            wind_turbine_df = pd.DataFrame(ResponsePoint['data'])
            wind_turbine_df['modelName'] = [value['defaultValue'] for value in wind_turbine_df['name']]
            multiAlgs = {}
            for alKey, alValue in algorithms_configs.items():
                modelAssetIds = []
                for modelKey, modelValue in alValue['privatePoints'].items():
                    modelId = modelKey
                    if nameConstrain == None:
                        wind_turbine_df_filted = wind_turbine_df[wind_turbine_df['modelId']==modelId]
                    else:
                        wind_turbine_df_filted = wind_turbine_df[wind_turbine_df['modelId']==modelId]
                        wind_turbine_df_filted = wind_turbine_df_filted[wind_turbine_df['modelName'].str.contains(nameConstrain)]
                    # turbineIds.loc[index, 'modelAssetId'] = wind_turbine_df_filted['assetId']
                    # turbineAssets.append(wind_turbine_df_filted.iloc[0]['assetId'])
                    try:
                        modelAssetIds.append(wind_turbine_df_filted.iloc[0]['assetId'])
                    except Exception as e:
                        errorInfomation = traceback.format_exc()
                        data_logger.info(f'\033[31m{errorInfomation}\033[0m')
                        data_logger.info(f'\033[33mgetWindTurbinesNode->{e}\033[0m')
                multiAlgs[alKey] = modelAssetIds
        else:
            multiAlgs = {}
            for alKey, alValue in algorithms_configs.items():
                modelAssetIds = []
            multiAlgs[alKey] = modelAssetIds

        multiAssetIds.append(multiAlgs)
        
    return multiAssetIds #[机型1:{算法1:[模型1(id),]}, ...]


async def getRawData(startTime, endTime, points, assetIds):
    DfTemp = pd.DataFrame()
    params = {"assetIds": ','.join(assetIds),
              "pointIds": points,
              "startTime": startTime,
              "endTime": endTime,
              "itemFormat": "1",
              "type": "ai_normalized",  # ai,ai_normalized,di,pi,generic
              "boundaryType": "sample",
              "interval": 600,
              "interpolation": "near",
              "pageSize": "20000"}

    ResponsePoint = await asyncio.to_thread(poseidon.urlopen, AccessKey, SecretKey, Url_raw, params)
    # ResponsePoint = await run_in_threadpool(poseidon.urlopen, AccessKey, SecretKey, Url_raw, params)
    if ResponsePoint and ResponsePoint['data'] and len(ResponsePoint['data']['items']) > 0:
        DfTemp = pd.DataFrame(ResponsePoint['data']['items'])
    return DfTemp

async def getGeneralData(algorithmName: str, startTime, endTime, assetId: str, points, resample_interval, algorithms_configs):
    turbinenames = dict(zip(algorithms_configs[algorithmName]['param_assetIds'],algorithms_configs[algorithmName]['param_turbine_num']))
    ResponsePoint = None
    params = {
                "Access Key":AccessKey,
                "Secret Key":SecretKey,
                "assetIds": assetId,
                "pointIds": ','.join(points),
                "startTime": startTime,
                "endTime": endTime,
                # "interval": "0",
                "itemFormat": "1",
                "pageSize": "20000",
                "type": "generic"}
    # ResponsePoint = poseidon.urlopen(AccessKey, SecretKey, Url_raw, params)
    ResponsePoint = await asyncio.to_thread(poseidon.urlopen,AccessKey, SecretKey, Url_raw, params)
    DfTemp = pd.DataFrame()
    if ResponsePoint and ResponsePoint['data'] and len(ResponsePoint['data']['items']) > 0:
        DfTemp = pd.DataFrame(ResponsePoint['data']['items'])
        DfTemp['localtime'] = pd.to_datetime(DfTemp['localtime'],errors='coerce')
        DfTemp.set_index('localtime', inplace=True)
        # DfTemp.index = pd.to_datetime(DfTemp.index)
        # #填充替换NaN
        # DfTemp = DfTemp.ffill()
        # DfTemp = DfTemp.bfill()
        # DfTemp = DfTemp.fillna(0)
        # value, unit = time_util.split_time_delta(resample_interval)
        # if pd.Timedelta(str(value)+unit) > pd.Timedelta('1min'):
        #     resample_interval = time_util.replace_to_resample(
        #         resample_interval)
        #     #实际取得的测点和自定义的可能不一致会报错
        #     common_columns = DfTemp.columns.intersection(points).tolist()
        #     DfTemp = DfTemp[common_columns].resample(
        #         resample_interval, closed='left').mean().ffill()#  # FIXME
        #     DfTemp = DfTemp[common_columns].astype(int)
        #重命名
        #算法名
        DfTemp['algorithm'] = algorithmName
        DfTemp['assetId'] = assetId
        DfTemp['wtid'] = turbinenames[assetId]
    return DfTemp

async def getDiData(algorithmName: str, startTime, endTime, assetId: str, points, resample_interval, algorithms_configs):
    
    turbinenames = dict(zip(algorithms_configs[algorithmName]['param_assetIds'],algorithms_configs[algorithmName]['param_turbine_num']))
    ResponsePoint = None
    params = {
                "Access Key":AccessKey,
                "Secret Key":SecretKey,
                "assetIds": assetId,
                "pointIds": ','.join(points),
                "startTime": startTime,
                "endTime": endTime,
                # "interval": "0",
                "itemFormat": "1",
                "pageSize": "20000",
                "type": "di"}
    # ResponsePoint = poseidon.urlopen(AccessKey, SecretKey, Url_di, params)
    ResponsePoint = await asyncio.to_thread(poseidon.urlopen,AccessKey, SecretKey, Url_di, params)
    DfTemp = pd.DataFrame()
    if ResponsePoint and ResponsePoint['data'] and  len(ResponsePoint['data']['items']) > 0:
        DfTemp = pd.DataFrame(ResponsePoint['data']['items'])
        DfTemp['localtime'] = pd.to_datetime(DfTemp['localtime'],errors='coerce')
        DfTemp.set_index('localtime', inplace=True)
        # DfTemp.index = pd.to_datetime(DfTemp.index)
        # #填充替换NaN
        # DfTemp = DfTemp.ffill()
        # DfTemp = DfTemp.bfill()
        # DfTemp = DfTemp.fillna(0)
        # value, unit = time_util.split_time_delta(resample_interval)
        # if pd.Timedelta(str(value)+unit) > pd.Timedelta('1min'):
        #     resample_interval = time_util.replace_to_resample(
        #         resample_interval)
        #     #实际取得的测点和自定义的可能不一致会报错
        #     common_columns = DfTemp.columns.intersection(points).tolist()
        #     DfTemp = DfTemp[common_columns].resample(
        #         resample_interval, closed='left').mean().ffill()#  # FIXME
        #     DfTemp = DfTemp[common_columns].astype(int)
        # #重命名
        # if 'WTUR.TurbineSts' not in DfTemp.columns.tolist():
        #     DfTemp.rename(columns={'WTUR.TurbineSts_Map':'WTUR.TurbineSts'}, inplace=True)
        #算法名
        DfTemp['algorithm'] = algorithmName
        DfTemp['assetId'] = assetId
        DfTemp['wtid'] = turbinenames[assetId]
    return DfTemp


async def getAiData(algorithmName: str, startTime, endTime, assetId: str, points, resample_interval, algorithms_configs):

    turbinenames = dict(zip(algorithms_configs[algorithmName]['param_assetIds'],algorithms_configs[algorithmName]['param_turbine_num']))
    # print(startTime, endTime, assetId, points, resample_interval)
    ResponsePoint = None
    # time.sleep(5)
    if time_util.use_raw_api(resample_interval):
        params = {"assetIds": assetId,
                  "pointIds": ','.join(points),
                  "startTime": startTime,
                  "endTime": endTime,
                  "itemFormat": "1",
                  "pageSize": "20000",
                  "boundaryType": 'inside'}
        ResponsePoint = await asyncio.to_thread(poseidon.urlopen, AccessKey, SecretKey, Url_ai, params)
        # ResponsePoint = await run_in_threadpool(poseidon.urlopen, AccessKey, SecretKey, Url_ai, params)
    else:
        params = {"assetIds": assetId,
                  "pointIdsWithLogic": ','.join(points),
                  "startTime": startTime,
                  "endTime": endTime,
                  "interval": "0",
                  "itemFormat": "1",
                  "pageSize": "20000"}
        ResponsePoint = await asyncio.to_thread(poseidon.urlopen,AccessKey, SecretKey, Url_ai_normalized, params)
        # ResponsePoint = await run_in_threadpool(poseidon.urlopen,
            # AccessKey, SecretKey, Url_ai_normalized, params)
    # logging.getLogger().info(ResponsePoint)
    data_logger.info(f"#####################当前算法：{algorithmName}=>风机ID/机号:{algorithms_configs[algorithmName]['param_assetIds']}/{algorithms_configs[algorithmName]['param_turbine_num']}=>设备ID:{assetId}=>数据时间范围{startTime}到{endTime}#############################")
    data_logger.info(f"数据接口url：{Url_ai_normalized}")
    data_logger.info(f"数据接口参数：{params}")
    DfTemp = pd.DataFrame()
    if ResponsePoint and ResponsePoint['data'] and len(ResponsePoint['data']['items']) > 0:
        DfTemp = pd.DataFrame(ResponsePoint['data']['items'])
        DfTemp['localtime'] = pd.to_datetime(DfTemp['localtime'],errors='coerce')
        DfTemp.set_index('localtime', inplace=True)
        # DfTemp.index = pd.to_datetime(DfTemp.index)
        # algorithm = importlib.import_module('.' + algorithmName, package='algorithms')
        # if hasattr(algorithm, 'vane_nan_num'):
        #     for realName, rename in  algConfig[algorithmName]['ai_rename'].items():
        #         if rename == 'WNAC.WindVaneDirection':
        #             break
        #     if len(algConfig[algorithmName]['ai_rename']) > 0:
        #         algorithm.vane_nan_num = algorithm.vane_nan_num + DfTemp[realName].isna().sum()
        #         data_logger.info(realName+f"缺失值的功率{list(DfTemp[DfTemp[realName].isna()]['WGEN.GenActivePW'])}")
            # print(algorithm.vane_nan_num)
        # #填充替换NaN
        # DfTemp = DfTemp.ffill()
        # DfTemp = DfTemp.bfill()
        # DfTemp = DfTemp.fillna(0)
        # value, unit = time_util.split_time_delta(resample_interval)
        # if pd.Timedelta(str(value)+unit) > pd.Timedelta('1min'):
        #     resample_interval = time_util.replace_to_resample(
        #         resample_interval)
        #     #实际取得的测点和自定义的可能不一致会报错
        #     common_columns = DfTemp.columns.intersection(points).tolist()
        #     DfTemp = DfTemp[common_columns].resample(
        #         resample_interval, closed='left').mean()  
       
        # DfTemp = DfTemp.round(4)
        #算法名
        DfTemp['algorithm'] = algorithmName
        DfTemp['assetId'] = assetId
        DfTemp['wtid'] = turbinenames[algorithms_configs[algorithmName]['param_assetIds'][-1]]
        data_logger.info(f"提取的测点有：{list(DfTemp.columns)}")
        data_logger.info(f"前10行数据：{DfTemp.iloc[:10]}")
    else:
        data_logger.info(f"没有提取到测点")
    return DfTemp

def process_dataframe(df, result):
    # 创建一个结果的副本，以避免并发写入问题
    local_result = result.copy()
    
    # 使用 pandas 的向量化操作来更新非空值
    mask = df.notna()
    local_result.update(df.where(mask))
    
    return local_result

def parallel_fill_data(df_list, result, num_processes=None):
    if num_processes is None:
        num_processes = mp.cpu_count()

    # 创建一个进程池
    with mp.Pool(processes=num_processes) as pool:
        # 使用偏函数来固定 result 参数
        process_func = partial(process_dataframe, result=result)
        
        # 使用 map 来并行处理每个数据框
        results = pool.map_async(process_func, df_list)
        pool.close()
        pool.join()

    # 合并所有结果
    for res in results.get():
        result.update(res)

    return result

# def custom_merge(df_list):
#     funStartTime = time.time()
#     # 获取所有DataFrame的行索引和列名
#     all_index = set()
#     all_columns = set()
#     for df in df_list:
#         all_index.update(df.index)
#         all_columns.update(df.columns)
    
#     # 创建一个空的DataFrame，包含所有可能的行和列
#     result = pd.DataFrame(index=sorted(all_index), columns=sorted(all_columns))
    
#     # # 填充数据
#     # for df in df_list:
#     #     for idx in df.index:
#     #         for col in df.columns:
#     #             if pd.notna(df.loc[idx, col]):  # 只更新非空值
#     #                 result.loc[idx, col] = df.loc[idx, col]

#     # #多线程处理
#     # result = parallel_fill_data(df_list, result, 3)

#     for df in df_list:
#         local_result = pd.DataFrame(index=sorted(all_index), columns=sorted(all_columns))
#         mask = df.notna()
#         local_result.update(df.where(mask))
#         result.update(local_result)

#     time_logger.info(f'###########################执行一次 custom_merge pandas表融合时长{time.time()-funStartTime}秒###################')
#     return result

def custom_merge(df_list):
    funStartTime = time.time()
    # 1. 转换所有DataFrame为Dask DataFrame
    ddfs = [dd.from_pandas(df, npartitions=4) for df in df_list]
    
    # 2. 获取所有唯一索引和列名
    all_index = dd.concat([ddf.index for ddf in ddfs]).unique().compute()
    all_columns = list(set().union(*[ddf.columns for ddf in ddfs]))
    
    # 3. 创建完整索引的空DataFrame作为基础
    base_df = dd.from_pandas(
        pd.DataFrame(index=all_index, columns=all_columns), 
        npartitions=4
    )
    merged = base_df.compute()
    for df in df_list:
        # _, merged = df.align(merged, join="outer", axis=None)
        mask = df.notna()
        merged.update(df.where(mask))
    # # 4. 定义合并函数
    # def safe_update(base, new):
    #     # 仅在base值为nan时更新
    #     mask = base.isna()
    #     return base.where(~mask, new)
    
    # # 5. 逐步合并所有DataFrame
    # merged = base_df
    # for ddf in ddfs:
    #     # 对齐索引和列
    #     aligned = ddf.compute().reindex(index=all_index, columns=all_columns) #aligned, _ = ddf.align(base_df, join="outer", axis=None)#
    #     # 安全更新
    #     # map_partitions 是 Dask 的核心方法之一，它：

    #     # 将函数应用于每个分区（并行处理）

    #     # 保持 DataFrame 结构不变

    #     # 适用于自定义的分区级操作
    #     merged = merged.map_partitions(
    #         lambda df: safe_update(df, aligned.loc[df.index]),
    #         meta=merged._meta # 指定返回类型
    #     )
    
    result =  merged#.compute()

    time_logger.info(f'###########################执行一次 custom_merge pandas表融合时长{time.time()-funStartTime}秒###################')
    return result


# async def TimeDeviceSlice(algorithms_configs): #, ai_points, resample_interval, getData
#     funStartTime = time.time()
#     # 按算法种类、时间和设备分片
#     time_asset_param = []
#     di_time_asset_param = []
#     for key, value in algorithms_configs.items():
#         # print(key)
#         # if algorithms_configs[key]['PrepareTurbines'] == True:

#         # #ai测点分组，为了解决请求数据时测点数量的限制以及快速合并数据
#         # numPoints = algorithms_configs[key]['pointNum']
#         # if len(algorithms_configs[key]["aiPoints"]) > numPoints:   
#         #     accumCount = 0
#         #     groupIndex = 0
#         #     while len(algorithms_configs[key]["aiPoints"][accumCount:]) >= numPoints: 
#         #         algorithms_configs[key]["aiPoints"+str(groupIndex)] = algorithms_configs[key]["aiPoints"][accumCount:accumCount+numPoints]
#         #         accumCount += numPoints
#         #         groupIndex += 1
#         # algorithms_configs[key]["groupNum"] = groupIndex

#         # 私有设备id扁平化
#         privateIds = []
#         for ids in value['param_private_assetIds']:
#             privateIds += ids
#         # privateIds = value['param_private_assetIds'] #[风机1[model1(id),model2(id)], 风机2[model1(id),model2(id)]]
#         assetIds = value['param_assetIds']
#         startTime = value['startTime']
#         endTime = value['endTime']
#         #di测点一般7天有一次数据, 小于7天可能获取不到数据
#         di_startTime = value['startTime']
#         di_endTime = value['endTime']
#         # if di_endTime - di_startTime < timedelta(days=7):
#         #     di_startTime = di_endTime - timedelta(days=7)
#         #时间分片
#         date_range = []
#         timeUnit = algorithms_configs[key]['timeSlice'].split("=")[0]
#         timeValue = algorithms_configs[key]['timeSlice'].split("=")[-1]
#         if timeUnit == "hours":
#             if endTime - startTime > timedelta(hours=float(timeValue)): #hours=12,hours=4, minutes=10
#                 date_range = pd.date_range(startTime, endTime, freq=str(timeValue)+"h").strftime(
#                     '%Y-%m-%d %H:%M:%S').to_list() #12h, 4h, 10min
#             else:
#                 date_range = [startTime.strftime('%Y-%m-%d %H:%M:%S'), endTime.strftime('%Y-%m-%d %H:%M:%S')]
#         time_param = [(key, date_range[i], date_range[i + 1])
#                     for i in range(len(date_range) - 1)]
#         #针对di时间
#         di_date_range = []
#         di_timeUnit = algorithms_configs[key]['timeSlice'].split("=")[0]
#         di_timeValue = algorithms_configs[key]['timeSlice'].split("=")[-1]
#         if di_timeUnit == "hours":
#             if di_endTime - di_startTime > timedelta(hours=float(di_timeValue)): #hours=12, hours=4, minutes=10
#                 di_date_range = pd.date_range(di_startTime, di_endTime, freq=str(timeValue)+"h").strftime(
#                     '%Y-%m-%d %H:%M:%S').to_list() #12h, 4h, 10min
#             else:
#                 di_date_range = [di_startTime.strftime('%Y-%m-%d %H:%M:%S'), di_endTime.strftime('%Y-%m-%d %H:%M:%S')]
#         di_time_param = [(key, di_date_range[i], di_date_range[i + 1])
#                     for i in range(len(di_date_range) - 1)]
#         # 加上设备（笛卡尔积）
#         if len(value["privatePoints"])>0:
#             # for modelKey, pointsValue in value["privatePoints"].items():
#             time_asset_param += product(time_param, privateIds)
#         if len(value["aiPoints"]) > 0 or len(value["generalPoints"])>0:
#             time_asset_param += product(time_param, assetIds)
#         if len(value["diPoints"]) + len(value["cjDiPoints"]) + len(value["tyDiPoints"])> 0:
#             di_time_asset_param += product(di_time_param, assetIds)
#     if len(time_asset_param) == 0 and len(di_time_asset_param) == 0:
#         return {}, {}, {}, {}, {}, {}
#     final_time_asset_param = [(item[0][0], item[0][1], item[0][2], item[1])
#                               for item in time_asset_param]  #(algorithm, startTime,startTime+12,turbineId)
#     di_final_time_asset_param = [(item[0][0], item[0][1], item[0][2], item[1])
#                               for item in di_time_asset_param]  #(algorithm, startTime,startTime+12,turbineId)
#     # getAiDataWithTimeFunc = partial(getAiData, points=ai_points, resample_interval=resample_interval)
#     # 获取ai数据
#     timeout = 120#120 #180 #秒
#     # aiTasks = [getAiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["aiPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in final_time_asset_param if len(algorithms_configs[time_tuple[0]]["aiPoints"])>0]
#     aiTasks = []
#     #ai测点分组，为了解决请求数据时测点数量的限制以及快速合并数据
#     numPoints = algorithms_configs[key]['pointNum']
#     for time_tuple in final_time_asset_param:
#         if len(algorithms_configs[time_tuple[0]]["aiPoints"])>0:
#             if len(algorithms_configs[time_tuple[0]]["aiPoints"]) > numPoints:
#                 accumCount = 0
#                 while len(algorithms_configs[time_tuple[0]]["aiPoints"][accumCount:]) >= numPoints:
#                     if Platform == "ZhongTai":
#                         aiTasks.append([])
#                         aiTasks[-1].append(getAiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["aiPoints"][accumCount:accumCount+numPoints], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs))
#                     elif Platform == "WuLianWang":
#                         aiTasks.append([])
#                         aiTasks[-1].append(getAiDataIntel(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["aiPoints"][accumCount:accumCount+numPoints], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs))
#                     accumCount += numPoints
#                 if len(algorithms_configs[time_tuple[0]]["aiPoints"][accumCount:]) > 0:
#                     if Platform == "ZhongTai":
#                         aiTasks.append([])
#                         aiTasks[-1].append(getAiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["aiPoints"][accumCount:], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs))
#                     elif Platform == "WuLianWang":
#                         aiTasks.append([])
#                         aiTasks[-1].append(getAiDataIntel(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["aiPoints"][accumCount:], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs))
#             else:
#                 if Platform == "ZhongTai":
#                     aiTasks.append([])
#                     aiTasks[-1].append(getAiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["aiPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs))
#                 elif Platform == "WuLianWang":
#                     aiTasks.append([])
#                     aiTasks[-1].append(getAiDataIntel(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["aiPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs))

#     aiResults = []
#     if len(aiTasks) > 0:
#         for i in range(len(aiTasks)):
#             done, pending = await asyncio.wait(aiTasks[i], timeout=timeout)   
#             aiResults.append([])
#             aiResults[-1] = [task.result() for task in done]
#             tmp = [task.cancel() for task in pending]
#     # else:
#     #     aiResults = []
#     # 获取di数据
#     if Platform == "ZhongTai":
#         diTasks = [getDiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["diPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in di_final_time_asset_param if len(algorithms_configs[time_tuple[0]]["diPoints"])>0]
#     elif Platform == "WuLianWang":
#         diTasks = [getDiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["diPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in di_final_time_asset_param if len(algorithms_configs[time_tuple[0]]["diPoints"])>0]
#     if len(diTasks) > 0:
#         done, pending = await asyncio.wait(diTasks, timeout=timeout)   
#         diResults = [task.result() for task in done]
#         tmp = [task.cancel() for task in pending]
#     else:
#         diResults = []
#     # 获取cj_di数据
#     if Platform == "ZhongTai":
#         cjDiTasks = [getDiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["cjDiPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in di_final_time_asset_param if len(algorithms_configs[time_tuple[0]]["cjDiPoints"])>0]
#     elif Platform == "WuLianWang":
#         cjDiTasks = [getDiDataIntel(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["cjDiPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in di_final_time_asset_param if len(algorithms_configs[time_tuple[0]]["cjDiPoints"])>0]
#     if len(cjDiTasks) > 0:
          
#         done, pending = await asyncio.wait(cjDiTasks, timeout=timeout)   
#         cjDiResults = [task.result() for task in done]
#         tmp = [task.cancel() for task in pending]
#     else:
#         cjDiResults = []
#     # 获取ty_di数据
#     if Platform == "ZhongTai":
#         tyDiTasks = [getDiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["tyDiPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in di_final_time_asset_param if len(algorithms_configs[time_tuple[0]]["tyDiPoints"])>0]
#     elif Platform == "WuLianWang":
#         tyDiTasks = [getDiDataIntel(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["tyDiPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in di_final_time_asset_param if len(algorithms_configs[time_tuple[0]]["tyDiPoints"])>0]
#     if len(tyDiTasks) > 0:
          
#         done, pending = await asyncio.wait(tyDiTasks, timeout=timeout)   
#         tyDiResults = [task.result() for task in done]
#         tmp = [task.cancel() for task in pending]
#     else:
#         tyDiResults = []
#     # 获取general数据
#     if Platform == "ZhongTai":
#         generalTasks = [getGeneralData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["generalPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in final_time_asset_param if len(algorithms_configs[time_tuple[0]]["generalPoints"])>0]
#     elif Platform == "WuLianWang":
#         generalTasks = [getGeneralDataIntel(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["generalPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in final_time_asset_param if len(algorithms_configs[time_tuple[0]]["generalPoints"])>0]
#     if len(generalTasks) > 0:
#         done, pending = await asyncio.wait(generalTasks, timeout=timeout)   
#         generalResults = [task.result() for task in done]
#         tmp = [task.cancel() for task in pending]
#     else:
#         generalResults = []

#     time_logger.info(f'###########################执行一次 TimeDeviceSlice 请求总时长{time.time()-funStartTime}秒###################')
#     funStartTime = time.time()
#     #存储数据结果
#     ai_df = {}
#     di_df = {}
#     cj_di_df = {}
#     ty_di_df = {}
#     general_df = {}
#     private_df = {}
#     for key, value in algorithms_configs.items():
#         name = importlib.import_module('.' + key, package='algorithms')
#         ###################################################################
#         #组内测点纵向合并时间，组件横向合并各测点
#         aiResultList = []
#         ai_df[key] = pd.DataFrame()
#         #遍历组
#         for groupResult in aiResults:
#             inGroupResult = pd.DataFrame()
#             #遍历时间块
#             for result in groupResult:
#                 if result.empty == False:
#                     aiResultList.append(result.loc[result['algorithm']==key])
#                 else:
#                     aiResultList.append(pd.DataFrame())
        
#                 if len(aiResultList) > 0:
#                     inGroupResult = pd.concat(aiResultList)
#                     inGroupResult = inGroupResult[~inGroupResult.index.duplicated()]
#                     if 'timestamp' in inGroupResult.columns.to_list():
#                         inGroupResult.drop('timestamp', axis=1, inplace=True)
#             ai_df[key] = pd.concat([ai_df[key], inGroupResult], axis=1)
#         # ai_df[key].drop('assetId', axis=1, inplace=True)
#         ai_df[key] = ai_df[key].dropna(axis=1, thresh=int(len(ai_df[key])*0.1))
#         ai_df[key].loc[:,ai_df[key].dtypes=='float64'] = ai_df[key].loc[:,ai_df[key].dtypes=='float64'].astype('float32')
#         #重命名
#         if len(name.ai_rename) != 0:
#             for pointkey, evalue in name.ai_rename.items():
#                 if pointkey in ai_df[key].columns.tolist():
#                     ai_df[key].rename(columns={pointkey:evalue}, inplace=True)
#                     # if pointkey in name.ai_points:
#                         # index_key = name.ai_points.index(pointkey)
#                         # name.ai_points[index_key] = evalue
#         #去重复列名
#         ai_df[key] = ai_df[key].dropna(axis=1,thresh=int(len(ai_df[key])*0.1))#某列非空值数量小于总数的10%剔除该列
#         ai_df[key] = ai_df[key].loc[:,~ai_df[key].columns.duplicated()]
#         # elif 'WGEN.GenSpdInstant' in ai_df[key].columns.tolist():
#         #     ai_df[key].rename(columns={'WGEN.GenSpdInstant':'WGEN.GenSpd'}, inplace=True
#         ###################################################################
#         diResultList = []
#         for result in diResults:
#             if result.empty == False:
#                 diResultList.append(result.loc[result['algorithm']==key])
#             else:
#                 diResultList.append(pd.DataFrame())
#         di_df[key] = pd.concat(diResultList)
#         if len(di_df[key]) > 0:
#             di_df[key] = di_df[key][~di_df[key].index.duplicated()]#时间重复
#             if 'timestamp' in di_df[key].columns.to_list():
#                 di_df[key].drop('timestamp', axis=1, inplace=True)
#             # di_df[key].drop('assetId', axis=1, inplace=True)
#             # di_df[key] = di_df[key].dropna(axis=1,how='all')
#             # if (endTime - startTime) != (di_endTime - di_startTime):
#             #     resample_interval = algorithms_configs[key]["resampleTime"]
#             #     resample_interval = time_util.replace_to_resample(
#             #     resample_interval)
#             #     if startTime < di_df[key].index.min() or startTime > di_df[key].index.max():
#             #         di_df[key].loc[startTime] = np.nan
#             #     if endTime < di_df[key].index.min() or endTime > di_df[key].index.max():
#             #         di_df[key].loc[endTime] = np.nan
#             #     #实际取得的测点和自定义的可能不一致会报错
#             #     # common_columns = DfTemp.columns.intersection(points).tolist()
#             #     di_df[key] = di_df[key].resample(resample_interval, closed='left').ffill()
#             #     di_df[key] = di_df[key].ffill()
#             #     di_df[key] = di_df[key].bfill()
#             #     di_df[key] = di_df[key][(di_df[key].index >= startTime) & (di_df[key].index <= endTime)]
#             #重命名
#             if len(name.di_rename) != 0:
#                 for pointkey, evalue in name.di_rename.items():
#                     if pointkey in di_df[key].columns.tolist():
#                         di_df[key].rename(columns={pointkey:evalue}, inplace=True)
#                         # if pointkey in name.di_points:
#                         #     index_key = name.di_points.index(pointkey)
#                         #     name.di_points[index_key] = evalue
#             #去重复列名
#             di_df[key] = di_df[key].dropna(axis=1,how='all')
#             di_df[key] = di_df[key].loc[:,~di_df[key].columns.duplicated()]
#             ####剔除连续时间重复状态
#             di_df[key]['sta1'] = di_df[key]['statel'].shift(periods=1, axis=0)
#             di_df[key]['shift'] = di_df[key]['statel'] - di_df[key]['sta1']
#             di_df[key] = di_df[key].loc[di_df[key]['shift']!=0, ['statel', 'assetId']]
#         else:
#             di_df[key] = pd.DataFrame()
#         ###################################################################
#         cjDiResultList = []
#         for result in cjDiResults:
#             if result.empty == False:
#                 cjDiResultList.append(result.loc[result['algorithm']==key])
#             else:
#                 cjDiResultList.append(pd.DataFrame())
#         cj_di_df[key] = pd.concat(cjDiResultList)
#         if len(cj_di_df[key]) > 0:
#             cj_di_df[key] = cj_di_df[key][~cj_di_df[key].index.duplicated()]#时间重复
#             if 'timestamp' in cj_di_df[key].columns.to_list():
#                 cj_di_df[key].drop('timestamp', axis=1, inplace=True)
#             # cj_di_df[key].drop('assetId', axis=1, inplace=True)
#             # cj_di_df[key] = cj_di_df[key].dropna(axis=1,how='all')
#             # if (endTime - startTime) != (di_endTime - di_startTime):
#             #     resample_interval = algorithms_configs[key]["resampleTime"]
#             #     resample_interval = time_util.replace_to_resample(
#             #     resample_interval)
#             #     if startTime < cj_di_df[key].index.min() or startTime > cj_di_df[key].index.max():
#             #         cj_di_df[key].loc[startTime] = np.nan
#             #     if endTime < cj_di_df[key].index.min() or endTime > cj_di_df[key].index.max():
#             #         cj_di_df[key].loc[endTime] = np.nan
#             #     #实际取得的测点和自定义的可能不一致会报错
#             #     # common_columns = DfTemp.columns.intersection(points).tolist()
#             #     cj_di_df[key] = cj_di_df[key].resample(resample_interval, closed='left').ffill()
#             #     cj_di_df[key] = cj_di_df[key].ffill()
#             #     cj_di_df[key] = cj_di_df[key].bfill()
#             #     cj_di_df[key] = cj_di_df[key][(cj_di_df[key].index >= startTime) & (cj_di_df[key].index <= endTime)]
#             #重命名
#             if len(name.cj_di_rename) != 0:
#                 for pointkey, evalue in name.cj_di_rename.items():
#                     if pointkey in cj_di_df[key].columns.tolist():
#                         cj_di_df[key].rename(columns={pointkey:evalue}, inplace=True)
#                         # if pointkey in name.cj_di_points:
#                         #     index_key = name.cj_di_points.index(pointkey)
#                         #     name.cj_di_points[index_key] = evalue
#             #去重复列名
#             cj_di_df[key] = cj_di_df[key].dropna(axis=1,how='all')
#             cj_di_df[key] = cj_di_df[key].loc[:,~cj_di_df[key].columns.duplicated()]
#             ####剔除连续时间重复状态
#             cj_di_df[key]['sta1'] = cj_di_df[key]['state'].shift(periods=1, axis=0)
#             cj_di_df[key]['shift'] = cj_di_df[key]['state'] - cj_di_df[key]['sta1']
#             cj_di_df[key] = cj_di_df[key].loc[cj_di_df[key]['shift']!=0, ['state', 'assetId']]
#         else:
#             cj_di_df[key] = pd.DataFrame()
#         ###################################################################
#         tyDiResultList = []
#         for result in tyDiResults:
#             if result.empty == False:
#                 tyDiResultList.append(result.loc[result['algorithm']==key])
#             else:
#                 tyDiResultList.append(pd.DataFrame())
#         ty_di_df[key] = pd.concat(tyDiResultList)
#         if len(ty_di_df[key]) > 0:
#             ty_di_df[key] = ty_di_df[key][~ty_di_df[key].index.duplicated()]#时间重复
#             if 'timestamp' in ty_di_df[key].columns.to_list():
#                 ty_di_df[key].drop('timestamp', axis=1, inplace=True)
#             # ty_di_df[key].drop('assetId', axis=1, inplace=True)
#             # ty_di_df[key] = ty_di_df[key].dropna(axis=1,how='all')
#             # if (endTime - startTime) != (di_endTime - di_startTime):
#             #     resample_interval = algorithms_configs[key]["resampleTime"]
#             #     resample_interval = time_util.replace_to_resample(
#             #     resample_interval)
#             #     if startTime < ty_di_df[key].index.min() or startTime > ty_di_df[key].index.max():
#             #        ty_di_df[key].loc[startTime] = np.nan
#             #     if endTime < ty_di_df[key].index.min() or endTime > ty_di_df[key].index.max():
#             #         ty_di_df[key].loc[endTime] = np.nan
#             #     #实际取得的测点和自定义的可能不一致会报错
#             #     # common_columns = DfTemp.columns.intersection(points).tolist()
#             #     ty_di_df[key] = ty_di_df[key].resample(resample_interval, closed='left').ffill()
#             #     ty_di_df[key] = ty_di_df[key].ffill()
#             #     ty_di_df[key] = ty_di_df[key].bfill()
#             #     ty_di_df[key] = ty_di_df[key][(ty_di_df[key].index >= startTime) & (ty_di_df[key].index <= endTime)]
#             #重命名
#             if len(name.ty_di_rename) != 0:
#                 for pointkey, evalue in name.ty_di_rename.items():
#                     if pointkey in ty_di_df[key].columns.tolist():
#                         ty_di_df[key].rename(columns={pointkey:evalue}, inplace=True)
#                         # if pointkey in name.ty_di_points:
#                         #     index_key = name.ty_di_points.index(pointkey)
#                         #     name.ty_di_points[index_key] = evalue
#             #去重复列名
#             ty_di_df[key] = ty_di_df[key].dropna(axis=1,how='all')
#             ty_di_df[key] = ty_di_df[key].loc[:,~ty_di_df[key].columns.duplicated()]
#             ####剔除连续时间重复状态
#             ty_di_df[key]['sta1'] = ty_di_df[key]['statety'].shift(periods=1, axis=0)
#             ty_di_df[key]['shift'] = ty_di_df[key]['statety'] - ty_di_df[key]['sta1']
#             ty_di_df[key] = ty_di_df[key].loc[ty_di_df[key]['shift']!=0, ['statety', 'assetId']]
#         else:
#             ty_di_df[key] = pd.DataFrame()
#         ###################################################################
#         generalResultList = []
#         for result in generalResults:
#             if result.empty == False:
#                 generalResultList.append(result.loc[result['algorithm']==key])
#             else:
#                 generalResultList.append(pd.DataFrame())
#         general_df[key] = pd.concat(generalResultList)
#         if len(general_df[key]) > 0:
#             general_df[key] = general_df[key][~general_df[key].index.duplicated()]
#             if 'timestamp' in general_df[key].columns.to_list():
#                 general_df[key].drop('timestamp',axis=1,inplace=True)
#             # general_df[key].drop('assetId',axis=1,inplace=True)
#             general_df[key] = general_df[key].dropna(axis=1,how='all')
#             #重命名
#             if len(name.general_rename) != 0:
#                 for pointkey, evalue in name.general_rename.items():
#                     if pointkey in general_df[key].columns.tolist():
#                         general_df[key].rename(columns={pointkey:evalue}, inplace=True)
#                         # if pointkey in name.general_points:
#                         #     index_key = name.general_points.index(pointkey)
#                         #     name.general_points[index_key] = evalue
#             #去重复列名
#             general_df[key] = general_df[key].dropna(axis=1,how='all')
#             general_df[key] = general_df[key].loc[:,~general_df[key].columns.duplicated()]
#             ####剔除连续时间重复故障
#             general_df[key]['flt1'] = general_df[key]['fault'].shift(periods=1, axis=0)
#             general_df[key]['shift'] = general_df[key]['fault'] - general_df[key]['flt1']
#             general_df[key] = general_df[key].loc[general_df[key]['shift']!=0, ['fault', 'assetId']]
#         else:
#             general_df[key] = pd.DataFrame()
#         ###################################################################
#         # privateResultList = []
#         # for result in privateResults:
#         #     if result.empty == False:
#         #         privateResultList.append(result.loc[result['algorithm']==key])
#         #     else:
#         #         privateResultList.append(pd.DataFrame())
#         # private_df[key] = pd.concat(privateResultList)
#         # if len(privateResults) > 0:
#         #     private_df[key] = private_df[key][~private_df[key].index.duplicated()]
#         # else:
#         private_df[key] = pd.DataFrame()
#         # #合并数据
#         # fn_df = pd.DataFrame()
#         # if ai_df[key].empty == False:
        
#     # df = pd.concat(results)

#     time_logger.info(f'###########################执行一次 TimeDeviceSlice 存储去重整理格式总时间{time.time()-funStartTime}秒###################')
#     return ai_df, di_df, cj_di_df, ty_di_df, general_df, private_df

async def TimeDeviceSlice(algorithms_configs): #, ai_points, resample_interval, getData
    funStartTime = time.time()
    # 按算法种类、时间和设备分片
    time_asset_param = []
    di_time_asset_param = []
    for key, value in algorithms_configs.items():
        # print(key)
        # if algorithms_configs[key]['PrepareTurbines'] == True:
        # 私有设备id扁平化
        privateIds = []
        for ids in value['param_private_assetIds']:
            privateIds += ids
        # privateIds = value['param_private_assetIds'] #[风机1[model1(id),model2(id)], 风机2[model1(id),model2(id)]]
        assetIds = value['param_assetIds']
        startTime = value['startTime']
        endTime = value['endTime']
        #di测点一般7天有一次数据, 小于7天可能获取不到数据
        di_startTime = value['startTime']
        di_endTime = value['endTime']
        # if di_endTime - di_startTime < timedelta(days=7):
        #     di_startTime = di_endTime - timedelta(days=7)
        #时间分片
        date_range = []
        timeUnit = algorithms_configs[key]['timeSlice'].split("=")[0]
        timeValue = algorithms_configs[key]['timeSlice'].split("=")[-1]
        if timeUnit == "hours":
            if endTime - startTime > timedelta(hours=float(timeValue)): #hours=12,hours=4, minutes=10
                date_range = pd.date_range(startTime, endTime, freq=str(timeValue)+"h").strftime(
                    '%Y-%m-%d %H:%M:%S').to_list() #12h, 4h, 10min
            else:
                date_range = [startTime.strftime('%Y-%m-%d %H:%M:%S'), endTime.strftime('%Y-%m-%d %H:%M:%S')]
        time_param = [(key, date_range[i], date_range[i + 1])
                    for i in range(len(date_range) - 1)]
        #针对di时间
        di_date_range = []
        di_timeUnit = algorithms_configs[key]['timeSlice'].split("=")[0]
        di_timeValue = algorithms_configs[key]['timeSlice'].split("=")[-1]
        if di_timeUnit == "hours":
            if di_endTime - di_startTime > timedelta(hours=float(di_timeValue)): #hours=12, hours=4, minutes=10
                di_date_range = pd.date_range(di_startTime, di_endTime, freq=str(timeValue)+"h").strftime(
                    '%Y-%m-%d %H:%M:%S').to_list() #12h, 4h, 10min
            else:
                di_date_range = [di_startTime.strftime('%Y-%m-%d %H:%M:%S'), di_endTime.strftime('%Y-%m-%d %H:%M:%S')]
        di_time_param = [(key, di_date_range[i], di_date_range[i + 1])
                    for i in range(len(di_date_range) - 1)]
        # 加上设备（笛卡尔积）
        if len(value["privatePoints"])>0:
            # for modelKey, pointsValue in value["privatePoints"].items():
            time_asset_param += product(time_param, privateIds)
        if len(value["aiPoints"]) > 0 or len(value["generalPoints"])>0:
            time_asset_param += product(time_param, assetIds)
        if len(value["diPoints"]) + len(value["cjDiPoints"]) + len(value["tyDiPoints"])> 0:
            di_time_asset_param += product(di_time_param, assetIds)
    if len(time_asset_param) == 0 and len(di_time_asset_param) == 0:
        return {}, {}, {}, {}, {}, {}
    final_time_asset_param = [(item[0][0], item[0][1], item[0][2], item[1])
                              for item in time_asset_param]  #(algorithm, startTime,startTime+12,turbineId)
    di_final_time_asset_param = [(item[0][0], item[0][1], item[0][2], item[1])
                              for item in di_time_asset_param]  #(algorithm, startTime,startTime+12,turbineId)
    # getAiDataWithTimeFunc = partial(getAiData, points=ai_points, resample_interval=resample_interval)
    # 获取ai数据
    timeout = 120#120 #180 #秒
    # aiTasks = [getAiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["aiPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in final_time_asset_param if len(algorithms_configs[time_tuple[0]]["aiPoints"])>0]
    aiTasks = []
    numPoints = algorithms_configs[key]['pointNum']
    for time_tuple in final_time_asset_param:
        if len(algorithms_configs[time_tuple[0]]["aiPoints"])>0:
            if len(algorithms_configs[time_tuple[0]]["aiPoints"]) > numPoints:
                accumCount = 0
                while len(algorithms_configs[time_tuple[0]]["aiPoints"][accumCount:]) >= numPoints:
                    if Platform == "ZhongTai":
                        aiTasks.append(getAiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["aiPoints"][accumCount:accumCount+numPoints], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs))
                    elif Platform == "WuLianWang":
                        aiTasks.append(getAiDataIntel(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["aiPoints"][accumCount:accumCount+numPoints], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs))
                    accumCount += numPoints
                if len(algorithms_configs[time_tuple[0]]["aiPoints"][accumCount:]) > 0:
                    if Platform == "ZhongTai":
                        aiTasks.append(getAiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["aiPoints"][accumCount:], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs))
                    elif Platform == "WuLianWang":
                        aiTasks.append(getAiDataIntel(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["aiPoints"][accumCount:], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs))
            else:
                if Platform == "ZhongTai":
                    aiTasks.append(getAiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["aiPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs))
                elif Platform == "WuLianWang":
                    aiTasks.append(getAiDataIntel(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["aiPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs))
    if len(aiTasks) > 0:
        done, pending = await asyncio.wait(aiTasks, timeout=timeout)   
        aiResults = [task.result() for task in done]
        tmp = [task.cancel() for task in pending]
    else:
        aiResults = []
    # 获取di数据
    if Platform == "ZhongTai":
        diTasks = [getDiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["diPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in di_final_time_asset_param if len(algorithms_configs[time_tuple[0]]["diPoints"])>0]
    elif Platform == "WuLianWang":
        diTasks = [getDiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["diPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in di_final_time_asset_param if len(algorithms_configs[time_tuple[0]]["diPoints"])>0]
    if len(diTasks) > 0:
        done, pending = await asyncio.wait(diTasks, timeout=timeout)   
        diResults = [task.result() for task in done]
        tmp = [task.cancel() for task in pending]
    else:
        diResults = []
    # 获取cj_di数据
    if Platform == "ZhongTai":
        cjDiTasks = [getDiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["cjDiPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in di_final_time_asset_param if len(algorithms_configs[time_tuple[0]]["cjDiPoints"])>0]
    elif Platform == "WuLianWang":
        cjDiTasks = [getDiDataIntel(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["cjDiPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in di_final_time_asset_param if len(algorithms_configs[time_tuple[0]]["cjDiPoints"])>0]
    if len(cjDiTasks) > 0:
          
        done, pending = await asyncio.wait(cjDiTasks, timeout=timeout)   
        cjDiResults = [task.result() for task in done]
        tmp = [task.cancel() for task in pending]
    else:
        cjDiResults = []
    # 获取ty_di数据
    if Platform == "ZhongTai":
        tyDiTasks = [getDiData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["tyDiPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in di_final_time_asset_param if len(algorithms_configs[time_tuple[0]]["tyDiPoints"])>0]
    elif Platform == "WuLianWang":
        tyDiTasks = [getDiDataIntel(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["tyDiPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in di_final_time_asset_param if len(algorithms_configs[time_tuple[0]]["tyDiPoints"])>0]
    if len(tyDiTasks) > 0:
          
        done, pending = await asyncio.wait(tyDiTasks, timeout=timeout)   
        tyDiResults = [task.result() for task in done]
        tmp = [task.cancel() for task in pending]
    else:
        tyDiResults = []
    # 获取general数据
    if Platform == "ZhongTai":
        generalTasks = [getGeneralData(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["generalPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in final_time_asset_param if len(algorithms_configs[time_tuple[0]]["generalPoints"])>0]
    elif Platform == "WuLianWang":
        generalTasks = [getGeneralDataIntel(time_tuple[0], time_tuple[1], time_tuple[2], time_tuple[3], algorithms_configs[time_tuple[0]]["generalPoints"], algorithms_configs[time_tuple[0]]["resampleTime"], algorithms_configs) for time_tuple in final_time_asset_param if len(algorithms_configs[time_tuple[0]]["generalPoints"])>0]
    if len(generalTasks) > 0:
        done, pending = await asyncio.wait(generalTasks, timeout=timeout)   
        generalResults = [task.result() for task in done]
        tmp = [task.cancel() for task in pending]
    else:
        generalResults = []

    time_logger.info(f'###########################执行一次 TimeDeviceSlice 请求总时长{time.time()-funStartTime}秒###################')
    funStartTime = time.time()
    #存储数据结果
    ai_df = {}
    di_df = {}
    cj_di_df = {}
    ty_di_df = {}
    general_df = {}
    private_df = {}
    for key, value in algorithms_configs.items():
        name = importlib.import_module('.' + key, package='algorithms')
        ###################################################################
        aiResultList = []
        for result in aiResults:
            if result.empty == False:
                aiResultList.append(result.loc[result['algorithm']==key])
            else:
                aiResultList.append(pd.DataFrame())
        ai_df[key] = pd.DataFrame()
        if len(aiResultList) > 0:
            if algConfig[key]["customMergeFlag"]:
                ai_df[key] = custom_merge(aiResultList)
            else:
                ai_df[key] = pd.concat(aiResultList)
                ai_df[key] = ai_df[key][~ai_df[key].index.duplicated()]
            if 'timestamp' in ai_df[key].columns.to_list():
                ai_df[key].drop('timestamp', axis=1, inplace=True)
            # ai_df[key].drop('assetId', axis=1, inplace=True)
            ai_df[key] = ai_df[key].dropna(axis=1, thresh=int(len(ai_df[key])*0.1))
            ai_df[key].loc[:,ai_df[key].dtypes=='float64'] = ai_df[key].loc[:,ai_df[key].dtypes=='float64'].astype('float32')
        #重命名
        if len(name.ai_rename) != 0:
            for pointkey, evalue in name.ai_rename.items():
                if pointkey in ai_df[key].columns.tolist():
                    ai_df[key].rename(columns={pointkey:evalue}, inplace=True)
                    # if pointkey in name.ai_points:
                        # index_key = name.ai_points.index(pointkey)
                        # name.ai_points[index_key] = evalue
        #去重复列名
        ai_df[key] = ai_df[key].dropna(axis=1,thresh=int(len(ai_df[key])*0.1))#某列非空值数量小于总数的10%剔除该列
        ai_df[key] = ai_df[key].loc[:,~ai_df[key].columns.duplicated()]
        # elif 'WGEN.GenSpdInstant' in ai_df[key].columns.tolist():
        #     ai_df[key].rename(columns={'WGEN.GenSpdInstant':'WGEN.GenSpd'}, inplace=True
        ###################################################################
        diResultList = []
        for result in diResults:
            if result.empty == False:
                diResultList.append(result.loc[result['algorithm']==key])
            else:
                diResultList.append(pd.DataFrame())
        di_df[key] = pd.concat(diResultList)
        if len(di_df[key]) > 0:
            di_df[key] = di_df[key][~di_df[key].index.duplicated()]#时间重复
            if 'timestamp' in di_df[key].columns.to_list():
                di_df[key].drop('timestamp', axis=1, inplace=True)
            # di_df[key].drop('assetId', axis=1, inplace=True)
            # di_df[key] = di_df[key].dropna(axis=1,how='all')
            # if (endTime - startTime) != (di_endTime - di_startTime):
            #     resample_interval = algorithms_configs[key]["resampleTime"]
            #     resample_interval = time_util.replace_to_resample(
            #     resample_interval)
            #     if startTime < di_df[key].index.min() or startTime > di_df[key].index.max():
            #         di_df[key].loc[startTime] = np.nan
            #     if endTime < di_df[key].index.min() or endTime > di_df[key].index.max():
            #         di_df[key].loc[endTime] = np.nan
            #     #实际取得的测点和自定义的可能不一致会报错
            #     # common_columns = DfTemp.columns.intersection(points).tolist()
            #     di_df[key] = di_df[key].resample(resample_interval, closed='left').ffill()
            #     di_df[key] = di_df[key].ffill()
            #     di_df[key] = di_df[key].bfill()
            #     di_df[key] = di_df[key][(di_df[key].index >= startTime) & (di_df[key].index <= endTime)]
            #重命名
            if len(name.di_rename) != 0:
                for pointkey, evalue in name.di_rename.items():
                    if pointkey in di_df[key].columns.tolist():
                        di_df[key].rename(columns={pointkey:evalue}, inplace=True)
                        # if pointkey in name.di_points:
                        #     index_key = name.di_points.index(pointkey)
                        #     name.di_points[index_key] = evalue
            #去重复列名
            di_df[key] = di_df[key].dropna(axis=1,how='all')
            di_df[key] = di_df[key].loc[:,~di_df[key].columns.duplicated()]
            ####剔除连续时间重复状态
            di_df[key]['sta1'] = di_df[key]['statel'].shift(periods=1, axis=0)
            di_df[key]['shift'] = di_df[key]['statel'] - di_df[key]['sta1']
            di_df[key] = di_df[key].loc[di_df[key]['shift']!=0, ['statel', 'assetId']]
        else:
            di_df[key] = pd.DataFrame()
        ###################################################################
        cjDiResultList = []
        for result in cjDiResults:
            if result.empty == False:
                cjDiResultList.append(result.loc[result['algorithm']==key])
            else:
                cjDiResultList.append(pd.DataFrame())
        cj_di_df[key] = pd.concat(cjDiResultList)
        if len(cj_di_df[key]) > 0:
            cj_di_df[key] = cj_di_df[key][~cj_di_df[key].index.duplicated()]#时间重复
            if 'timestamp' in cj_di_df[key].columns.to_list():
                cj_di_df[key].drop('timestamp', axis=1, inplace=True)
            # cj_di_df[key].drop('assetId', axis=1, inplace=True)
            # cj_di_df[key] = cj_di_df[key].dropna(axis=1,how='all')
            # if (endTime - startTime) != (di_endTime - di_startTime):
            #     resample_interval = algorithms_configs[key]["resampleTime"]
            #     resample_interval = time_util.replace_to_resample(
            #     resample_interval)
            #     if startTime < cj_di_df[key].index.min() or startTime > cj_di_df[key].index.max():
            #         cj_di_df[key].loc[startTime] = np.nan
            #     if endTime < cj_di_df[key].index.min() or endTime > cj_di_df[key].index.max():
            #         cj_di_df[key].loc[endTime] = np.nan
            #     #实际取得的测点和自定义的可能不一致会报错
            #     # common_columns = DfTemp.columns.intersection(points).tolist()
            #     cj_di_df[key] = cj_di_df[key].resample(resample_interval, closed='left').ffill()
            #     cj_di_df[key] = cj_di_df[key].ffill()
            #     cj_di_df[key] = cj_di_df[key].bfill()
            #     cj_di_df[key] = cj_di_df[key][(cj_di_df[key].index >= startTime) & (cj_di_df[key].index <= endTime)]
            #重命名
            if len(name.cj_di_rename) != 0:
                for pointkey, evalue in name.cj_di_rename.items():
                    if pointkey in cj_di_df[key].columns.tolist():
                        cj_di_df[key].rename(columns={pointkey:evalue}, inplace=True)
                        # if pointkey in name.cj_di_points:
                        #     index_key = name.cj_di_points.index(pointkey)
                        #     name.cj_di_points[index_key] = evalue
            #去重复列名
            cj_di_df[key] = cj_di_df[key].dropna(axis=1,how='all')
            cj_di_df[key] = cj_di_df[key].loc[:,~cj_di_df[key].columns.duplicated()]
            ####剔除连续时间重复状态
            cj_di_df[key]['sta1'] = cj_di_df[key]['state'].shift(periods=1, axis=0)
            cj_di_df[key]['shift'] = cj_di_df[key]['state'] - cj_di_df[key]['sta1']
            cj_di_df[key] = cj_di_df[key].loc[cj_di_df[key]['shift']!=0, ['state', 'assetId']]
        else:
            cj_di_df[key] = pd.DataFrame()
        ###################################################################
        tyDiResultList = []
        for result in tyDiResults:
            if result.empty == False:
                tyDiResultList.append(result.loc[result['algorithm']==key])
            else:
                tyDiResultList.append(pd.DataFrame())
        ty_di_df[key] = pd.concat(tyDiResultList)
        if len(ty_di_df[key]) > 0:
            ty_di_df[key] = ty_di_df[key][~ty_di_df[key].index.duplicated()]#时间重复
            if 'timestamp' in ty_di_df[key].columns.to_list():
                ty_di_df[key].drop('timestamp', axis=1, inplace=True)
            # ty_di_df[key].drop('assetId', axis=1, inplace=True)
            # ty_di_df[key] = ty_di_df[key].dropna(axis=1,how='all')
            # if (endTime - startTime) != (di_endTime - di_startTime):
            #     resample_interval = algorithms_configs[key]["resampleTime"]
            #     resample_interval = time_util.replace_to_resample(
            #     resample_interval)
            #     if startTime < ty_di_df[key].index.min() or startTime > ty_di_df[key].index.max():
            #        ty_di_df[key].loc[startTime] = np.nan
            #     if endTime < ty_di_df[key].index.min() or endTime > ty_di_df[key].index.max():
            #         ty_di_df[key].loc[endTime] = np.nan
            #     #实际取得的测点和自定义的可能不一致会报错
            #     # common_columns = DfTemp.columns.intersection(points).tolist()
            #     ty_di_df[key] = ty_di_df[key].resample(resample_interval, closed='left').ffill()
            #     ty_di_df[key] = ty_di_df[key].ffill()
            #     ty_di_df[key] = ty_di_df[key].bfill()
            #     ty_di_df[key] = ty_di_df[key][(ty_di_df[key].index >= startTime) & (ty_di_df[key].index <= endTime)]
            #重命名
            if len(name.ty_di_rename) != 0:
                for pointkey, evalue in name.ty_di_rename.items():
                    if pointkey in ty_di_df[key].columns.tolist():
                        ty_di_df[key].rename(columns={pointkey:evalue}, inplace=True)
                        # if pointkey in name.ty_di_points:
                        #     index_key = name.ty_di_points.index(pointkey)
                        #     name.ty_di_points[index_key] = evalue
            #去重复列名
            ty_di_df[key] = ty_di_df[key].dropna(axis=1,how='all')
            ty_di_df[key] = ty_di_df[key].loc[:,~ty_di_df[key].columns.duplicated()]
            ####剔除连续时间重复状态
            ty_di_df[key]['sta1'] = ty_di_df[key]['statety'].shift(periods=1, axis=0)
            ty_di_df[key]['shift'] = ty_di_df[key]['statety'] - ty_di_df[key]['sta1']
            ty_di_df[key] = ty_di_df[key].loc[ty_di_df[key]['shift']!=0, ['statety', 'assetId']]
        else:
            ty_di_df[key] = pd.DataFrame()
        ###################################################################
        generalResultList = []
        for result in generalResults:
            if result.empty == False:
                generalResultList.append(result.loc[result['algorithm']==key])
            else:
                generalResultList.append(pd.DataFrame())
        general_df[key] = pd.concat(generalResultList)
        if len(general_df[key]) > 0:
            general_df[key] = general_df[key][~general_df[key].index.duplicated()]
            if 'timestamp' in general_df[key].columns.to_list():
                general_df[key].drop('timestamp',axis=1,inplace=True)
            # general_df[key].drop('assetId',axis=1,inplace=True)
            general_df[key] = general_df[key].dropna(axis=1,how='all')
            #重命名
            if len(name.general_rename) != 0:
                for pointkey, evalue in name.general_rename.items():
                    if pointkey in general_df[key].columns.tolist():
                        general_df[key].rename(columns={pointkey:evalue}, inplace=True)
                        # if pointkey in name.general_points:
                        #     index_key = name.general_points.index(pointkey)
                        #     name.general_points[index_key] = evalue
            #去重复列名
            general_df[key] = general_df[key].dropna(axis=1,how='all')
            general_df[key] = general_df[key].loc[:,~general_df[key].columns.duplicated()]
            ####剔除连续时间重复故障
            general_df[key]['flt1'] = general_df[key]['fault'].shift(periods=1, axis=0)
            general_df[key]['shift'] = general_df[key]['fault'] - general_df[key]['flt1']
            general_df[key] = general_df[key].loc[general_df[key]['shift']!=0, ['fault', 'assetId']]
        else:
            general_df[key] = pd.DataFrame()
        ###################################################################
        # privateResultList = []
        # for result in privateResults:
        #     if result.empty == False:
        #         privateResultList.append(result.loc[result['algorithm']==key])
        #     else:
        #         privateResultList.append(pd.DataFrame())
        # private_df[key] = pd.concat(privateResultList)
        # if len(privateResults) > 0:
        #     private_df[key] = private_df[key][~private_df[key].index.duplicated()]
        # else:
        private_df[key] = pd.DataFrame()
        # #合并数据
        # fn_df = pd.DataFrame()
        # if ai_df[key].empty == False:
        
    # df = pd.concat(results)

    time_logger.info(f'###########################执行一次 TimeDeviceSlice 存储去重整理格式总时间{time.time()-funStartTime}秒###################')
    return ai_df, di_df, cj_di_df, ty_di_df, general_df, private_df


async def getDataForMultiAlgorithms(algorithms_configs, state): #assetIds：风机id ,mainLog, algorithmLogs, algorithms_configs是一个算法命中的配置，不是全局配置algorithms_configs, 注意和main_job中的algorithms_configs做区别

    df_ai, df_di, df_di_cj, df_di_ty, df_general, df_private = await TimeDeviceSlice(algorithms_configs)
    funStartTime = time.time()
    final_df = {}
    countsAlg = 0
    for key, value in algorithms_configs.items():
        # if algorithms_configs[key]['PrepareTurbines'] == False:
        #     continue
        assetIds = value['param_assetIds']
        privateIds = value['param_private_assetIds']
        countsAlg += 1
        # mainLog.info(key+":"+str(countsAlg)+'/'+str(len(algorithms_configs)))
        # algorithmLogs[key].info(f'获取{assetIds}风机本算法需要的数据:')
        # algorithmLogs[key].info(key+":"+str(countsAlg)+'/'+str(len(algorithms_configs)))
        final_df[key] = pd.DataFrame()
        #获取ai数据
        if df_ai[key].empty == False:
            for index, assetId in enumerate(assetIds):
                if final_df[key].empty == False:
                    df_current_assetId = final_df[key][final_df[key]['assetId'] == assetId].copy()
                    #final_df中删除筛选到的行
                    final_df[key] = final_df[key].drop(df_current_assetId.index)
                else:
                    df_current_assetId = pd.DataFrame()
                df_add_assetId = df_ai[key][df_ai[key]['assetId'] == assetId].copy()
                #剔除重名字段
                common_columns = df_current_assetId.columns.intersection(df_add_assetId.columns)
                for tag in list(common_columns):
                    del df_add_assetId[tag]
                df_current_assetId = df_current_assetId.join(df_add_assetId, how='outer')
                df_current_assetId = df_current_assetId.ffill()#用前面行/列的值填充空值
                df_current_assetId = df_current_assetId.bfill()#用后面行/列的值填充空值
                # allow_points = list(set(df_current_assetId.columns) & set(algorithms_configs[key]["aiPoints"]))
                # df_current_assetId[allow_points] = df_current_assetId[allow_points].ffill()#用前面行/列的值填充空值
                # df_current_assetId[allow_points] = df_current_assetId[allow_points].bfill()#用后面行/列的值填充空值
                final_df[key] = pd.concat([final_df[key], df_current_assetId])
            
        #general数据
        if df_general[key].empty == False:
            for index, assetId in enumerate(assetIds):
                if final_df[key].empty == False:
                    df_current_assetId = final_df[key][final_df[key]['assetId'] == assetId].copy()
                    #final_df中删除筛选到的行
                    final_df[key] = final_df[key].drop(df_current_assetId.index)
                else:
                    df_current_assetId = pd.DataFrame()
                df_add_assetId = df_general[key][df_general[key]['assetId'] == assetId].copy()
                #剔除重名字段
                common_columns = df_current_assetId.columns.intersection(df_add_assetId.columns)
                for tag in list(common_columns):
                    del df_add_assetId[tag]
                df_current_assetId = df_current_assetId.join(df_add_assetId, how='outer')
                df_current_assetId = df_current_assetId.ffill()#用前面行/列的值填充空值
                df_current_assetId = df_current_assetId.bfill()#用后面行/列的值填充空值
                # allow_points = list(set(df_current_assetId.columns) & set(algorithms_configs[key]["generalPoints"]))
                # df_current_assetId[allow_points] = df_current_assetId[allow_points].ffill()#用前面行/列的值填充空值
                # df_current_assetId[allow_points] = df_current_assetId[allow_points].bfill()#用后面行/列的值填充空值
                final_df[key] = pd.concat([final_df[key], df_current_assetId])
        else:
            final_df[key]['fault'] = 0
        #di数据
        if df_di[key].empty == False:
            for index, assetId in enumerate(assetIds):
                if final_df[key].empty == False:
                    df_current_assetId = final_df[key][final_df[key]['assetId'] == assetId].copy()
                    #final_df中删除筛选到的行
                    final_df[key] = final_df[key].drop(df_current_assetId.index)
                else:
                    df_current_assetId = pd.DataFrame()
                df_add_assetId = df_di[key][df_di[key]['assetId'] == assetId].copy()
                #剔除重名字段
                common_columns = df_current_assetId.columns.intersection(df_add_assetId.columns)
                for tag in list(common_columns):
                    del df_add_assetId[tag]
                df_current_assetId = df_current_assetId.join(df_add_assetId, how='outer')
                df_current_assetId = df_current_assetId.ffill()#用前面行/列的值填充空值
                df_current_assetId = df_current_assetId.bfill()#用后面行/列的值填充空值
                # allow_points = list(set(df_current_assetId.columns) & set(algorithms_configs[key]["diPoints"]))
                # df_current_assetId[allow_points] = df_current_assetId[allow_points].ffill()#用前面行/列的值填充空值
                # df_current_assetId[allow_points] = df_current_assetId[allow_points].bfill()#用后面行/列的值填充空值
                final_df[key] = pd.concat([final_df[key], df_current_assetId])
        else:
            final_df[key]['statel'] = 90002
        
        #cj_di数据
        if df_di_cj[key].empty == False:
            for index, assetId in enumerate(assetIds):
                if final_df[key].empty == False:
                    df_current_assetId = final_df[key][final_df[key]['assetId'] == assetId].copy()
                    #final_df中删除筛选到的行
                    final_df[key] = final_df[key].drop(df_current_assetId.index)
                else:
                    df_current_assetId = pd.DataFrame()
                df_add_assetId = df_di_cj[key][df_di_cj[key]['assetId'] == assetId].copy()
                #剔除重名字段
                common_columns = df_current_assetId.columns.intersection(df_add_assetId.columns)
                for tag in list(common_columns):
                    del df_add_assetId[tag]
                df_current_assetId = df_current_assetId.join(df_add_assetId, how='outer')
                df_current_assetId = df_current_assetId.ffill()#用前面行/列的值填充空值
                df_current_assetId = df_current_assetId.bfill()#用后面行/列的值填充空值
                # allow_points = list(set(df_current_assetId.columns) & set(algorithms_configs[key]["diPoints"]))
                # df_current_assetId[allow_points] = df_current_assetId[allow_points].ffill()#用前面行/列的值填充空值
                # df_current_assetId[allow_points] = df_current_assetId[allow_points].bfill()#用后面行/列的值填充空值
                final_df[key] = pd.concat([final_df[key], df_current_assetId])
        else:
            final_df[key]['state'] = state #algorithms_configs['state']

        #ty_di数据
        if df_di_ty[key].empty == False:
            for index, assetId in enumerate(assetIds):
                if final_df[key].empty == False:
                    df_current_assetId = final_df[key][final_df[key]['assetId'] == assetId].copy()
                    #final_df中删除筛选到的行
                    final_df[key] = final_df[key].drop(df_current_assetId.index)
                else:
                    df_current_assetId = pd.DataFrame()
                df_add_assetId = df_di_ty[key][df_di_ty[key]['assetId'] == assetId].copy()
                #剔除重名字段
                common_columns = df_current_assetId.columns.intersection(df_add_assetId.columns)
                for tag in list(common_columns):
                    del df_add_assetId[tag]
                df_current_assetId = df_current_assetId.join(df_add_assetId, how='outer')
                df_current_assetId = df_current_assetId.ffill()#用前面行/列的值填充空值
                df_current_assetId = df_current_assetId.bfill()#用后面行/列的值填充空值
                # allow_points = list(set(df_current_assetId.columns) & set(algorithms_configs[key]["diPoints"]))
                # df_current_assetId[allow_points] = df_current_assetId[allow_points].ffill()#用前面行/列的值填充空值
                # df_current_assetId[allow_points] = df_current_assetId[allow_points].bfill()#用后面行/列的值填充空值
                final_df[key] = pd.concat([final_df[key], df_current_assetId])
        else:
            final_df[key]['statety'] = 71

        # 获取private数据
        if df_private[key].empty == False:
            # 多个模型测点id
            df_models_private = pd.DataFrame()
            multiColumnIndex = [] #记录测点类型id
            transmit_privateIds = list(zip(*privateIds))#[机号1:(测点模型id1,测点模型id2,...),...]->[测点模型id1:(机号1,机号2,...),测点模型id2:(机号1,机号2,...)...]
            for classIndex, assets in enumerate(transmit_privateIds): #遍历测点类型
                #单个模型测点id
                multi_index = pd.MultiIndex.from_tuples([("privateAssetId","class"+str(classIndex))])
                df_private[key].rename(columns={"assetId":multi_index[0]}, inplace=True)
                #删除相同列名
                common_columns_df_private = df_models_private.columns.intersection(df_private[key].columns)
                for tag in list(common_columns_df_private):
                    del df_private[tag]
                if len(list(df_private[key].columns)) > 1:
                    multiColumnIndex.append(('privateAssetId', "class"+str(classIndex)))
                    df_models_private = pd.concat([df_models_private, df_private[key]], axis=1)

            #遍历所有风机id
            for index, assetId in enumerate(assetIds):
                if final_df[key].empty == False:
                    df_current_assetId = final_df[key][final_df[key]['assetId'] == assetId].copy()
                    #final_df中删除筛选到的行
                    final_df[key] = final_df[key].drop(df_current_assetId.index)
                else:
                    df_current_assetId = pd.DataFrame()
                # print(df_models_private.columns)
                # print(multiColumnIndex)
                #此方法提取出的值为NaN
                # df_add_assetId = df_models_private[df_models_private[multiColumnIndex].isin(privateIds[index])].copy()
                #处理本风机的多个类型测点id
                if df_models_private.empty == False:
                    condition = None
                    modelIds = list(set(privateIds[index]))
                    #提取同一风机的所有类型的测点id
                    for modelId in df_models_private[multiColumnIndex].columns.tolist():
                        if type(condition) == type(None):
                            condition = df_models_private[modelId].isin(modelIds) 
                        else:
                            condition = condition & df_models_private[modelId].isin(modelIds)
                    df_add_assetId = df_models_private[condition]
                    #剔除重名字段
                    common_columns = df_current_assetId.columns.intersection(df_add_assetId.columns)
                    for tag in list(common_columns):
                        del df_add_assetId[tag]
                    df_current_assetId = df_current_assetId.join(df_add_assetId, how='outer')
                    df_current_assetId = df_current_assetId.ffill()#用前面行/列的值填充空值
                    df_current_assetId = df_current_assetId.bfill()#用前面行/列的值填充空值
                    # allow_points = list(set(df_current_assetId.columns) & set(algorithms_configs[key]["privatePoints"]))
                    # df_current_assetId[allow_points] = df_current_assetId[allow_points].ffill()#用前面行/列的值填充空值
                    # df_current_assetId[allow_points] = df_current_assetId[allow_points].bfill()#用前面行/列的值填充空值
                final_df[key] = pd.concat([final_df[key], df_current_assetId])
    time_logger.info(f'###########################执行一次 getDataForMultiAlgorithms 去除 TimeDeviceSlice 后的总时间{time.time()-funStartTime}秒###################')
    #算法名：(数据1分钟)， 数据10分钟)
    return final_df
    


def wash_data(Df_all, algorithms_configs):
    if Df_all.empty == False:
        turbine_name = Df_all.iloc[0]['wtid']
        Df_all.drop('assetId',axis=1,inplace=True)
        Df_all.drop('wtid',axis=1,inplace=True)
        Df_all.drop('algorithm',axis=1,inplace=True)
        Df_all_m = Df_all.resample('10min',closed='left').apply({mymode,np.nanmean,np.nanmax,np.nanmin,np.nanstd}) #应填10min
        Df_all_m.insert(0,'wtid',turbine_name)  
        Df_all.insert(0,'wtid',turbine_name)  
        Df_all_m.loc[:,'localtime'] = Df_all_m.index
        Df_all.loc[:,'localtime'] = Df_all.index
    else:
        Df_all = pd.DataFrame()
        Df_all_m = pd.DataFrame()
    algorithms_configs['Df_all_all'] = pd.concat([algorithms_configs['Df_all_all'],Df_all])#.append(Df_all)#全场1min数据
    algorithms_configs['Df_all_m_all'] = pd.concat([algorithms_configs['Df_all_m_all'],Df_all_m])#.append(Df_all_m)#全10min场数据  

    

def define_parameters(algorithms_configs, algorithm_name):
    ##############################################################
    clear_rotspd = algorithms_configs['clear_rotspd']
    algorithms_configs['Df_all_all'].loc[:,algorithms_configs['Df_all_all'].dtypes=='float64'] = algorithms_configs['Df_all_all'].loc[:,algorithms_configs['Df_all_all'].dtypes=='float64'].astype('float32')
    algorithms_configs['Df_all_m_all'].loc[:,algorithms_configs['Df_all_m_all'].dtypes=='float64'] = algorithms_configs['Df_all_m_all'].loc[:,algorithms_configs['Df_all_m_all'].dtypes=='float64'].astype('float32')
    
    if((['rotspd'] in algorithms_configs['Df_all_all'].columns.values)==False)&(((['rotspdzz'] in algorithms_configs['Df_all_all'].columns.values)==True)):
        algorithms_configs['Df_all_all']['rotspd'] = algorithms_configs['Df_all_all']['rotspdzz']
        algorithms_configs['Df_all_m_all']['rotspd','nanmean'] = algorithms_configs['Df_all_m_all']['rotspdzz','nanmean']
        print('发电机转速不存在')
    elif((['rotspd'] in algorithms_configs['Df_all_all'].columns.values)==False)&(((['rotspdzz'] in algorithms_configs['Df_all_all'].columns.values)==False))&(((['rotspd_max'] in algorithms_configs['Df_all_all'].columns.values)==True)):
        algorithms_configs['Df_all_all']['rotspd'] = algorithms_configs['Df_all_all']['rotspd_max']
        algorithms_configs['Df_all_m_all']['rotspd','nanmean'] = algorithms_configs['Df_all_m_all']['rotspd_max','nanmean']
        print('发电机转速不存在')
    elif((['rotspd'] in algorithms_configs['Df_all_all'].columns.values)==False)&(((['rotspdzz'] in algorithms_configs['Df_all_all'].columns.values)==False))&(((['rotspd_max'] in algorithms_configs['Df_all_all'].columns.values)==False)):
        algorithms_configs['Df_all_all']['rotspd'] = 100
        algorithms_configs['Df_all_m_all']['rotspd','nanmean'] = 100
        print('发电机转速不存在')
    else:
        pass
    
    algorithms_configs['Df_all_all'] = algorithms_configs['Df_all_all'][(algorithms_configs['Df_all_all']['wspd']<50)&(algorithms_configs['Df_all_all']['wspd']>-0.5)] 
    algorithms_configs['Df_all_all'] = algorithms_configs['Df_all_all'][(algorithms_configs['Df_all_all']['pwrat']>-100)&(algorithms_configs['Df_all_all']['pwrat']<algorithms_configs['Pwrat_Rate']*1.2)]
    algorithms_configs['Df_all_m_all'] = algorithms_configs['Df_all_m_all'][(algorithms_configs['Df_all_m_all']['wspd','nanmean']<35)&(algorithms_configs['Df_all_m_all']['wspd','nanmean']>0)] 
    algorithms_configs['Df_all_m_all'] = algorithms_configs['Df_all_m_all'][(algorithms_configs['Df_all_m_all']['pwrat','nanmean']>-100)&(algorithms_configs['Df_all_m_all']['pwrat','nanmean']<algorithms_configs['Pwrat_Rate']*1.2)]
    #Df_all_m_all temp = Df_all_m_all[(Df_all_m_all['rotspd','nanmean']>-10)&(Df_all_m_all['rotspd','nanmean']<2500)]
    print('瞬态数据长度'+str(len(algorithms_configs['Df_all_all'])))
    print('10min数据长度'+str(len(algorithms_configs['Df_all_m_all'])))
    
    algorithms_configs['Df_all_m_all'].insert(0,'type',algorithms_configs['typeName'])  
    algorithms_configs['Df_all_all'].insert(0,'type',algorithms_configs['typeName'])
    #aaa = pd.Series(np.unique(Df_all_m_all['fault','mymode']))
    #aaa = aaa.dropna()
    
    algorithms_configs['Df_all_all_alltype'] = pd.concat([algorithms_configs['Df_all_all_alltype'],algorithms_configs['Df_all_all']])#.append(algorithms_configs['Df_all_all'])#全场1min数据
    algorithms_configs['Df_all_m_all_alltype'] = pd.concat([algorithms_configs['Df_all_m_all_alltype'],algorithms_configs['Df_all_m_all']])#.append(algorithms_configs['Df_all_m_all'])#全10min场数据
    algorithms_configs['Df_all_all_alltype'].loc[:,algorithms_configs['Df_all_all_alltype'].dtypes=='float64'] = algorithms_configs['Df_all_all_alltype'].loc[:,algorithms_configs['Df_all_all_alltype'].dtypes=='float64'].astype('float32')
    algorithms_configs['Df_all_m_all_alltype'].loc[:,algorithms_configs['Df_all_m_all_alltype'].dtypes=='float64'] = algorithms_configs['Df_all_m_all_alltype'].loc[:,algorithms_configs['Df_all_m_all_alltype'].dtypes=='float64'].astype('float32')
    '''
    ###########从本地读取历史数据
    os.chdir(r'D:\我的\项目\python_new')
    Df_all_m_all = pd.read_csv(str(os.getcwd()+'/Df_all_m_all_my85.csv'),header=[0,1],index_col=[0],dtype={'wtid':str})
    wtid = Df_all_m_all['wtid'].values
    Df_all_m_all.drop(columns=['wtid'],inplace=True)
    Df_all_m_all.insert(0,'wtid',wtid)
    Df_all_m_all.index = pd.DatetimeIndex(Df_all_m_all.index)
    
    Df_all_all = pd.read_csv(str(os.getcwd()+'/Df_all_all.csv'),header=[0],index_col=[0],dtype={'wtid':str})
    Df_all_all.index = pd.DatetimeIndex(Df_all_all.index)
    '''
    ##############关键参数确定
    algorithms_configs['wtids'] =  np.unique(algorithms_configs['Df_all_m_all'][('wtid')])
    for i in range(len(algorithms_configs['wtids'])):
        Df_all_m = algorithms_configs['Df_all_m_all'][algorithms_configs['Df_all_m_all']['wtid'] == algorithms_configs['wtids'][i]]
        algorithms_configs['turbine_param_all'].loc[i,'wtid'] = algorithms_configs['wtids'][i]
        
        if algorithms_configs['Pitch_Min'] != None and algorithms_configs[algorithm_name]['endTime']-algorithms_configs[algorithm_name]['startTime'] > timedelta(days=29):
            temp = Df_all_m[(Df_all_m['pwrat','nanmean']>10.0)&(Df_all_m['pwrat','nanmean']<algorithms_configs['Pwrat_Rate']*0.2)&(Df_all_m['statel','nanmean']==90002)&(Df_all_m['state','nanmean']==algorithms_configs['state'])&(Df_all_m['statety','nanmean']==71)]
            if len(temp)>0:
                Pitch_Min = round((np.mean(temp['pitch1','nanmean'].nsmallest(20)) + np.mean(temp['pitch1','nanmax'].nsmallest(20))) * 0.5,1)
            else:
                Pitch_Min = algorithms_configs['Pitch_Min']#0.0
            algorithms_configs['turbine_param_all'].loc[i,'Pitch_Min'] = Pitch_Min
        else:
            algorithms_configs['turbine_param_all'].loc[i,'Pitch_Min'] = algorithms_configs['Pitch_Min']

        if algorithms_configs['Rotspd_Rate'] != None and algorithms_configs[algorithm_name]['endTime']-algorithms_configs[algorithm_name]['startTime'] > timedelta(days=29):
            temp = Df_all_m[(Df_all_m['pwrat','nanmean']>algorithms_configs['Pwrat_Rate']*0.95)&(Df_all_m['pwrat','nanmean']<algorithms_configs['Pwrat_Rate']*1.1)&(Df_all_m[clear_rotspd,'nanmean']>1.1)&(Df_all_m['state','nanmean']==algorithms_configs['state'])&(Df_all_m['statety','nanmean']==71)]
            if len(temp)>0:
                Rotspd_Rate = round(np.nanmean(temp[clear_rotspd,'nanmean']),1)
            else:
                Rotspd_Rate = algorithms_configs['Rotspd_Rate']#np.nan
            algorithms_configs['turbine_param_all'].loc[i,'Rotspd_Rate'] = Rotspd_Rate
        else:
            algorithms_configs['turbine_param_all'].loc[i,'Rotspd_Rate'] = algorithms_configs['Rotspd_Rate']
        
        if algorithms_configs['Rotspd_Connect'] != None and algorithms_configs[algorithm_name]['endTime']-algorithms_configs[algorithm_name]['startTime'] > timedelta(days=29):
            temp = Df_all_m[(Df_all_m['pwrat','nanmean']>10.0)&(Df_all_m['pwrat','nanmean']<algorithms_configs['Pwrat_Rate']*0.08)&(Df_all_m['pitch1','mymode']<=(Pitch_Min+1.0))&(Df_all_m['state','nanmean']==algorithms_configs['state'])&(Df_all_m['statety','nanmean']==71)]
            if len(temp)>0:
                #Rotspd_Connect = round(np.mean(temp['rotspd','nanmean'].nlargest(50)),1)
                rotspd_q1 = temp[clear_rotspd,'nanmean'].quantile(0.6)##分位数
                rotspd_q2 = temp[clear_rotspd,'nanmean'].quantile(0.9)
                Rotspd_Connect = round(np.nanmean(temp[(temp[clear_rotspd,'nanmean']>=rotspd_q1)&(temp[clear_rotspd,'nanmean']<=rotspd_q2)][clear_rotspd,'nanmean']),1)
            else:
                Rotspd_Connect = algorithms_configs['Rotspd_Connect']
            algorithms_configs['turbine_param_all'].loc[i,'Rotspd_Connect'] = Rotspd_Connect
        else:
            algorithms_configs['turbine_param_all'].loc[i,'Rotspd_Connect'] = algorithms_configs['Rotspd_Connect']

    algorithms_configs['turbine_param_all']['Rotspd_Rate'].fillna(value=np.nanmean(algorithms_configs['turbine_param_all']['Rotspd_Rate']),inplace=True)
    algorithms_configs['turbine_param_all']['Rotspd_Connect'].fillna(value=np.nanmean(algorithms_configs['turbine_param_all']['Rotspd_Connect']),inplace=True)
        
    algorithms_configs['threshold'] = 3.0
    algorithms_configs['neighbors_num'] = 10






if __name__ == '__main__':
    # assetIds=['BYA2LVsH', 'xmYFZRiI', 'JGT06pg7', 'TzpeZh7e', '57Fed4kz', 'Unbt3ciP', 'fBzGDVYJ', 'nWsNJAdv', 'cSK9gHFh', '3DLQLrSX', 'vRirwair', '2BJhqqzr', 'CQlCBfh2', 'KGvXjY63', 'OUh7PkSh', 'ZuLQjI3u', 'hgpFittR', 'bXOMyiBc', 'hH5RrDsb', '1uW8O7am', 'ZeKO3BAs', 'i8Zkc5Cq', 'kuO9Oo9U', 'jAL17JML', 'anCgqvv5', 'IZhfhGS3', 'FcP4S2Gg', 'wl10lKQB', 'DgQDh9ay', '187RVbfL', 'x8RkQrZK', 'dUcQbdPf', 'D1uY3w78', 'eU70J6Qt', 'JhiFaiCG', 'UESzBt6l', 'ORFXFmGh', 'Ma4fi1Wa', 'YpyUGZbq', 'BsSUf9so', '8xJVOwOI', 'ADFXvze4', 'zWyJfsaZ', 'gSsqGg1Q', 'iTrYcuok', 'pXVKCt43', 'YMLyUQXH', 'TFKtTIox', 'rZ9tFVkp', '7FBhRoZz', 'OXo5VwP6', 'e29Rp0Vs', 'kXuVsrtX', 'bFc3bAwC', 'BXuLDLb7', 'eDuo2Fcm', 'IvoSmzdq', 'H4YKUKqN', 'sdbFengw', 'YYJpio3S', '50TyNWJ1', 'nElJZtqH', 'PXvtd6yq', 'rfWItMF7', 'AJu9oT46', 'oqPsh0pL', '08VyWN67', '9ITke9Lf', 'usq5e0LN', '7aeH3jeI', 'jZYgirEb', 'LekDIfJC', 'eIPfQedJ', 'heZAM65W', 'ghwm5IeY', 'UeS7Wirh', 'GeGBYtD4', 'G7RgSex2', 'ADueJxBq', 'oqXBQ020']
    # getData('2023-07-10 00:00:00', '2023-07-11 00:00:00', ['WNAC.TemNacelleCab'], assetIds)

    # , 'WROT.Blade1Position', 'WROT.Blade2Position', 'WROT.Blade3Position', 'WROT.CurBlade1Motor', 'WROT.CurBlade2Motor', 'WROT.CurBlade3Motor'
    getAiData('2023-07-23 12:00:00', '2023-07-24 00:00:00', '0ZfTpctM', ['WNAC.WindSpeed', 'WGEN.GenSpd', 'WGEN.GenActivePW', 'WROT.Blade1Position'], '10m')

    # getDiData('2023-06-01 00:00:00', '2023-08-11 14:10:00', 'WTUR.TurbineSts', '08VyWN67')

    '''
    import matplotlib.pyplot as plt
    plt.figure()
    plt.scatter(df_ai['WNAC.WindSpeed'], df_ai['WGEN.GenActivePW'], s=1)
    plt.xlabel('风速(m/s)',fontsize=14)
    plt.ylabel('功率(kW)',fontsize=14)
    plt.show()
    
    plt.figure()
    plt.scatter(df_ai['WGEN.GenSpd'],df_ai['WGEN.GenActivePW'], s=1)
    plt.show()
    
    plt.figure()
    plt.scatter(df_ai['WGEN.GenActivePW'],df_ai['WROT.Blade1Position'], s=1)
    plt.show()
    '''
