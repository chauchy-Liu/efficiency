# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 11:00:00 2023

@author: sunyan
"""

from poseidon import poseidon  # 打包时注意
import pandas as pd
from functools import partial  # 偏函数，用于map传递多个参数
from multiprocessing import Pool  # 计算密集型采用该并行
import numpy as np
from sklearn.neighbors import LocalOutlierFactor
from itertools import product
import utils.time_util as time_util
from datetime import timedelta
from configs import config
import logging
from configs.config import AccessKey, SecretKey, GW_Url, OrgId
from matplotlib import pyplot as plt
import importlib


Url_asset = GW_Url + '/cds-asset-service/v1.0/hierarchy?orgId=' + OrgId  # 这个api哪里来的？
Url_ai_normalized = GW_Url + '/tsdb-service/v2.1/ai-normalized?orgId=' + OrgId
Url_ai = GW_Url + '/tsdb-service/v2.1/ai?orgId=' + OrgId
Url_raw = GW_Url + '/tsdb-service/v2.1/raw?orgId=' + OrgId
Url_di = GW_Url + '/tsdb-service/v2.1/di?orgId=' + OrgId

pd.options.mode.use_inf_as_na = True

# global Pwrat_Rate  #额定功率
# global Rotspd_Connect #并网转速
# global Rotspd_Rate  #额定转速 
# global minPitch  #最小桨距角

def getWindTurbines(wind_farm):
    '''
    获取风场下的风机
    '''
    url_asset = Url_asset + '&mdmIds=' + wind_farm + \
        '&mdmTypes=EnOS_Wind_Turbine&attributes=mdmId,name,ratedPower'
    ResponsePoint = poseidon.urlopen(AccessKey, SecretKey, url_asset)
    if ResponsePoint['pagination']['pageSize'] > 0:
        wind_turbine_df = pd.DataFrame(
            ResponsePoint['data'][wind_farm]['mdmObjects']['EnOS_Wind_Turbine'][:])
        wind_turbine_df['name'] = list(
            map(lambda x: x['name'],  wind_turbine_df['attributes']))
        wind_turbine_df['ratedPower'] = list(
            map(lambda x: x['ratedPower'],  wind_turbine_df['attributes']))
        wind_turbine_df.drop('attributes', axis=1, inplace=True)
        return wind_turbine_df
    else:
        return pd.DataFrame()


def getRawData(startTime, endTime, points, assetIds):
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

    ResponsePoint = poseidon.urlopen(AccessKey, SecretKey, Url_raw, params)
    if len(ResponsePoint['data']['items']) > 0:
        DfTemp = pd.DataFrame(ResponsePoint['data']['items'])
    return DfTemp

def getGeneralData(startTime, endTime, algName, assetId: str, points, resample_interval):
    # print(startTime, endTime, points, assetId)
    # DfTemp = pd.DataFrame()
    # params = {"assetIds": assetId,
    #           "pointIds": points,
    #           "startTime": startTime,
    #           "endTime": endTime,
    #         #   "autoInterpolate": True,
    #           "itemFormat": "1",
    #           "pageSize":"20000",
    #           "type": "generic"}

    # ResponsePoint = poseidon.urlopen(AccessKey, SecretKey, Url_di, params)
    # if len(ResponsePoint['data']['items']) > 0:
    #     DfTemp = pd.DataFrame(ResponsePoint['data']['items'])
    # return DfTemp
    ResponsePoint = None
    # if time_util.use_raw_api(resample_interval):
    #     params = {"assetIds": assetId,
    #               "pointIds": ','.join(points),
    #               "startTime": startTime,
    #               "endTime": endTime,
    #               "itemFormat": "1",
    #               "pageSize": "20000",
    #               "boundaryType": 'inside'}
    #     ResponsePoint = poseidon.urlopen(AccessKey, SecretKey, Url_di, params)
    # else:
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
    ResponsePoint = poseidon.urlopen(AccessKey, SecretKey, Url_raw, params)
    DfTemp = pd.DataFrame()
    if len(ResponsePoint['data']['items']) > 0:
        DfTemp = pd.DataFrame(ResponsePoint['data']['items'])
        DfTemp.set_index('localtime', inplace=True)
        DfTemp.index = pd.to_datetime(DfTemp.index)
        #填充替换NaN
        DfTemp = DfTemp.ffill()
        DfTemp = DfTemp.bfill()
        DfTemp = DfTemp.fillna(0)
        if time_util.split_time_delta(resample_interval)[0] > 1:
            resample_interval = time_util.replace_to_resample(
                resample_interval)
            #实际取得的测点和自定义的可能不一致会报错
            common_columns = DfTemp.columns.intersection(points).tolist()
            DfTemp = DfTemp[common_columns].resample(
                resample_interval, closed='left').mean().ffill()#  # FIXME
        DfTemp = DfTemp[common_columns].astype(int)
        DfTemp['assetId'] = assetId
        #重命名
    return DfTemp

def getDiData(startTime, endTime, algName, assetId: str, points, resample_interval):
    # print(startTime, endTime, points, assetId)
    # DfTemp = pd.DataFrame()
    # params = {"assetIds": assetId,
    #           "pointIds": points,
    #           "startTime": startTime,
    #           "endTime": endTime,
    #           "autoInterpolate": True,
    #           "itemFormat": "1",
    #           "pageSize":"20000",
    #           "type":"di"}

    # ResponsePoint = poseidon.urlopen(AccessKey, SecretKey, Url_di, params)
    # if len(ResponsePoint['data']['items']) > 0:
    #     DfTemp = pd.DataFrame(ResponsePoint['data']['items'])
    #     #重命名
    #     if 'WTUR.TurbineSts_Map' in DfTemp.columns.tolist():
    #         DfTemp.rename(columns={'WTUR.TurbineSts_Map':'WWTUR.TurbineSts'}, inplace=True)
    # return DfTemp
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
    ResponsePoint = poseidon.urlopen(AccessKey, SecretKey, Url_di, params)
    DfTemp = pd.DataFrame()
    if len(ResponsePoint['data']['items']) > 0:
        DfTemp = pd.DataFrame(ResponsePoint['data']['items'])
        DfTemp.set_index('localtime', inplace=True)
        DfTemp.index = pd.to_datetime(DfTemp.index)
        #填充替换NaN
        DfTemp = DfTemp.ffill()
        DfTemp = DfTemp.bfill()
        DfTemp = DfTemp.fillna(0)
        if time_util.split_time_delta(resample_interval)[0] > 1:
            resample_interval = time_util.replace_to_resample(
                resample_interval)
            #实际取得的测点和自定义的可能不一致会报错
            common_columns = DfTemp.columns.intersection(points).tolist()
            DfTemp = DfTemp[common_columns].resample(
                resample_interval, closed='left').mean().ffill()#  # FIXME
        DfTemp = DfTemp[common_columns].astype(int)
        DfTemp['assetId'] = assetId
        #重命名
        if 'WTUR.TurbineSts' not in DfTemp.columns.tolist():
            DfTemp.rename(columns={'WTUR.TurbineSts_Map':'WTUR.TurbineSts'}, inplace=True)
    return DfTemp


def getAiData(startTime, endTime, algName, assetId: str, points, resample_interval):
    # print(startTime, endTime, assetId, points, resample_interval)
    ResponsePoint = None
    if time_util.use_raw_api(resample_interval):
        params = {"assetIds": assetId,
                  "pointIds": ','.join(points),
                  "startTime": startTime,
                  "endTime": endTime,
                  "itemFormat": "1",
                  "pageSize": "20000",
                  "boundaryType": 'inside'}
        ResponsePoint = poseidon.urlopen(AccessKey, SecretKey, Url_ai, params)
    else:
        params = {"assetIds": assetId,
                  "pointIdsWithLogic": ','.join(points),
                  "startTime": startTime,
                  "endTime": endTime,
                  "interval": "0",
                  "itemFormat": "1",
                  "pageSize": "20000"}
        ResponsePoint = poseidon.urlopen(
            AccessKey, SecretKey, Url_ai_normalized, params)
    DfTemp = pd.DataFrame()
    if len(ResponsePoint['data']['items']) > 0:
        DfTemp = pd.DataFrame(ResponsePoint['data']['items'])
        DfTemp.set_index('localtime', inplace=True)
        DfTemp.index = pd.to_datetime(DfTemp.index)
        #填充替换NaN
        DfTemp = DfTemp.ffill()
        DfTemp = DfTemp.bfill()
        DfTemp = DfTemp.fillna(0)
        value, unit = time_util.split_time_delta(resample_interval)
        if pd.Timedelta(str(value)+unit) > pd.Timedelta('1min'):
            resample_interval = time_util.replace_to_resample(
                resample_interval)
            #实际取得的测点和自定义的可能不一致会报错
            common_columns = DfTemp.columns.intersection(points).tolist()
            DfTemp = DfTemp[common_columns].resample(
                resample_interval, closed='left').mean()  # FIXME
        DfTemp['assetId'] = assetId
        #重命名
        name = importlib.import_module('.' + algName, package='algorithms')
        if len(name.ai_rename) != 0:
            for key, evalue in name.ai_rename.items():
                if key in DfTemp.columns.tolist():
                    DfTemp.rename(columns={key:evalue}, inplace=True)
                    if key in name.ai_points:
                        index_key = name.ai_points.index(key)
                        name.ai_points[index_key] = evalue
        if 'WGEN.GenSpdInstant' in DfTemp.columns.tolist():
            DfTemp.rename(columns={'WGEN.GenSpdInstant':'WGEN.GenSpd'}, inplace=True)
        DfTemp = DfTemp.round(2)
    return DfTemp

def TimeDeviceSlice(startTime, endTime, algName, assetIds, ai_points, resample_interval, getData):
    # 按时间和设备分片
    date_range = []
    if endTime - startTime > timedelta(hours=12):
        date_range = pd.date_range(startTime, endTime, freq="12h").strftime(
            '%Y-%m-%d %H:%M:%S').to_list()
    else:
        date_range = [startTime.strftime('%Y-%m-%d %H:%M:%S'), endTime.strftime('%Y-%m-%d %H:%M:%S')]
    time_param = [(date_range[i], date_range[i + 1])
                  for i in range(len(date_range) - 1)]
    # 加上设备（笛卡尔积）
    time_asset_param = product(time_param, assetIds)
    final_time_asset_param = [(item[0][0], item[0][1], algName, item[1])
                              for item in time_asset_param]
    getAiDataWithTimeFunc = partial(getData, points=ai_points, resample_interval=resample_interval)
    # 获取ai数据
    pool = Pool()
    map_result = pool.starmap_async(
        getAiDataWithTimeFunc, final_time_asset_param)
    pool.close()
    pool.join()

    df = pd.concat(map_result.get())
    return df

def getDataForMultiAlgorithms(startTime, endTime, assetIds: list, algorithms):
    ai_points = list(set([ai_point for algorithm in algorithms for ai_points in algorithm.ai_points for ai_point in ai_points]))
    di_points = list(set([di_point for algorithm in algorithms for di_points in algorithm.di_points for di_point in di_points]))
    general_points = list(set([general_point for algorithm in algorithms for general_points in algorithm.general_points for general_point in di_points]))
    resample_interval = algorithms[0].resample_interval
    
    # 按时间和设备分片提取数据
    #ai数据
    df_ai = TimeDeviceSlice(startTime, endTime, assetIds, ai_points, resample_interval, getAiData)

    #general数据
    if len(general_points) > 0:
        df_general = TimeDeviceSlice(startTime, endTime, assetIds, general_points, resample_interval, getGeneralData)
        #剔除重名字段
        common_columns = df_ai.columns.intersection(df_general.columns)
        for tag in list(common_columns):
            del df_general[tag]
        df_ai = df_ai.join(df_general, how='outer')

    #di数据
    if len(di_points) > 0:
        df_di = TimeDeviceSlice(startTime, endTime, assetIds, di_points, resample_interval, getDiData)
        #剔除重名字段
        common_columns = df_ai.columns.intersection(df_di.columns)
        for tag in list(common_columns):
            del df_di[tag]
        df_ai = df_ai.join(df_di, how='outer')
    
    return df_ai

    # if df_ai.shape[0] > 0 and len(di_points) > 0 and len(general_points) > 0:
    #     final_df = pd.DataFrame()
    #     for assetId in assetIds:
    #         df_current_assetId = df_ai[df_ai['assetId'] == assetId].copy()
    #         df_current_general_assetId = df_general[df_general['assetId'] == assetId].copy()
    #         if 'timestamp' in list(df_current_general_assetId.columns):
    #             df_current_general_assetId.drop('timestamp', axis=1, inplace=True)
    #         df_current_assetId = df_current_assetId.join(df_current_general_assetId, how='outer')

    #         getDiDataWithTimeFunc = partial(getDiData, assetId=assetId, points=','.join(di_points))
    #         df_di = getDiDataWithTimeFunc(startTime=startTime.strftime('%Y-%m-%d %H:%M:%S'), endTime=endTime.strftime('%Y-%m-%d %H:%M:%S'))
    #         df_di.set_index('localtime', inplace=True)
    #         df_di.index = pd.to_datetime(df_di.index)
    #         df_di.drop('assetId', axis=1, inplace=True)
    #         df_di.drop('timestamp', axis=1, inplace=True)
    #         #------------------------------------------------
    #         df_current_assetId = df_current_assetId.join(df_di, how='outer')
    #         allow_points = list(set(df_current_assetId.columns) & set(di_points))
    #         df_current_assetId[allow_points] = df_current_assetId[allow_points].ffill()#fillna(method='ffill')
    #         final_df = pd.concat([final_df, df_current_assetId]) 
    #     # final_df.dropna(inplace=True)
    #     return final_df
    # else:
    #     return df_ai


def getDataCommon(startTime, endTime, algName, assetIds, ai_points, di_points, general_points, resample_interval):
    # # 按时间和设备分片
    # date_range = []
    # if endTime - startTime > timedelta(hours=12):
    #     date_range = pd.date_range(startTime, endTime, freq="12h").strftime(
    #         '%Y-%m-%d %H:%M:%S').to_list()
    # else:
    #     date_range = [startTime.strftime('%Y-%m-%d %H:%M:%S'), endTime.strftime('%Y-%m-%d %H:%M:%S')]
    # time_param = [(date_range[i], date_range[i + 1])
    #               for i in range(len(date_range) - 1)]
    # # 加上设备（笛卡尔积）
    # time_asset_param = product(time_param, assetIds)
    # final_time_asset_param = [(item[0][0], item[0][1], item[1])
    #                           for item in time_asset_param]
    # getAiDataWithTimeFunc = partial(
    #     getAiData, points=ai_points, resample_interval=resample_interval)
    # # 获取ai数据
    # pool = Pool()
    # map_result = pool.starmap_async(
    #     getAiDataWithTimeFunc, final_time_asset_param)
    # pool.close()
    # pool.join()

    # df_ai = pd.concat(map_result.get())
    # 按时间和设备分片提取数据
    #ai数据
    df_ai = TimeDeviceSlice(startTime, endTime, algName, assetIds, ai_points, resample_interval, getAiData)
    #general数据
    if len(general_points) > 0:
        df_general = TimeDeviceSlice(startTime, endTime, algName, assetIds, general_points, resample_interval, getGeneralData)
        #剔除重名字段
        common_columns = df_ai.columns.intersection(df_general.columns)
        for tag in list(common_columns):
            del df_general[tag]
        df_ai = df_ai.join(df_general, how='outer')
    #di数据
    if len(di_points) > 0:
        df_di = TimeDeviceSlice(startTime, endTime, algName, assetIds, di_points, resample_interval, getDiData)
        #剔除重名字段
        common_columns = df_ai.columns.intersection(df_di.columns)
        for tag in list(common_columns):
            del df_di[tag]
        df_ai = df_ai.join(df_di, how='outer')
    
    return df_ai
    
    # if df_ai.shape[0] > 0 and len(di_points) > 0 and len(general_points) > 0:
    #     final_df = pd.DataFrame()
    #     for assetId in assetIds:
    #         df_current_assetId = df_ai[df_ai['assetId'] == assetId].copy()
    #         getDiDataWithTimeFunc = partial(getDiData, assetId=assetId, points=','.join(di_points))
    #         df_current_general_assetId = df_general[df_general['assetId'] == assetId].copy()
    #         if 'timestamp' in list(df_current_general_assetId.columns):
    #             df_current_general_assetId.drop('timestamp', axis=1, inplace=True)
    #         if 'assetId' in list(df_current_general_assetId.columns):
    #             df_current_general_assetId.drop('assetId', axis=1, inplace=True)
    #         df_current_assetId = df_current_assetId.join(df_current_general_assetId, how='outer')

    #         df_di = getDiDataWithTimeFunc(startTime=startTime.strftime('%Y-%m-%d %H:%M:%S'), endTime=endTime.strftime('%Y-%m-%d %H:%M:%S'))
    #         df_di.set_index('localtime', inplace=True)
    #         df_di.index = pd.to_datetime(df_di.index)
    #         df_di.drop('assetId', axis=1, inplace=True)
    #         df_di.drop('timestamp', axis=1, inplace=True)
    #         #------------------------------------------------
    #         df_current_assetId = df_current_assetId.join(df_di, how='outer')
    #         allow_points = list(set(df_current_assetId.columns) & set(di_points))
    #         df_current_assetId[allow_points] = df_current_assetId[allow_points].ffill()#fillna(method='ffill')
    #         final_df = pd.concat([final_df, df_current_assetId])
    #     # final_df.dropna(inplace=True)
    #     return final_df
    # else:
    #     return df_ai


def getData(startTime, endTime, assetIds, algorithm):
    return getDataCommon(startTime, endTime, assetIds, algorithm.ai_points, algorithm.di_points, algorithm.general_points, algorithm.resample_interval)


def thresholdfun_pwrat_out(raw_df,neighbors_num,clear):
    '''
    根据数学统计剔数-功率
    '''
    temp_all = raw_df[raw_df['clear'] != clear ]
    temp_clear = raw_df[raw_df['clear'] == clear ]
    X_train = pd.DataFrame()
    X_train['wspd'] = temp_clear['WNAC.WindSpeed']
    X_train['pwrat'] = temp_clear['WGEN.GenActivePW']    
    clf = LocalOutlierFactor(n_neighbors=neighbors_num,p=1,n_jobs=-1)
    #clf = IsolationForest(n_estimators=50,contamination=0.25,random_state=1)
    y_pred = clf.fit_predict(X_train)
    temp_clear['y_pred'] = y_pred
    temp_clear.loc[temp_clear['y_pred']==1,'clear'] = clear-1
    temp_all = pd.concat([temp_all, temp_clear])
    return temp_all

def thresholdfun_pwrat(df_ai, threshold, clear):
    '''
    根据数学统计剔数-功率
    '''
    # temp_all = pd.DataFrame()
    # wind_bin = np.arange(2.0,np.ceil(np.nanmax(df_ai['WNAC.WindSpeed'])),0.5)
    # for m in range(len(wind_bin)):
    #     temp = df_ai[(df_ai['WNAC.WindSpeed']>=wind_bin[m]-0.25) & (df_ai['WNAC.WindSpeed']<wind_bin[m]+0.25)]
    #     pwrat_mean = np.mean(temp['WGEN.GenActivePW'])
    #     pwrat_std = np.std(temp['WGEN.GenActivePW'])
    #     temp = temp[(np.abs(temp['WGEN.GenActivePW']-pwrat_mean)/pwrat_std < threshold)]
    #     temp_all = pd.concat([temp_all, temp])
    # return temp_all
    temp_all = df_ai[df_ai['clear'] != clear ]    
    wind_bin = np.arange(2.0,np.ceil(np.nanmax(df_ai['WNAC.WindSpeed'])),0.5)
    for m in range(len(wind_bin)):
        temp = df_ai[(df_ai['WNAC.WindSpeed']>=wind_bin[m]-0.25) & (df_ai['WNAC.WindSpeed']<wind_bin[m]+0.25)&(df_ai['clear']==clear)]
        pwrat_mean = np.nanmean(temp['WGEN.GenActivePW'])
        pwrat_std = np.nanstd(temp['WGEN.GenActivePW'])
        temp.loc[((temp['WGEN.GenActivePW']-pwrat_mean)/pwrat_std < threshold) & ((temp['WGEN.GenActivePW']-pwrat_mean)/pwrat_std > -threshold),'clear'] = clear-1
        temp_all = pd.concat([temp_all, temp])
    return temp_all


def thresholdfun_pitch(df_ai, threshold, clear):
    '''
    根据数学统计剔数-桨距角
    '''
    # temp_all = pd.DataFrame()
    # wind_bin = np.arange(2.0,np.ceil(np.nanmax(df_ai['WNAC.WindSpeed'])),0.5)
    # for m in range(len(wind_bin)):
    #     temp = df_ai[(df_ai['WNAC.WindSpeed']>=wind_bin[m]-0.25) & (df_ai['WNAC.WindSpeed']<wind_bin[m]+0.25)]
    #     pitch_mean = np.mean(temp['WROT.Blade1Position'])
    #     pitch_std = np.std(temp['WROT.Blade1Position'])
    #     temp = temp[(np.abs(temp['WROT.Blade1Position']-pitch_mean)/pitch_std < threshold)]
    #     temp_all = pd.concat([temp_all, temp])
    # return temp_all
    temp_all = df_ai[df_ai['clear'] != clear ]    
    wind_bin = np.arange(2.0,np.ceil(np.nanmax(df_ai['WNAC.WindSpeed'])),0.5)
    for m in range(len(wind_bin)):
        temp = df_ai[(df_ai['WNAC.WindSpeed']>=wind_bin[m]-0.25) & (df_ai['WNAC.WindSpeed']<wind_bin[m]+0.25)&(df_ai['clear']==clear)]
        pitch_mean = np.mean(temp['WROT.Blade1Position'])
        pitch_std = np.std(temp['WROT.Blade1Position'])
        temp.loc[((temp['WROT.Blade1Position']-pitch_mean)/pitch_std < threshold) & ((temp['WROT.Blade1Position']-pitch_mean)/pitch_std > -threshold),'clear'] = clear-1
        temp_all = pd.concat([temp_all, temp])
    return temp_all


def thresholdfun_rotspd(df_ai, neighbors_num, clear):
    '''
    根据算法模型剔数 功率-桨距角 剔除数据
    '''
    # df_ai.dropna(subset=['WGEN.GenActivePW','WROT.Blade1Position'], inplace=True)
    # X_train = df_ai[[PW','WROT.Blade1Position']]
    # clf = LocalOutlierFactor(n_neighbors=neighbors_num,p=1,n_jobs=-1)
    # y_pred = clf.fit_predict(X_train)
    # df_ai['y_pred'] = y_pred
    # final_df = df_ai[df_ai['y_pred']==1]
    # return final_df
    temp_all = df_ai[(df_ai['clear'] != clear)]
    temp_clear = df_ai[(df_ai['clear'] == clear)]
    #temp_clear = temp_clear.dropna(axis=0,subset=[('pwrat','nanmean')],inplace=True)
    #temp_clear = temp_clear.dropna(axis=0,subset=[('pitch1','nanmean')],inplace=True)
    X_train = pd.DataFrame()
    X_train['pwrat'] = temp_clear['WGEN.GenActivePW']
    X_train['pitch'] = temp_clear['WROT.Blade1Position']    
    clf = LocalOutlierFactor(n_neighbors=neighbors_num,p=1,n_jobs=-1)
    #clf = IsolationForest(n_estimators=50,contamination=0.25,random_state=1)
    y_pred = clf.fit_predict(X_train)
    temp_clear['y_pred'] = y_pred
    temp_clear.loc[temp_clear['y_pred']==1,'clear'] = clear-1
    temp_all = pd.concat([temp_all, temp_clear])
    return temp_all


def wash_data_mechanization_new(df_ai, ratedPower):
    '''
    机理清数
    '''
    # turbine_name = df_ai.iloc[0]["assetId"]
    # fig = plt.figure(figsize=(10,8),dpi=100)  
    # plt.title(str(turbine_name)) 
    # with plt.style.context('ggplot'):  
    #     # plt.scatter(df_ai['WNAC.WindSpeed'],df_ai['WGEN.GenActivePW'],c=df_ai['WTUR.TurbineSts'],cmap='jet',s=15)
    #     plt.scatter(df_ai['WGEN.GenSpd'],df_ai['WGEN.GenActivePW'],s=15)
    #     plt.grid()
    #     #plt.ylim(-3,25)
    #     plt.xlabel('风速(wspd)',fontsize=14)
    #     plt.ylabel('功率(kW)',fontsize=14)
    #     plt.colorbar()
    # fig.savefig(str(turbine_name) + '_torqueerr_风速_功率.png',dpi=100)

    global Pwrat_Rate
    global Rotspd_Connect
    global Rotspd_Rate
    global minPitch

    # 根据状态码分组，正常组的数据最多，计算众数
    aiGroupedStates = df_ai.groupby('WTUR.TurbineSts').count()['WNAC.WindSpeed']
    normalCode = aiGroupedStates.idxmax()

    # 最小桨距角
    aiGroupedPitch = df_ai.groupby('WROT.Blade1Position').count()['WNAC.WindSpeed']
    minPitch = aiGroupedPitch.idxmax()

    # 转速（并网、额定）
    #生成bin,bin size 10
    # bins = np.arange(df_ai['WGEN.GenSpd'].min(), df_ai['WGEN.GenSpd'].max(),10)
    # #分组
    # grouped = pd.cut(df_ai['WGEN.GenSpd'], bins)
    # df_ai['rateSpeedGroup'] = grouped
    # aiGroupedRate = df_ai.groupby('rateSpeedGroup').count()['WGEN.GenActivePW']
    # if aiGroupedRate.nlargest(5).shape[0] >= 2:
    #     connectRateSpeed = aiGroupedRate.nlargest(5).index[1].left#aiGroupedPitch.iloc[:len(aiGroupedPitch)//4].idxmax()
    # elif aiGroupedRate.nlargest(5).shape[0] == 1:
    #     connectRateSpeed = aiGroupedRate.nlargest(5).index[0].left#aiGroupedPitch.iloc[:len(aiGroupedPitch)//4].idxmax()
    # else:
    #     connectRateSpeed = 0
    # RateSpeed = aiGroupedRate.nlargest(5).index[0].left#aiGroupedPitch.iloc[len(aiGroupedPitch)*3//4:].idxmax()

    Pwrat_Rate = ratedPower
    # if connectRateSpeed == 0:
        # connectRateSpeed = config.Rotspd_Connect
    Rotspd_Connect = config.Rotspd_Connect #connectRateSpeed
    # if RateSpeed == 0:
        # RateSpeed = config.Rotspd_Rate
    Rotspd_Rate = config.Rotspd_Rate #RateSpeed

    df_ai = df_ai.dropna(axis=0,thresh=len(df_ai.columns)*0.8)
    df_ai['clear'] = 10
    df_ai['pitchlim'] = (df_ai['WGEN.GenActivePW'] - 0.8*ratedPower)*(5.0-2.5-minPitch) / (ratedPower*(0.95-0.8)) + (minPitch+2.5)

    #风机发电状态
    df_ai.loc[(df_ai['WTUR.TurbineSts']==normalCode),'clear'] = 9

    #风机转速、功率正常阈值内
    df_ai.loc[(df_ai['WGEN.GenSpd']>Rotspd_Connect*0.9)&(df_ai['WGEN.GenSpd']<Rotspd_Rate*1.1)&(df_ai['WGEN.GenActivePW']>0)&(df_ai['WGEN.GenActivePW']<ratedPower*1.1)&(df_ai['clear']==9),'clear'] = 8

    #非限功率状态
    df_ai.loc[(df_ai['WTUR.TurbineAIStatus'].isin([90002]))|(df_ai['clear']==8),'clear'] = 7

    Pitch_Min_temp = np.nanmean(df_ai[(df_ai['clear']==7)&(df_ai['WGEN.GenActivePW']>100)&(df_ai['WGEN.GenActivePW']<ratedPower*0.6)]['WROT.Blade1Position'])
    if np.abs(minPitch - Pitch_Min_temp)>1.0:
        minPitch = Pitch_Min_temp
    
    #无超过正常范围的变桨
    df_ai.loc[((df_ai['WGEN.GenActivePW']>=ratedPower*0.95)|
              ((df_ai['WGEN.GenActivePW']<ratedPower*0.8)&(df_ai['WROT.Blade1Position']<minPitch+2.5))|
                  ((df_ai['WGEN.GenActivePW']<ratedPower*0.95)&(df_ai['WGEN.GenActivePW']>ratedPower*0.8)&(df_ai['WROT.Blade1Position']<df_ai['pitchlim'])))&(df_ai['clear']==7),'clear'] = 6
    
    return df_ai 

def wash_data_mechanization(df_ai, ratedPower):
    '''
    机理清数
    '''
    #风速功率状态码分布图
    # turbine_name = df_ai.iloc[0]["assetId"]
    # fig = plt.figure(figsize=(10,8),dpi=100)  
    # plt.title(str(turbine_name)) 
    # with plt.style.context('ggplot'):  
    #     plt.scatter(df_ai['WNAC.WindSpeed'],df_ai['WGEN.GenActivePW'],c=df_ai['WTUR.TurbineSts'],cmap='jet',s=15)
    #     plt.grid()
    #     #plt.ylim(-3,25)
    #     plt.xlabel('风速(wspd)',fontsize=14)
    #     plt.ylabel('功率(kW)',fontsize=14)
    #     plt.colorbar()
    # fig.savefig(str(turbine_name) + '_torqueerr_风速_功率.png',dpi=100)

    # 根据状态码分组，正常组的数据最多，计算众数
    aiGroupedStates = df_ai.groupby('WTUR.TurbineSts').count()['WNAC.WindSpeed']
    normalCode = aiGroupedStates.idxmax()
    # aiGroupedAIStates = df_ai.groupby('WTUR.TurbineAIStatus').count()['WNAC.WindSpeed']
    # limitCode = aiGroupedAIStates.idxmax()

    # 根据风机状态码清洗数据
    df_ai = df_ai[(~df_ai['WTUR.TurbineAIStatus'].isin([90002, 90001])) & (df_ai['WTUR.TurbineSts'] == normalCode)]

    # 根据机理清洗数据
    df_ai['pitchlim'] = (df_ai['WGEN.GenActivePW']-0.72 *
                         ratedPower) * (5.0-2.5) / (ratedPower*(0.98-0.72)) + 2.5
    
    df_ai = df_ai[(df_ai['WGEN.GenSpd'] > config.Rotspd_Connect * 0.9) &
                  (df_ai['WGEN.GenActivePW'] > 0) & 
                  (df_ai['WGEN.GenActivePW'] < ratedPower*1.1)]
    
    # 清洗开始变桨之前的数据
    df_ai = df_ai[(df_ai['WGEN.GenActivePW'] >= ratedPower*0.72) | 
                  ((df_ai['WGEN.GenActivePW'] < ratedPower*0.72) & 
                   (df_ai['WROT.Blade1Position'] < config.Pitch_Min+2.5))]
    
    # 清洗提前变桨阶段的数据
    df_ai = df_ai[(df_ai['WGEN.GenActivePW'] >= ratedPower*0.98) |
                  (df_ai['WGEN.GenActivePW'] <= ratedPower*0.72) | 
                  ((df_ai['WGEN.GenActivePW'] < ratedPower*0.98) & 
                   (df_ai['WGEN.GenActivePW'] > ratedPower*0.72) & 
                   (df_ai['WROT.Blade1Position'] < df_ai['pitchlim']))]
    return df_ai


def wash_data_for_train(df_ai, ratedPower):
    '''
    数据清洗（拟合剔数）
    Parameters
    ----------
    df_ai : TYPE
        DESCRIPTION.
    ratedPower : TYPE
        额定功率.
    Returns
    -------
    None.

    '''
    df_ai = wash_data_mechanization_new(df_ai, ratedPower)
    if df_ai.empty == True:
        return df_ai
    # 数学统计清洗数据
    threshold = 3
    neighbors_num = 20
    df_ai = thresholdfun_rotspd(df_ai, neighbors_num,6)
    df_ai = thresholdfun_pitch(df_ai, threshold, 5)    
    df_ai = thresholdfun_pwrat(df_ai, threshold, 4)
    df_ai = thresholdfun_pwrat_out(df_ai,neighbors_num,3)

    # df_ai = df_ai[df_ai['clear'] == 2]#1为干净值

    return df_ai




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
