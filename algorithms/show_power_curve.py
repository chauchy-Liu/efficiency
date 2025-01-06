from pandas import DataFrame
# from alarms import alarm
import numpy as np
from utils.display_util import DisplayResultXY, DisplayFigures
import pandas as pd
import utils.time_util as time_util
import asyncio
from datetime import datetime as datetime
from configs.config import algConfig
import data.efficiency_function as turbine_efficiency_function
from db.db import selectPwTurbineAll,selectTheoryWindPower, insertWindDirectionPicture, upload, insertPwTurbineAll, insertWindFrequencyPicture, insertAirDensityPicture,insertTurbulencePicture,insertNavigationBiasDirectionPicture,insertNavigationBiasControlPicture,insertPitchAnglePicture,insertPitchActionPicture,insertTorqueControlPicture
from matplotlib import pyplot as plt
from pylab import mpl
import sys
import statistics as st
from scipy import signal,integrate
from datetime import datetime
# from configs.config import wspd, pwrat
# from faultcode.faultcode_MINYANG_GANSU_QINGSHUI import wspd, pwrat

def analyse(farmName, typeName:str, wtid:list, startTime, endTime):
    #赋值
    windbin = np.arange(2.0,25.0,0.5)
    pw_startTime = startTime
    pw_endTime = endTime
    turbine_type = typeName#np.unique(Turbine_attr['turbineTypeID'])[i_type]
    pw_turbine_all, turbine_list = selectPwTurbineAll(pd.DataFrame(), farmName, typeName, datetime.strptime(startTime, "%Y-%m-%d"), datetime.strptime(endTime, "%Y-%m-%d"))
    # turbine_list = #Turbine_attr_type.loc[0:5,'name']
    # turbine_list = turbine_list.reset_index(drop = True)
    pw_df_all = pd.DataFrame()
    pw_df_all['windbin'] = windbin
    
    wspd, pwrat = selectTheoryWindPower(farmName, typeName)
    pwrat_standard = pd.DataFrame({'windbin':wspd,'pwrat':pwrat})
    
    powerCurve = {
        'legend': {
            'textStyle':{
                'color':'#FFF'
            },
            'type': 'scroll',
            'pageIconColor': '#FFF',
            'pageIconInactiveColor': '#FFF',
            'pageTextStyle':{
                'color':'#FFF'
            },
            'data': []
        },
        'grid': {
            'left': '20',
            'right': '61',
            'bottom': '10',
            'containLabel': 'true'
        },
        'xAxis':{
            'data': [],
            'type': 'category',
            'name': '风速m/s'
        },
        'yAxis': {
            'type': 'value',
            'name': '功率kw'
        },
        'series': []
    }
    #######机组功率曲线计算,用wtid替换turbine_list
    for num in range(len(wtid)):
        turbine_name = wtid[num]
        pw_df = turbine_efficiency_function.pwratcurve_rho(pw_turbine_all,turbine_type,turbine_name,pw_startTime,pw_endTime,windbin,6)
        pw_df = pw_df.loc[:,['windbin','pwrat','count']]
        pw_df.rename(columns = {'pwrat':turbine_name,'count':str(turbine_name+'_'+'count')},inplace = True) 
        pw_df_all = pd.merge(pw_df_all,pw_df,how='outer',on='windbin')
        
    
    #####某机型实测功率曲线及理论功率曲线
    pw_df_all = pw_df_all.dropna(axis=0,how='all',subset=pw_df_all.columns[1:],inplace=False)
    pw_df_all = pd.merge(pw_df_all,pwrat_standard,how='outer',on='windbin')
    #########绘制功率曲线 ,输出内容：功率一致性分析：功率曲线
    powerCurve['xAxis']['data'] = pw_df_all['windbin'].to_list()
    elem = {}
    elem['name'] = 'theoryPower' 
    powerCurve['legend']['data'].append('theoryPower')
    elem['type'] = 'line'
    elem['data']= pw_df_all['pwrat'].to_list()
    powerCurve['series'].append(elem)
    #输入风机控制输出风机
    for turbine_name in wtid: #turbine_list:
        elem = {}
        elem['name'] = turbine_name
        elem['data']= pw_df_all[turbine_name].to_list()
        elem['type'] = 'line'
        powerCurve['legend']['data'].append(turbine_name)
        powerCurve['series'].append(elem)
    #处理非数数据
    # for i in range(len(powerCurve['xAxis']['xData'])):
    #     if powerCurve['xAxis']['xData'][i] == np.nan or str(powerCurve['xAxis']['xData'][i]) == 'nan':
    #         powerCurve['xAxis']['xData'][i] = None
    #     if powerCurve['yAxis']['theoryPower'][i] == np.nan or str(powerCurve['yAxis']['theoryPower'][i]) == 'nan':
    #         powerCurve['yAxis']['theoryPower'][i] = None
    #     #输入风机控制输出风机
    #     for turbine_name in wtid: #turbine_list:
    #         if powerCurve['yAxis'][turbine_name][i] == np.nan or str(powerCurve['yAxis'][turbine_name][i]) == 'nan':
    #             powerCurve['yAxis'][turbine_name][i] = None
    #借助map处理非数数据
    powerCurve['xAxis']['data'] = list(map(lambda x: None if x==np.nan or str(x)=='nan' else x, powerCurve['xAxis']['data']))
    # powerCurve['yAxis']['theoryPower'] = list(map(lambda x: None if x==np.nan or str(x)=='nan' else x, powerCurve['yAxis']['theoryPower']))
    #输入风机控制输出风机
    for elem in powerCurve['series']: #turbine_list:
        elem['data'] = list(map(lambda x: None if x==np.nan or str(x)=='nan' else x, elem['data']))
    # resList = [list(map(lambda x: None if x==np.nan or str(x)=='nan' else x, z)) for z in list(map(lambda y: powerCurve['yAxis'][y], wtid))]

    # turbine_num = 16
    # pnum = len(turbine_list)//turbine_num
    # rem = len(turbine_list)%turbine_num
    # if pnum > 0: 
    #     if rem > 0:
    #         pnum_new = pnum + 1
    #     else:
    #         pnum_new = pnum
    #     rem_pw = turbine_num
    #     ############################################
    #     for j_pw in range(pnum_new):
    #         powerCurve['yAxis']['理论功率'] = pw_df_all['pwrat'].to_list()
    #         if j_pw >= pnum:
    #             rem_pw = rem
    #         for i in range(rem_pw):
    #             # plt.plot(pw_df_all['windbin'],pw_df_all[turbine_list[i+j_pw*turbine_num]],'-o',color=camp20(i),markersize=5,label=turbine_list[i+j_pw*turbine_num])
    #             powerCurve['yAxis'][turbine_list[i+j_pw*turbine_num]] = pw_df_all[turbine_list[i+j_pw*turbine_num]]].to_list()
    # else: 
    #     powerCurve['yAxis']['理论功率'] = pw_df_all['pwrat'].to_list()
    #     for i in range(rem):
    #         powerCurve['yAxis'][turbine_list[i+j_pw*turbine_num]] = pw_df_all[turbine_list[i+j_pw*turbine_num]].to_list()
        
    return powerCurve