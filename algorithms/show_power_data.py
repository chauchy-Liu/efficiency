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
from db.db import selectPwTurbineAll, selectTheoryWindPower, insertWindDirectionPicture, upload, insertPwTurbineAll, insertWindFrequencyPicture, insertAirDensityPicture,insertTurbulencePicture,insertNavigationBiasDirectionPicture,insertNavigationBiasControlPicture,insertPitchAnglePicture,insertPitchActionPicture,insertTorqueControlPicture
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
    result = {'table':[]}
    #######机组功率曲线计算wtid替换turbine_list
    for num in range(len(wtid)):
        turbine_name = wtid[num]
        pw_df = turbine_efficiency_function.pwratcurve_rho(pw_turbine_all,turbine_type,turbine_name,pw_startTime,pw_endTime,windbin,6)
        pw_df = pw_df.loc[:,['windbin','pwrat','count']]
        pw_df.rename(columns = {'pwrat':turbine_name,'count':str(turbine_name+'_'+'count')},inplace = True) 
        pw_df_all = pd.merge(pw_df_all,pw_df,how='outer',on='windbin')
        
    
    #####某机型实测功率曲线及理论功率曲线
    pw_df_all = pw_df_all.dropna(axis=0,how='all',subset=pw_df_all.columns[1:],inplace=False)
    pw_df_all = pd.merge(pw_df_all,pwrat_standard,how='outer',on='windbin')
    #传入风机数量控制传出风机数量
    for turbine_name in wtid: #turbine_list:
        for i in range(len(pw_df_all['windbin'])):
            elem = {
                #机组
                'wtid': turbine_name,
                #风速
                'windSpeed': None,
                #理论值
                'theory': None,
                #实测值
                'actual': None
            }
            #风速
            if str(pw_df_all.iloc[i]['windbin']) == 'nan':
                elem['windSpeed'] = None
            else:
                elem['windSpeed'] = str(pw_df_all.iloc[i]['windbin'])
            #理论值
            if str(pw_df_all.iloc[i]['pwrat']) == 'nan':
                elem['theory'] = None
            else:
                elem['theory'] = '%.2f' %(pw_df_all.iloc[i]['pwrat'])
            #实测值
            if str(pw_df_all.iloc[i][turbine_name]) == 'nan':
                elem['actual'] = None
            else:
                elem['actual'] = '%.2f' %(pw_df_all.iloc[i][turbine_name])

            result['table'].append(elem)
    
    return result