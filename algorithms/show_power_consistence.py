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
from db.db import selectPwTurbineAll, selectTheoryWindPower,insertWindDirectionPicture, upload, insertPwTurbineAll, insertWindFrequencyPicture, insertAirDensityPicture,insertTurbulencePicture,insertNavigationBiasDirectionPicture,insertNavigationBiasControlPicture,insertPitchAnglePicture,insertPitchActionPicture,insertTorqueControlPicture
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
    #######机组功率曲线计算 wtid替换turbine_list
    for num in range(len(wtid)):
        turbine_name = wtid[num]
        pw_df = turbine_efficiency_function.pwratcurve_rho(pw_turbine_all,turbine_type,turbine_name,pw_startTime,pw_endTime,windbin,6)
        pw_df = pw_df.loc[:,['windbin','pwrat','count']]
        pw_df.rename(columns = {'pwrat':turbine_name,'count':str(turbine_name+'_'+'count')},inplace = True) 
        pw_df_all = pd.merge(pw_df_all,pw_df,how='outer',on='windbin')
        
    
    #####某机型实测功率曲线及理论功率曲线
    pw_df_all = pw_df_all.dropna(axis=0,how='all',subset=pw_df_all.columns[1:],inplace=False)
    pw_df_all = pd.merge(pw_df_all,pwrat_standard,how='outer',on='windbin') 
    
    #######功率曲线一致性排序：输出内容：功率一致性分析：功率一致性
    pw_df_order = pd.DataFrame()
    pw_df_order['wtid'] = wtid #wtid替换turbine_list
    pw_df_order['k_order'] = 1.0
    #
    for num in range(len(wtid)): #wtid替换turbine_list
        turbine_name = wtid[num]
        pw_df_temp = pw_df_all.loc[:,['windbin','pwrat',turbine_name,str(turbine_name+'_'+'count')]]
        pw_df_temp = pw_df_temp.dropna()
        #pw_df_order.loc[pw_df_order['wtid']==turbine_name,'k_order'] = 1 - np.sum(1-pw_df_temp[turbine_name]/pw_df_temp['pwrat'])/len(pw_df_temp)
        #turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'k_order'] = 1 - np.sum(1-pw_df_temp[turbine_name]/pw_df_temp['pwrat'])/len(pw_df_temp)
        pw_df_order.loc[pw_df_order['wtid']==turbine_name,'k_order'] = np.sum(pw_df_temp[turbine_name]*pw_df_temp[str(turbine_name+'_'+'count')])/np.sum(pw_df_temp['pwrat']*pw_df_temp[str(turbine_name+'_'+'count')])
        # turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'k_order'] = pw_df_order.loc[pw_df_order['wtid']==turbine_name,'k_order'].values

    pw_df_order.sort_values('k_order',inplace=True, ascending=False)
    for i in range(len(pw_df_order)):
        #使用传入机位号控制传出曲线数量
        if pw_df_order.iloc[i]['wtid'] in wtid:
            elem = {}
            elem["wtid"] = "%s"%(pw_df_order.iloc[i]['wtid'])
            elem["consistence"] = "%.3f"%(pw_df_order.iloc[i]['k_order'])
            elem["order"] = str(i+1)
            result['table'].append(elem)
    # #风机编号
    # result['wtid'] = pw_df_order['wtid'].to_list()
    # #一致性系数
    # result['consistence'] = pw_df_order['k_order'].to_list()
    if len(pw_df_order[((pw_df_order['k_order']>1.05*1.05)|(pw_df_order['k_order']<0.95*0.95))])>0:
        print(f'##########风场:{farmName}, 机型{typeName}, 功率曲线一致性较差###########')
    return result, pw_df_order