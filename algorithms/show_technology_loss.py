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
from db.db import selectTechnologyLossAll
from matplotlib import pyplot as plt
from pylab import mpl
import sys
import statistics as st
from scipy import signal,integrate
from datetime import datetime
# from configs.config import wspd, pwrat

def analyse(farmName, typeName:list, startTime, endTime):
    #赋值
    Technology_loss_all, turbine_dict = selectTechnologyLossAll(pd.DataFrame(), farmName, typeName, datetime.strptime(startTime, "%Y-%m-%d"), datetime.strptime(endTime, "%Y-%m-%d"))
    turbine_type = typeName
    turbine_list = []
    for type_name, turbine_names in turbine_dict.items():
        # for turbine_name in turbine_names:
        turbine_list += turbine_names
    result = {'table':[]}
    #################################
    if Technology_loss_all.empty():
        return result
    #技术待命展示
    Technology_loss_show = pd.DataFrame()
    Technology_loss_all_temp = Technology_loss_all[Technology_loss_all['type'].isin(typeName)]
    Technology_loss_all_temp = Technology_loss_all_temp.loc[startTime:endTime,:]
    Technology_loss_all_temp = Technology_loss_all_temp[Technology_loss_all_temp['wtid'].isin(turbine_list)]
    
    for num in range(len(turbine_list)):
        Technology_loss_show_temp = pd.DataFrame()
        temp_turbine = Technology_loss_all_temp[Technology_loss_all_temp['wtid']==turbine_list[num]]
        if len(temp_turbine) > 0:
            fnum_list = np.unique(temp_turbine['fault'])
            for i in range(len(fnum_list)):
                temp = temp_turbine[(temp_turbine['fault']==fnum_list[i])]
                Technology_loss_show_temp.loc[i,'fault'] = fnum_list[i]
                Technology_loss_show_temp.loc[i,'count'] = np.nansum(temp['count'])
                Technology_loss_show_temp.loc[i,'time'] = np.nansum(temp['time'])
                Technology_loss_show_temp.loc[i,'loss'] = np.nansum(temp['loss'])#kwh
                Technology_loss_show_temp.loc[i,'wspd'] = np.nanmean(temp['wspd'])
                Technology_loss_show_temp.loc[i,'fault_describe'] = temp[temp['fault']==fnum_list[i]]['fault_describe'].values[0]
        Technology_loss_show_temp.insert(0, 'wtid', turbine_list[num])
        Technology_loss_show = pd.concat([Technology_loss_show,Technology_loss_show_temp])#.append(Technology_loss_show_temp)
    if len(Technology_loss_show)>0:
        Technology_loss_show.loc[(Technology_loss_show['count']==0),'count'] = 1
        for i in range(len(Technology_loss_show)):
            elem = {}
            #机位号
            elem['wtid'] = '%s'%(Technology_loss_show.iloc[i]['wtid'])
            #故障编码
            elem['faultCode'] = '%s'%(Technology_loss_show.iloc[i]['fault'])
            #故障频次
            elem['faultCount'] = '%.4f'%(Technology_loss_show.iloc[i]['count'])
            #故障时长(h)
            elem['faultTime'] = '%.4f'%(Technology_loss_show.iloc[i]['time'])
            #故障损失电量(kwh)
            elem['faultLoss'] = '%.4f'%(Technology_loss_show.iloc[i]['loss'])
            #平均风速(m/s)
            elem['meanWindSpeed'] = '%.4f'%(Technology_loss_show.iloc[i]['wspd'])
            #故障描述
            elem['faultDescribe'] = '%s'%(Technology_loss_show.iloc[i]['fault_describe'])
            result['table'].append(elem)

    return result