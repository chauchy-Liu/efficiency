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
from db.db import selectLimturbineLossAll
from matplotlib import pyplot as plt
from pylab import mpl
import sys
import statistics as st
from scipy import signal,integrate
from datetime import datetime
# from configs.config import wspd, pwrat

def analyse(farmName, typeName:list, startTime, endTime):
    #赋值
    limturbine_loss_all, turbine_dict = selectLimturbineLossAll(pd.DataFrame(), farmName, typeName, datetime.strptime(startTime, "%Y-%m-%d"), datetime.strptime(endTime, "%Y-%m-%d"))
    turbine_type = typeName
    turbine_list = []
    for type_name, turbine_names in turbine_dict.items():
        # for turbine_name in turbine_names:
        turbine_list += turbine_names
    result = {'table':[]}
    #################################
    if limturbine_loss_all.empty:
        return result
    #机组自限电展示
    limturbine_loss_show = pd.DataFrame()
    limturbine_loss_all_temp = limturbine_loss_all[limturbine_loss_all['type'].isin(typeName)]
    limturbine_loss_all_temp = limturbine_loss_all_temp.loc[startTime:endTime,:]
    limturbine_loss_all_temp = limturbine_loss_all_temp[limturbine_loss_all_temp['wtid'].isin(turbine_list)]
    
    for num in range(len(turbine_list)):
        limturbine_loss_show_temp = pd.DataFrame()
        temp_turbine = limturbine_loss_all_temp[limturbine_loss_all_temp['wtid']==turbine_list[num]]
        if len(temp_turbine) > 0:
            limturbine_loss_show_temp.loc[num,'time'] = np.nansum(temp_turbine['time'])
            limturbine_loss_show_temp.loc[num,'loss'] = np.nansum(temp_turbine['loss'])#kwh
            limturbine_loss_show_temp.loc[num,'wspd'] = np.nanmean(temp_turbine['wspd'])
            limturbine_loss_show_temp.loc[num,'wtid'] = turbine_list[num]
            elem = {}
            #机位号
            elem['wtid'] = '%s'%(limturbine_loss_show_temp.loc[num,'wtid'])
            #故障时长(h)
            elem['faultTime'] = '%.4f'%(limturbine_loss_show_temp.loc[num,'time'])
            #故障损失电量(kwh)
            elem['faultLoss'] = '%.4f'%(limturbine_loss_show_temp.loc[num,'loss'])
            #平均风速(m/s)
            elem['meanWindSpeed'] = '%.4f'%(limturbine_loss_show_temp.loc[num,'wspd'])
            result['table'].append(elem)
            #stop_loss_show_temp.loc[i,'exltmp'] = np.nanmean(temp_turbine['exltmp'])
        # limturbine_loss_show_temp.insert(0, 'wtid', turbine_list[num])
        limturbine_loss_show = pd.concat([limturbine_loss_show,limturbine_loss_show_temp])#.append(limturbine_loss_show_temp)

    return result