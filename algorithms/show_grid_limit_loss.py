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
from db.db import selectLimgridLossAll
from matplotlib import pyplot as plt
from pylab import mpl
import sys
import statistics as st
from scipy import signal,integrate
from datetime import datetime
# from configs.config import wspd, pwrat

def analyse(farmName, typeName:list, startTime, endTime):
    #赋值
    limgrid_loss_all, turbine_dict = selectLimgridLossAll(pd.DataFrame(), farmName, typeName, datetime.strptime(startTime), datetime.strptime(endTime))
    turbine_type = typeName
    turbine_list = []
    for type_name, turbine_names in turbine_dict.items():
        # for turbine_name in turbine_names:
        turbine_list += turbine_names
    result = {'table':[]}
    #################################
    #电网限电展示
    limgrid_loss_show = pd.DataFrame()
    limgrid_loss_all_temp = limgrid_loss_all[limgrid_loss_all['type'].isin(turbine_type)]
    limgrid_loss_all_temp = limgrid_loss_all_temp.loc[startTime:endTime,:]
    limgrid_loss_all_temp = limgrid_loss_all_temp[limgrid_loss_all_temp['wtid'].isin(turbine_list)]
    
    for num in range(len(turbine_list)):
        limgrid_loss_show_temp = pd.DataFrame()
        temp_turbine = limgrid_loss_all_temp[limgrid_loss_all_temp['wtid']==turbine_list[num]]
        if len(temp_turbine) > 0:
            limgrid_loss_show_temp.loc[num,'time'] = np.nansum(temp_turbine['time'])
            limgrid_loss_show_temp.loc[num,'loss'] = np.nansum(temp_turbine['loss'])#kwh
            limgrid_loss_show_temp.loc[num,'wspd'] = np.nanmean(temp_turbine['wspd'])
            limgrid_loss_show_temp.loc('wtid', turbine_list[num])
            elem = {}
            elem['机位号'] = limgrid_loss_show_temp.iloc[num]['wtid']
            elem['故障时长(h)'] = limgrid_loss_show_temp.iloc[num]['time']
            elem['故障损失电量(kwh)'] = limgrid_loss_show_temp.iloc[num]['loss']
            elem['平均风速(m/s)'] = limgrid_loss_show_temp.iloc[num]['wspd']
            result['table'].append(elem)
            #stop_loss_show_temp.loc[i,'exltmp'] = np.nanmean(temp_turbine['exltmp'])
        # limgrid_loss_show_temp.insert(0, 'wtid', turbine_list[num])
        limgrid_loss_show = limgrid_loss_show.append(limgrid_loss_show_temp)
    
    return result