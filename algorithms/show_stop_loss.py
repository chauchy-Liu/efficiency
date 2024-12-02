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
from db.db import selectStopLossAll
from matplotlib import pyplot as plt
from pylab import mpl
import sys
import statistics as st
from scipy import signal,integrate
from datetime import datetime
# from configs.config import wspd, pwrat

def analyse(farmName, typeName:list, startTime, endTime):
    #赋值
    stop_loss_all, turbine_dict = selectStopLossAll(pd.DataFrame(), farmName, typeName, datetime.strptime(startTime, "%Y-%m-%d"), datetime.strptime(endTime, "%Y-%m-%d"))
    turbine_type = typeName
    turbine_list = []
    for type_name, turbine_names in turbine_dict.items():
        # for turbine_name in turbine_names:
        turbine_list += turbine_names
    result = {'table':[]}
    #################################
    #计划停机展示
    stop_loss_show = pd.DataFrame()
    stop_loss_all_temp = stop_loss_all[stop_loss_all['type'].isin(typeName)]
    stop_loss_all_temp = stop_loss_all_temp.loc[startTime:endTime,:]
    stop_loss_all_temp = stop_loss_all_temp[stop_loss_all_temp['wtid'].isin(turbine_list)]
    
    for num in range(len(turbine_list)):
        stop_loss_show_temp = pd.DataFrame()
        temp_turbine = stop_loss_all_temp[stop_loss_all_temp['wtid']==turbine_list[num]]
        if len(temp_turbine) > 0: #记录结果用loc[num,'column']
            stop_loss_show_temp.loc[num,'time'] = np.nansum(temp_turbine['time'])
            stop_loss_show_temp.loc[num,'loss'] = np.nansum(temp_turbine['loss'])#kwh
            stop_loss_show_temp.loc[num,'wspd'] = np.nanmean(temp_turbine['wspd'])
            stop_loss_show_temp.loc[num,'exltmp'] = np.nanmean(temp_turbine['exltmp'])
            stop_loss_show_temp.loc[num,'wtid'] = turbine_list[num]
            elem = {}
            #机位号
            elem['wtid'] = '%s'%(stop_loss_show_temp.loc[num,'wtid'])
            #故障时长(h)
            elem['faultTime'] = '%.4f'%(stop_loss_show_temp.loc[num,'time'])
            #故障损失电量(kwh)
            elem['faultLoss'] = '%.4f'%(stop_loss_show_temp.loc[num,'loss'])
            #平均风速(m/s)
            elem['meanWindSpeed'] = '%.4f'%(stop_loss_show_temp.loc[num,'wspd'])
            result['table'].append(elem)
        # stop_loss_show_temp.insert(0, 'wtid', turbine_list[num])
        stop_loss_show = pd.concat([stop_loss_show,stop_loss_show_temp])#.append(stop_loss_show_temp)

    return result