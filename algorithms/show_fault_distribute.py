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
from db.db import selectFaultLossAll
from matplotlib import pyplot as plt
from pylab import mpl
import sys
import statistics as st
from scipy import signal,integrate
from datetime import datetime
# from configs.config import wspd, pwrat


def analyse(farmName, typeName:list, startTime, endTime):

    #赋值
    fault_loss_all, turbine_dict = selectFaultLossAll(pd.DataFrame(), farmName, typeName, datetime.strptime(startTime, "%Y-%m-%d"), datetime.strptime(endTime, "%Y-%m-%d"))
    turbine_type = typeName
    turbine_list = []
    for type_name, turbine_names in turbine_dict.items():
        # for turbine_name in turbine_names:
        turbine_list += turbine_names
    result = {}
    #################################
    if fault_loss_all.empty:
        return result
    fault_loss_show = pd.DataFrame()
    fault_loss_all_temp = fault_loss_all[fault_loss_all['type'].isin(turbine_type)]
    fault_loss_all_temp = fault_loss_all_temp.loc[startTime:endTime,:]
    fault_loss_all_temp = fault_loss_all_temp[fault_loss_all_temp['wtid'].isin(turbine_list)]
    
    for num in range(len(turbine_list)):
        fault_loss_show_temp = pd.DataFrame()
        temp_turbine = fault_loss_all_temp[fault_loss_all_temp['wtid']==turbine_list[num]]
        if len(temp_turbine) > 0:
            fnum_list = np.unique(temp_turbine['fault'])
            for i in range(len(fnum_list)):
                temp = temp_turbine[(temp_turbine['fault']==fnum_list[i])]
                fault_loss_show_temp.loc[i,'fault'] = fnum_list[i]
                fault_loss_show_temp.loc[i,'count'] = np.nansum(temp['count'])
                fault_loss_show_temp.loc[i,'time'] = np.nansum(temp['time'])
                fault_loss_show_temp.loc[i,'loss'] = np.nansum(temp['loss'])#kwh
                fault_loss_show_temp.loc[i,'wspd'] = np.nanmean(temp['wspd'])
                fault_loss_show_temp.loc[i,'fault_describe'] = temp[temp['fault']==fnum_list[i]]['fault_describe'].values[0]
                fault_loss_show_temp.loc[i,'fsyst'] = temp[temp['fault']==fnum_list[i]]['fsyst'].values[0]
        fault_loss_show_temp.insert(0, 'wtid', turbine_list[num])
        fault_loss_show = pd.concat([fault_loss_show,fault_loss_show_temp])#.append(fault_loss_show_temp)
    if len(fault_loss_show)>0:
        fault_loss_show.loc[(fault_loss_show['count']==0),'count'] = 1
    ###########故障按系统分类统计   
    myfault = pd.DataFrame()
    if len(fault_loss_show)>0:
        temp = fault_loss_show.pivot_table(['count'],index=['fsyst'],aggfunc='sum')
        temp['freq'] = temp['count'] / np.sum(temp['count'])
        myfault = pd.DataFrame({'fnum':temp.index,'freq':temp['freq'].values})
        #myfault = myfault[(myfault['freq']>=0.025)]
        #new_row = pd.Series({'fnum':'其它','freq':1-np.sum(myfault['freq'])})
        #myfault = myfault.append(new_row,ignore_index=True)
        myfault = myfault[myfault['freq']>0] 
        for i in range(len(myfault['freq'].values)):
            result[str(myfault.iloc[i]['fnum'])] = myfault.iloc[i]['freq']
        # fig = plt.figure(figsize=(5,5),dpi=100)
        # plt.axes(aspect=1.0)
        # plt.pie(x=myfault['freq'],labels=myfault['fnum'],autopct='%3.1f %%',shadow=False,labeldistance=1.05,startangle=90,pctdistance=0.8,textprops={'fontsize':8,'color':'black'},radius=1.0)
        # fig.savefig(path + '/' + 'fault_syst.png',bbox_inches='tight')

    return result