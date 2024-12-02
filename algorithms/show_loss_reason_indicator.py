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
from db.db import selectFaultLossAll,selectLimturbineLossAll,selectStopLossAll,selectLimgridLossAll,selectFaultgridLossAll,selectTechnologyLossAll,selectEnyWspdAll
from matplotlib import pyplot as plt
from pylab import mpl
import sys
import statistics as st
from scipy import signal,integrate
from datetime import datetime
# from configs.config import wspd, pwrat

def analyse(farmName, typeName:list, startTime, endTime):
    #赋值
    fault_loss_all, turbine_FaultLoss_dict = selectFaultLossAll(pd.DataFrame(), farmName, typeName, datetime.strptime(startTime, "%Y-%m-%d"), datetime.strptime(endTime, "%Y-%m-%d"))
    limturbine_loss_all, turbine_LimturbineLoss_dict = selectLimturbineLossAll(pd.DataFrame(), farmName, typeName, datetime.strptime(startTime, "%Y-%m-%d"), datetime.strptime(endTime, "%Y-%m-%d"))
    stop_loss_all, turbine_StopLoss_dict = selectStopLossAll(pd.DataFrame(), farmName, typeName, datetime.strptime(startTime, "%Y-%m-%d"), datetime.strptime(endTime, "%Y-%m-%d"))
    limgrid_loss_all, turbine_LimgridLoss_dict = selectLimgridLossAll(pd.DataFrame(), farmName, typeName, datetime.strptime(startTime, "%Y-%m-%d"), datetime.strptime(endTime, "%Y-%m-%d"))
    faultgrid_loss_all, turbine_FaultgridLoss_dict = selectFaultgridLossAll(pd.DataFrame(), farmName, typeName, datetime.strptime(startTime, "%Y-%m-%d"), datetime.strptime(endTime, "%Y-%m-%d"))
    Technology_loss_all, turbine_TechnologyLoss_dict = selectTechnologyLossAll(pd.DataFrame(), farmName, typeName, datetime.strptime(startTime, "%Y-%m-%d"), datetime.strptime(endTime, "%Y-%m-%d"))
    eny_wspd_all, turbine_EnyWspd_dict = selectEnyWspdAll(pd.DataFrame(), farmName, typeName, datetime.strptime(startTime, "%Y-%m-%d"), datetime.strptime(endTime, "%Y-%m-%d"))

    turbine_type = typeName
    ####################################################3
    turbine_FaultLoss_list = []
    for type_name, turbine_names in turbine_FaultLoss_dict.items():
        # for turbine_name in turbine_names:
        turbine_FaultLoss_list += turbine_names
    turbine_LimturbineLoss_list = []
    for type_name, turbine_names in turbine_LimturbineLoss_dict.items():
        # for turbine_name in turbine_names:
        turbine_LimturbineLoss_list += turbine_names
    turbine_StopLoss_list = []
    for type_name, turbine_names in turbine_StopLoss_dict.items():
        # for turbine_name in turbine_names:
        turbine_StopLoss_list += turbine_names
    turbine_LimgridLoss_list = []
    for type_name, turbine_names in turbine_LimgridLoss_dict.items():
        # for turbine_name in turbine_names:
        turbine_LimgridLoss_list += turbine_names
    turbine_FaultgridLoss_list = []
    for type_name, turbine_names in turbine_FaultgridLoss_dict.items():
        # for turbine_name in turbine_names:
        turbine_FaultgridLoss_list += turbine_names
    turbine_TechnologyLoss_list = []
    for type_name, turbine_names in turbine_TechnologyLoss_dict.items():
        # for turbine_name in turbine_names:
        turbine_TechnologyLoss_list += turbine_names
    turbine_EnyWspd_list = []
    for type_name, turbine_names in turbine_EnyWspd_dict.items():
        # for turbine_name in turbine_names:
        turbine_EnyWspd_list += turbine_names
    result = {'reason':{}, 'indicator':{}}
    #################################
    #机组故障
    fault_loss_show = pd.DataFrame()
    if fault_loss_all.empty == False:
        fault_loss_all_temp = fault_loss_all[fault_loss_all['type'].isin(turbine_type)]
        fault_loss_all_temp = fault_loss_all_temp.loc[startTime:endTime,:]
        fault_loss_all_temp = fault_loss_all_temp[fault_loss_all_temp['wtid'].isin(turbine_FaultLoss_list)]
        
        for num in range(len(turbine_FaultLoss_list)):
            fault_loss_show_temp = pd.DataFrame()
            temp_turbine = fault_loss_all_temp[fault_loss_all_temp['wtid']==turbine_FaultLoss_list[num]]
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
            fault_loss_show_temp.insert(0, 'wtid', turbine_FaultLoss_list[num])
            fault_loss_show = pd.concat([fault_loss_show,fault_loss_show_temp])#.append(fault_loss_show_temp)
        if len(fault_loss_show)>0:
            fault_loss_show.loc[(fault_loss_show['count']==0),'count'] = 1
    #################################
    #机组自限电展示
    limturbine_loss_show = pd.DataFrame()
    if limturbine_loss_all.empty == False:
        limturbine_loss_all_temp = limturbine_loss_all[limturbine_loss_all['type'].isin(typeName)]
        limturbine_loss_all_temp = limturbine_loss_all_temp.loc[startTime:endTime,:]
        limturbine_loss_all_temp = limturbine_loss_all_temp[limturbine_loss_all_temp['wtid'].isin(turbine_LimturbineLoss_list)]
        
        for num in range(len(turbine_LimturbineLoss_list)):
            limturbine_loss_show_temp = pd.DataFrame()
            temp_turbine = limturbine_loss_all_temp[limturbine_loss_all_temp['wtid']==turbine_LimturbineLoss_list[num]]
            if len(temp_turbine) > 0:
                limturbine_loss_show_temp.loc[num,'time'] = np.nansum(temp_turbine['time'])
                limturbine_loss_show_temp.loc[num,'loss'] = np.nansum(temp_turbine['loss'])#kwh
                limturbine_loss_show_temp.loc[num,'wspd'] = np.nanmean(temp_turbine['wspd'])
                limturbine_loss_show_temp.loc[num,'wtid'] = turbine_LimturbineLoss_list[num]
                #stop_loss_show_temp.loc[i,'exltmp'] = np.nanmean(temp_turbine['exltmp'])
            # limturbine_loss_show_temp.insert(0, 'wtid', turbine_list[num])
            limturbine_loss_show = pd.concat([limturbine_loss_show,limturbine_loss_show_temp])#.append(limturbine_loss_show_temp)
    #################################
    #技术待命展示
    Technology_loss_show = pd.DataFrame()
    if Technology_loss_all.empty == False:
        Technology_loss_all_temp = Technology_loss_all[Technology_loss_all['type'].isin(typeName)]
        Technology_loss_all_temp = Technology_loss_all_temp.loc[startTime:endTime,:]
        Technology_loss_all_temp = Technology_loss_all_temp[Technology_loss_all_temp['wtid'].isin(turbine_TechnologyLoss_list)]
        
        for num in range(len(turbine_TechnologyLoss_list)):
            Technology_loss_show_temp = pd.DataFrame()
            temp_turbine = Technology_loss_all_temp[Technology_loss_all_temp['wtid']==turbine_TechnologyLoss_list[num]]
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
            Technology_loss_show_temp.insert(0, 'wtid', turbine_TechnologyLoss_list[num])
            Technology_loss_show = np.concat([Technology_loss_show,Technology_loss_show_temp])#.append(Technology_loss_show_temp)
        if len(Technology_loss_show)>0:
            Technology_loss_show.loc[(Technology_loss_show['count']==0),'count'] = 1
   #################################
    #计划停机展示
    stop_loss_show = pd.DataFrame()
    if stop_loss_all.empty == False:
        stop_loss_all_temp = stop_loss_all[stop_loss_all['type'].isin(typeName)]
        stop_loss_all_temp = stop_loss_all_temp.loc[startTime:endTime,:]
        stop_loss_all_temp = stop_loss_all_temp[stop_loss_all_temp['wtid'].isin(turbine_StopLoss_list)]
        
        for num in range(len(turbine_StopLoss_list)):
            stop_loss_show_temp = pd.DataFrame()
            temp_turbine = stop_loss_all_temp[stop_loss_all_temp['wtid']==turbine_StopLoss_list[num]]
            if len(temp_turbine) > 0:
                stop_loss_show_temp.loc[num,'time'] = np.nansum(temp_turbine['time'])
                stop_loss_show_temp.loc[num,'loss'] = np.nansum(temp_turbine['loss'])#kwh
                stop_loss_show_temp.loc[num,'wspd'] = np.nanmean(temp_turbine['wspd'])
                stop_loss_show_temp.loc[num,'exltmp'] = np.nanmean(temp_turbine['exltmp'])
                stop_loss_show_temp.loc[num,'wtid'] = turbine_StopLoss_list[num]
            # stop_loss_show_temp.insert(0, 'wtid', turbine_list[num])
            stop_loss_show = pd.concat([stop_loss_show,stop_loss_show_temp])#.append(stop_loss_show_temp) 
    #################################
    #电网限电展示
    limgrid_loss_show = pd.DataFrame()
    if limgrid_loss_all.empty == False:
        limgrid_loss_all_temp = limgrid_loss_all[limgrid_loss_all['type'].isin(turbine_type)]
        limgrid_loss_all_temp = limgrid_loss_all_temp.loc[startTime:endTime,:]
        limgrid_loss_all_temp = limgrid_loss_all_temp[limgrid_loss_all_temp['wtid'].isin(turbine_LimgridLoss_list)]
        
        for num in range(len(turbine_LimgridLoss_list)):
            limgrid_loss_show_temp = pd.DataFrame()
            temp_turbine = limgrid_loss_all_temp[limgrid_loss_all_temp['wtid']==turbine_LimgridLoss_list[num]]
            if len(temp_turbine) > 0:
                limgrid_loss_show_temp.loc[num,'time'] = np.nansum(temp_turbine['time'])
                limgrid_loss_show_temp.loc[num,'loss'] = np.nansum(temp_turbine['loss'])#kwh
                limgrid_loss_show_temp.loc[num,'wspd'] = np.nanmean(temp_turbine['wspd'])
                limgrid_loss_show_temp.loc('wtid', turbine_LimgridLoss_list[num])
                #stop_loss_show_temp.loc[i,'exltmp'] = np.nanmean(temp_turbine['exltmp'])
            # limgrid_loss_show_temp.insert(0, 'wtid', turbine_list[num])
            limgrid_loss_show = pd.concat([limgrid_loss_show,limgrid_loss_show_temp])#.append(limgrid_loss_show_temp)
    #################################
    #电网故障展示
    faultgrid_loss_show = pd.DataFrame()
    if faultgrid_loss_all.empty == False:
        faultgrid_loss_all_temp = faultgrid_loss_all[faultgrid_loss_all['type'].isin(turbine_type)]
        faultgrid_loss_all_temp = faultgrid_loss_all_temp.loc[startTime:endTime,:]
        faultgrid_loss_all_temp = faultgrid_loss_all_temp[faultgrid_loss_all_temp['wtid'].isin(turbine_FaultgridLoss_list)]
        
        for num in range(len(turbine_FaultgridLoss_list)):
            faultgrid_loss_show_temp = pd.DataFrame()
            temp_turbine = faultgrid_loss_all_temp[faultgrid_loss_all_temp['wtid']==turbine_FaultgridLoss_list[num]]
            if len(temp_turbine) > 0:
                fnum_list = np.unique(temp_turbine['fault'])
                for i in range(len(fnum_list)):
                    temp = temp_turbine[(temp_turbine['fault']==fnum_list[i])]
                    faultgrid_loss_show_temp.loc[i,'fault'] = fnum_list[i]
                    faultgrid_loss_show_temp.loc[i,'count'] = np.nansum(temp['count'])
                    faultgrid_loss_show_temp.loc[i,'time'] = np.nansum(temp['time'])
                    faultgrid_loss_show_temp.loc[i,'loss'] = np.nansum(temp['loss'])#kwh
                    faultgrid_loss_show_temp.loc[i,'wspd'] = np.nanmean(temp['wspd'])
                    faultgrid_loss_show_temp.loc[i,'fault_describe'] = temp[temp['fault']==fnum_list[i]]['fault_describe'].values[0]
            faultgrid_loss_show_temp.insert(0, 'wtid', turbine_FaultgridLoss_list[num])
            faultgrid_loss_show = pd.concat([faultgrid_loss_show,faultgrid_loss_show_temp])#.append(faultgrid_loss_show_temp)
        if len(faultgrid_loss_show)>0:
            faultgrid_loss_show.loc[(faultgrid_loss_show['count']==0),'count'] = 1
    ##################################
    #风能
    # if eny_wspd_all.empty == False:
    eny_wspd_temp = eny_wspd_all[eny_wspd_all['type'].isin(typeName)]
    eny_wspd_temp = eny_wspd_temp.loc[startTime:endTime,:]
    eny_wspd_temp = eny_wspd_temp[eny_wspd_temp['wtid'].isin(turbine_EnyWspd_list)]
    #平均风速
    if np.nansum(eny_wspd_temp['count']) != 0:
        result['indicator']['meanWindSpeed'] = '%.4f'%(np.nansum(eny_wspd_temp['wspd'].multiply(eny_wspd_temp['count']))/np.nansum(eny_wspd_temp['count']))
    else:
        result['indicator']['meanWindSpeed'] = '%.4f'%(np.nansum(eny_wspd_temp['wspd'])/len(eny_wspd_temp['wspd'].notnull()))
    #实发电量
    result['indicator']['actualPower'] = '%.4f'%(np.nansum(eny_wspd_temp['eny']) / 10000.)
    #等效小时
    result['indicator']['validHour'] = '%.4f'%(np.nansum(eny_wspd_temp['eny'])/np.nansum(eny_wspd_temp.loc[~eny_wspd_temp['wtid'].duplicated(),'Rate_power']))

    #####故障损失电量
    if len(fault_loss_show) <= 0:
            fault_loss_show['loss'] = 0
    if len(limgrid_loss_show) <= 0:
        limgrid_loss_show['loss'] = 0
    if len(faultgrid_loss_show) <= 0:
        faultgrid_loss_show['loss'] = 0
    if len(limturbine_loss_show) <= 0:
        limturbine_loss_show['loss'] = 0
    if len(stop_loss_show) <= 0:
        stop_loss_show['loss'] = 0
    if len(Technology_loss_show) <= 0:
        Technology_loss_show['loss'] = 0
    result['reason']['turbineFaultLoss'] = '%.4f'%(np.nansum(fault_loss_show['loss']) / 10000.)
    result['reason']['limGridLoss'] = '%.4f'%(np.nansum(limgrid_loss_show['loss']) / 10000.)
    result['reason']['gridFaultLoss'] = '%.4f'%(np.nansum(faultgrid_loss_show['loss']) / 10000.)
    result['reason']['limTurbineLoss'] = '%.4f'%(np.nansum(limturbine_loss_show['loss']) / 10000.)
    result['reason']['technologyLoss'] = '%.4f'%(np.nansum(Technology_loss_show['loss']) / 10000.)
    result['reason']['stopLoss'] = '%.4f'%(np.nansum(stop_loss_show['loss']))


    #时间可利用率
    if fault_loss_show.empty == False:
        result['indicator']['timeAvailableRate'] = '{:.2%}'. format(turbine_efficiency_function.Time_Avail(startTime,endTime,fault_loss_show))
    else:
        result['indicator']['timeAvailableRate'] = '100%'
    #MTBF
    if fault_loss_show.empty == False:
        result['indicator']['MTBF'] = '%.1f' %(turbine_efficiency_function.MTBT_Calculate(startTime,endTime,fault_loss_show))  
    else:
        result['indicator']['MTBF'] = '0.0'
    #能量可利用率
    result['indicator']['powerRate'] = '{:.2%}'.format(turbine_efficiency_function.Eny_Avail(float(result['indicator']['actualPower']),float(result['reason']['turbineFaultLoss']),float(result['reason']['limGridLoss']),float(result['reason']['limTurbineLoss']),float(result['reason']['gridFaultLoss']),float(result['reason']['stopLoss']),float(result['reason']['technologyLoss'])))
    #限电率
    result['indicator']['limitPowerRate'] ='{:.2%}'.format(float(result['reason']['limGridLoss'])/(float(result['indicator']['actualPower'])+float(result['reason']['limGridLoss']))) 
    #无故障时长
    if fault_loss_all.empty == False and faultgrid_loss_all.empty == False:
        result['indicator']['noFaultTime'] ='%.0f'%(turbine_efficiency_function.NotFault_Time(startTime,endTime,fault_loss_all_temp,faultgrid_loss_all_temp))
    elif fault_loss_all.empty == True and faultgrid_loss_all.empty == False:
        day_range = pd.date_range(startTime,endTime,freq="24H",normalize=True).strftime('%Y-%m-%d %H:%M:%S').to_list()
        result['indicator']['noFaultTime'] = '%.0f' %(len(day_range) - len(np.unique(faultgrid_loss_all_temp.index)))
    elif fault_loss_all.empty == False and faultgrid_loss_all.empty == True:
        day_range = pd.date_range(startTime,endTime,freq="24H",normalize=True).strftime('%Y-%m-%d %H:%M:%S').to_list()
        result['indicator']['noFaultTime'] = '%.0f' %(len(day_range) - len(np.unique(fault_loss_all_temp.index)))
    else:
        day_range = pd.date_range(startTime,endTime,freq="24H",normalize=True).strftime('%Y-%m-%d %H:%M:%S').to_list()
        result['indicator']['noFaultTime'] = '%.0f' %(len(day_range))

    #故障停机恢复时间
    if (len(fault_loss_show) <= 0)&(len(faultgrid_loss_show) <= 0):
            result['indicator']['faultStoreTime'] = '0.0'
    elif (len(fault_loss_show) > 0)&(len(faultgrid_loss_show) <= 0):
        result['indicator']['faultStoreTime'] = '%.1f' %(np.sum(fault_loss_show['time']) / np.sum(fault_loss_show['count']))
    elif (len(fault_loss_show) <= 0)&(len(faultgrid_loss_show) > 0):
        result['indicator']['faultStoreTime'] = '%.1f' %(np.sum(faultgrid_loss_show['time']) / np.sum(faultgrid_loss_show['count']))
    else:
        result['indicator']['faultStoreTime'] = '%.1f' %((np.sum(faultgrid_loss_show['time'])+np.sum(fault_loss_show['time'])) / (np.sum(faultgrid_loss_show['count'])+np.sum(fault_loss_show['count'])))

    return result