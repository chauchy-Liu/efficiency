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
from db.db import selectPwTimeAll, insertTechnologyLossAll, insertLimturbineLossAll, insertFaultgridLossAll,insertStopLossAll,insertFaultLossAll, insertLimgridLossAll,insertEnyWspdAll
from matplotlib import pyplot as plt
from pylab import mpl
import sys
import statistics as st
from scipy import signal,integrate

name = algConfig['record_loss_indicator']['name']#'叶片角度不平衡'
# 把所需测点定义到每个算法里
ai_points = algConfig['record_loss_indicator']['ai_points']
ai_rename = algConfig['record_loss_indicator']['ai_rename']
di_points = algConfig['record_loss_indicator']['di_points']
di_rename = algConfig['record_loss_indicator']['di_points']
cj_di_points = algConfig['record_loss_indicator']['cj_di_points']
cj_di_rename = algConfig['record_loss_indicator']['cj_di_rename']
ty_di_points = algConfig['record_loss_indicator']['ty_di_points']
ty_di_rename = algConfig['record_loss_indicator']['ty_di_rename']
general_points = algConfig['record_loss_indicator']['general_points']
general_rename = algConfig['record_loss_indicator']['general_rename']
private_points = algConfig['record_loss_indicator']['private_points']
time_duration = algConfig['record_loss_indicator']['time_duration']
resample_interval = algConfig['record_loss_indicator']['resample_interval']
error_data_time_duration = algConfig['record_loss_indicator']['error_data_time_duration']
need_all_turbines = algConfig['record_loss_indicator']['need_all_turbines']
store_file = algConfig['record_loss_indicator']['store_file']

def judge_model(algorithms_configs):
    algorithms_configs['pw_df_alltime'] = pd.DataFrame()
    #变量名替换
    fault_loss_all = algorithms_configs['fault_loss_all']
    limgrid_loss_all = algorithms_configs['limgrid_loss_all']
    limturbine_loss_all = algorithms_configs['limturbine_loss_all']
    stop_loss_all = algorithms_configs['stop_loss_all']
    faultgrid_loss_all = algorithms_configs['faultgrid_loss_all']
    Technology_loss_all = algorithms_configs['Technology_loss_all']
    turbine_warning_all = algorithms_configs['turbine_warning_all']
    eny_wspd_all = algorithms_configs['eny_wspd_all']
    Turbine_attr_type = algorithms_configs['Turbine_attr_type']
    wtids = algorithms_configs['wtids']
    turbine_param_all = algorithms_configs['turbine_param_all']
    zuobiao = algorithms_configs['zuobiao']
    Df_all_all = algorithms_configs['Df_all_all']
    Df_all_m_all = algorithms_configs['Df_all_m_all']
    state = algorithms_configs['state']
    Pwrat_Rate = algorithms_configs['Pwrat_Rate']
    path = algorithms_configs['path']
    hub_high = algorithms_configs['hub_high']
    windbin = algorithms_configs['windbin']
    windbinreg = algorithms_configs['windbinreg']
    ManufacturerID = algorithms_configs['ManufacturerID']
    rotor_radius = algorithms_configs['rotor_radius']
    turbine_err_all = algorithms_configs['rotor_radius']
    fault_code = algorithms_configs['fault_code']
    state_code = algorithms_configs['state_code']
    typeName = algorithms_configs['typeName']
    ###########################################################
    #从mysql表中提取数据
    pw_df_alltime = selectPwTimeAll(pd.DataFrame(), algorithms_configs['farmName'], algorithms_configs['typeName'])
    ##########################################################每天执行一次计算
    day_list = pd.date_range(np.min(Df_all_m_all.index),np.max(Df_all_m_all.index),freq="1d",normalize=True).strftime('%Y-%m-%d %H:%M:%S').to_list()#7天为一周期判断
    for i in range(len(day_list)-1):
        print(str(day_list[i]))
        for num in range(len(Turbine_attr_type)):
            
            fault_loss = pd.DataFrame()
            limgrid_loss = pd.DataFrame()
            limturbine_loss = pd.DataFrame()
            stop_loss = pd.DataFrame()
            faultgrid_loss = pd.DataFrame()
            Technology_loss = pd.DataFrame()
            eny_wspd = pd.DataFrame()
            
            
            turbine_name = wtids[num]
            Pitch_Min = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Pitch_Min'].values[0]
            Rotspd_Connect = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Rotspd_Connect'].values[0]
            Rotspd_Rate = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Rotspd_Rate'].values[0]
            
            #print(str(turbine_name + '损失电量计算'))
            
            #fault_code = faultcode_SANY.fault
            pw_df_temp = pw_df_alltime.loc[:,['windbin','pwrat',turbine_name]]
            pw_df_temp[turbine_name] = pw_df_temp[turbine_name].fillna(pw_df_temp.loc[pw_df_temp[turbine_name].isnull(),'pwrat'])
            
            Df_all_temp = Df_all_m_all[Df_all_m_all['wtid'] == turbine_name]
            Df_all_m = Df_all_temp.loc[day_list[i]:day_list[i+1],:] 
            Df_all_m['windbin'] = pd.cut(Df_all_m['wspd','nanmean'],windbinreg,labels=windbin)
            
            try:
                Df_all_m_clear = turbine_efficiency_function.data_min_clear(Df_all_m,state,Rotspd_Connect,Rotspd_Rate,Pwrat_Rate,Pitch_Min,neighbors_num=20,threshold=3)
            except Exception:
                Df_all_m_clear = Df_all_m
                Df_all_m_clear['clear'] = 6


            ###单机故障损失计算(分仓、功率曲线合并后的数据框---未知故障解释的停机全部归到故障停机中)###有故障时故障码不为0！！！！！！！！！！！！！！
            ##金风机组发生编码为733的故障时，风速不更新，导致损失电量计算不准确
            #输出内容：损失分析：三个表
            fault_loss = turbine_efficiency_function.Turbine_Fault_Loss(Df_all_m,turbine_name,pw_df_temp,fault_code,state_code) 
            fault_loss.insert(0, 'type', typeName)
            fault_loss.insert(0, 'localtime', pd.to_datetime(day_list[i],format='%Y-%m-%d'))              
            fault_loss_all = fault_loss_all.append(fault_loss)
            
            ###单机电网限电损失(不包括限功率停机)，最小桨距角异常、额定功率异常不计算，这两种控制异常中台都会标记为90001！！！！！！
            limgrid_loss = turbine_efficiency_function.Grid_Limit_Loss(Df_all_m,turbine_name,pw_df_temp,fault_code,state_code)
            limgrid_loss.insert(0, 'type', typeName)
            limgrid_loss.insert(0, 'localtime', pd.to_datetime(day_list[i],format='%Y-%m-%d'))
            limgrid_loss_all = limgrid_loss_all.append(limgrid_loss)
                            
            
            ###单机计划停机损失
            stop_loss = turbine_efficiency_function.Stop_Loss(Df_all_m,turbine_name,pw_df_temp,state_code)
            stop_loss.insert(0, 'type', typeName)
            stop_loss.insert(0, 'localtime', pd.to_datetime(day_list[i],format='%Y-%m-%d'))
            stop_loss_all = stop_loss_all.append(stop_loss)
            
            #单机电网故障损失
            faultgrid_loss = turbine_efficiency_function.Grid_Fault_Loss(Df_all_m,turbine_name,pw_df_temp,fault_code,state_code)
            faultgrid_loss.insert(0, 'type', typeName)
            faultgrid_loss.insert(0, 'localtime', pd.to_datetime(day_list[i],format='%Y-%m-%d'))
            faultgrid_loss_all = faultgrid_loss_all.append(faultgrid_loss)
            
            ##单机自限电损失输入
            (data_limt,limturbine_loss) = turbine_efficiency_function.Turbine_Limit_Loss(Df_all_m_clear[Df_all_m_clear['clear'] <= 7],turbine_name,pw_df_temp,Pitch_Min,Pwrat_Rate,Rotspd_Connect,state)
            limturbine_loss.insert(0, 'type', typeName)
            limturbine_loss.insert(0, 'localtime', pd.to_datetime(day_list[i],format='%Y-%m-%d'))
            limturbine_loss_all = limturbine_loss_all.append(limturbine_loss) 
            
            #单机技术待命损失    
            Technology_loss = turbine_efficiency_function.Turbine_Technology_Loss(Df_all_m,turbine_name,pw_df_temp,fault_code,state_code) 
            Technology_loss.insert(0, 'type', typeName)
            Technology_loss.insert(0, 'localtime', pd.to_datetime(day_list[i],format='%Y-%m-%d'))
            Technology_loss_all = Technology_loss_all.append(Technology_loss)
            
            ##告警统计
            turbine_warning = turbine_efficiency_function.Turbine_Warning(Df_all_m,turbine_name,fault_code,state_code)
            turbine_warning.insert(0, 'type', typeName)
            turbine_warning.insert(0, 'localtime', pd.to_datetime(day_list[i],format='%Y-%m-%d'))
            turbine_warning_all = turbine_warning_all.append(turbine_warning)
            
            #日发电量及风速
            
            eny_wspd.loc[num,'type'] = typeName
            eny_wspd.loc[num,'wtid'] = turbine_name
            eny_wspd.loc[num,'eny'] = np.sum(Df_all_m[(Df_all_m['pwrat','nanmean']>0)&(Df_all_m['pwrat','nanmean']<30000)]['pwrat','nanmean']/6.0)
            eny_wspd.loc[num,'wspd'] = np.mean(Df_all_m[(Df_all_m['wspd','nanmean']>0)&(Df_all_m['wspd','nanmean']<50)]['wspd','nanmean'])
            eny_wspd.loc[num,'count'] = len(Df_all_m)
            eny_wspd.loc[num,'Rate_power'] = Pwrat_Rate
            eny_wspd.insert(0, 'localtime', pd.to_datetime(day_list[i],format='%Y-%m-%d'))
            eny_wspd_all = eny_wspd_all.append(eny_wspd)
            
    #全场单机型       
    ##全场损失统计           
    # fenduan_all.to_csv(str(path+'/fenduan.csv'),index=True, encoding='utf-8')
    if len(fault_loss_all)>0:
        fault_loss_all.set_index(('localtime'),inplace= True)
        insertFaultLossAll(fault_loss_all, algorithms_configs)
    if len(faultgrid_loss_all)>0:
        faultgrid_loss_all.set_index(('localtime'),inplace= True)
        insertFaultgridLossAll(faultgrid_loss_all, algorithms_configs)
    if len(limgrid_loss_all)>0:
        limgrid_loss_all.set_index(('localtime'),inplace= True)
        insertLimgridLossAll(limgrid_loss_all, algorithms_configs)
    if len(stop_loss_all)>0:
        stop_loss_all.set_index(('localtime'),inplace= True)
        insertStopLossAll(stop_loss_all, algorithms_configs)
    if len(limturbine_loss_all)>0:
        limturbine_loss_all.set_index(('localtime'),inplace= True)
        insertLimturbineLossAll(limturbine_loss_all, algorithms_configs)
    if len(Technology_loss_all)>0:
        Technology_loss_all.set_index(('localtime'),inplace= True)
        insertTechnologyLossAll(Technology_loss_all, algorithms_configs)
    if len(eny_wspd_all)>0:
        eny_wspd_all.set_index(('localtime'),inplace= True)
        insertEnyWspdAll(eny_wspd_all, algorithms_configs)
    # fault_loss_all.to_csv(str(path+'/fault_loss_all.csv'),index=True, encoding='utf-8')
    # faultgrid_loss_all.to_csv(str(path+'/faultgrid_loss_all.csv'),index=True, encoding='utf-8')
    # limgrid_loss_all.to_csv(str(path+'/limgrid_loss_all.csv'),index=True, encoding='utf-8')
    # stop_loss_all.to_csv(str(path+'/stop_loss_all.csv'),index=True, encoding='utf-8')
    # limturbine_loss_all.to_csv(str(path+'/limturbine_loss_all.csv'),index=True, encoding='utf-8')
    # err_result_all.to_csv(str(path+'/err_result_all.csv'),index=True, encoding='utf-8')
    # turbine_err_all.to_csv(str(path+'/turbine_err_all.csv'),index=True, encoding='utf-8')
    # Technology_loss_all.to_csv(str(path+'/Technology_loss_all.csv'),index=True, encoding='utf-8') 
    # turbine_param_all.to_csv(str(path+'/turbine_param_all.csv'),index=True, encoding='utf-8') 
    # turbine_warning_all.to_csv(str(path+'/turbine_warning_all.csv'),index=True, encoding='utf-8') 
    # pw_df_alltime.to_csv(str(path+'/pw_df_alltime.csv'),index=True, encoding='utf-8')