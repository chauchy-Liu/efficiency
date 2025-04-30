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
from db.db import insertWindDirectionPicture, upload, insertPwTurbineAll, insertPwTimeAll, insertWindFrequencyPicture, insertAirDensityPicture,insertTurbulencePicture,insertNavigationBiasDirectionPicture,insertNavigationBiasControlPicture,insertPitchAnglePicture,insertPitchActionPicture,insertTorqueControlPicture, insertAllWindFrequencyPicture, insertAllAirDensityPicture,insertAllTurbulencePicture
from matplotlib import pyplot as plt
from pylab import mpl
import sys
import statistics as st
from scipy import signal,integrate
import os
from utils.display_util import get_os

mpl.interactive(False)
plt.switch_backend('agg')
os_type = get_os()
if os_type == "win":
    plt.rcParams['font.sans-serif'] = ['SimHei'] #Windows
elif os_type == "mac":
    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS'] #mac
elif os_type == "linux":
    plt.rcParams['font.sans-serif'] = ['WenQuanYi Zen Hei'] #Linux
else:
    plt.rcParams['font.sans-serif'] = ['WenQuanYi Zen Hei'] #Linux
plt.rcParams['axes.unicode_minus'] = False 

name = algConfig['record_pwrt_picture']['name']#'叶片角度不平衡'
# 把所需测点定义到每个算法里
ai_points = algConfig['record_pwrt_picture']['ai_points']
ai_rename = algConfig['record_pwrt_picture']['ai_rename']
di_points = algConfig['record_pwrt_picture']['di_points']
di_rename = algConfig['record_pwrt_picture']['di_points']
cj_di_points = algConfig['record_pwrt_picture']['cj_di_points']
cj_di_rename = algConfig['record_pwrt_picture']['cj_di_rename']
ty_di_points = algConfig['record_pwrt_picture']['ty_di_points']
ty_di_rename = algConfig['record_pwrt_picture']['ty_di_rename']
general_points = algConfig['record_pwrt_picture']['general_points']
general_rename = algConfig['record_pwrt_picture']['general_rename']
private_points = algConfig['record_pwrt_picture']['private_points']
time_duration = algConfig['record_pwrt_picture']['time_duration']
resample_interval = algConfig['record_pwrt_picture']['resample_interval']
error_data_time_duration = algConfig['record_pwrt_picture']['error_data_time_duration']
need_all_turbines = algConfig['record_pwrt_picture']['need_all_turbines']
store_file = algConfig['record_pwrt_picture']['store_file']
async def judge_model(algorithms_configs):
    algorithms_configs['zuobiao'] = pd.DataFrame()
    algorithms_configs['pw_df_alltime'] = pd.DataFrame()
    algorithms_configs['pw_turbine_all'] = pd.DataFrame()
    algorithms_configs['windbinreg'] = np.arange(1.75,25.25,0.5)
    algorithms_configs['windbin'] = np.arange(2.0,25.0,0.5)
    algorithms_configs['pw_df_alltime']['windbin'] = algorithms_configs['windbin']
    algorithms_configs['wind_ti_all'] = pd.DataFrame()
    algorithms_configs['wind_ti_all']['windbin'] = algorithms_configs['windbin']
    #变量替换名字####################################
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
    turbine_err_all = algorithms_configs['turbine_err_all']
    pw_df_alltime = algorithms_configs['pw_df_alltime']
    pw_df_alltime['windbin'] = windbin
    wind_ti_all = pd.DataFrame()
    wind_ti_all['windbin'] = windbin
    # zuobiao_all = algorithms_configs['zuobiao_all']
    #######################################
    for num in range(len(Turbine_attr_type)): 
        turbine_name = wtids[num]            
        Pitch_Min = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Pitch_Min'].values[0]
        Rotspd_Connect = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Rotspd_Connect'].values[0]
        Rotspd_Rate = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Rotspd_Rate'].values[0]
        #Pitch_Min = 0.0
        #Rotspd_Connect = 400.0
        #Rotspd_Rate = 1250.0
        
        ##机组坐标海拔
        zuobiao.loc[num,'wtid'] = turbine_name
        zuobiao.loc[num,'X'] = Turbine_attr_type.loc[Turbine_attr_type['name']==turbine_name,'longitude'].values
        zuobiao.loc[num,'Y'] = Turbine_attr_type.loc[Turbine_attr_type['name']==turbine_name,'latitude'].values
        zuobiao.loc[num,'Z'] = Turbine_attr_type.loc[Turbine_attr_type['name']==turbine_name,'altitude'].values
        
        Df_all_m = Df_all_m_all[Df_all_m_all['wtid'] == turbine_name]
        #10min数据清洗
        try:
            Df_all_m_clear = turbine_efficiency_function.data_min_clear(Df_all_m,state,Rotspd_Connect,Rotspd_Rate,Pwrat_Rate,Pitch_Min,neighbors_num=20,threshold=3)
            df_all_clear = Df_all_m_clear[Df_all_m_clear['clear'] == 2]#1为干净值
        except Exception:
            Df_all_m_clear = Df_all_m
            Df_all_m_clear['clear'] = 2
            Df_all_m_clear['pitchlim'] = 0
            df_all_clear = Df_all_m_clear

        if len(df_all_clear)<100:
            Df_all_m_clear = Df_all_m
            Df_all_m_clear['clear'] = 2
            Df_all_m_clear['pitchlim'] = 0
            df_all_clear = Df_all_m_clear
                
        ##风向玫瑰图, 输出内容：风资源1
        picture_path = turbine_efficiency_function.WindRose(Df_all_m,path)
        ########################################################
        #上传图片到minio
        url_picture = upload(picture_path, algorithms_configs)
        #mysql记录
        insertWindDirectionPicture(algorithms_configs, url_picture, turbine_name)
        ########################################################
        ##各机组功率折算到标准空气密度，并保存计算结果            
        altitude = Turbine_attr_type.loc[Turbine_attr_type['name']==turbine_name,'altitude'].values[0]
        df_all_clear['rho'] = (101325.0*(1.0-0.0065*(altitude+hub_high)/(df_all_clear['exltmp','nanmean']+273.15))**5.255584)/(287.05*(df_all_clear['exltmp','nanmean']+273.15))
        df_all_clear['wspd_rho'] = (df_all_clear['wspd','nanmean'])*(df_all_clear['rho']/1.225)**(1.0/3.0)
        
        #df_all_clear.reset_index(level=0,inplace=True)
        #df_all_clear['windbin'] = pd.cut(df_all_clear['wspd_rho'],windbinreg,labels=windbin)
        #df_all_clear.set_index(('localtime',''),inplace= True)
        columns_names = [('wspd_rho',''),('pwrat','nanmean')]
        pw_turbine = df_all_clear.loc[:,columns_names]
        pw_turbine.columns = pw_turbine.columns.droplevel(1)
        pw_turbine.rename(columns = {'pwrat':turbine_name,'wspd_rho':str(turbine_name+'_'+'wspd')},inplace = True) 
        
        #####绘制近三月的功率曲线，用于控制策略分析, 输出内容：功率曲线一致性分析：机型功率曲线
        pw_df = turbine_efficiency_function.pwratcurve_rho_alltime(df_all_clear,windbin,6)
        pw_df = pw_df.loc[:,['windbin','pwrat','count']]
        pw_df.rename(columns = {'pwrat':turbine_name,'count':str(turbine_name+'_'+'count')},inplace = True) 
        pw_df_alltime = pd.merge(pw_df_alltime,pw_df,how='outer',on='windbin')
        
        if num==0:
            pw_turbine_all = pw_turbine
        else:
            pw_turbine_all = pd.merge(pw_turbine_all,pw_turbine,left_index=True,right_index=True,how='outer')
    # pw_turbine_all.insert(0,'type',np.unique(Turbine_attr['turbineTypeID'])[i_type])
    pw_turbine_all.insert(0,'type',algorithms_configs['typeName'])
    ################################################################3
    #mysql记录
    insertPwTurbineAll(pw_turbine_all, algorithms_configs)
    ################################################################3
    pw_df_alltime = pw_df_alltime.dropna(axis=0,how='all',subset=pw_df_alltime.columns[1:],inplace=False)
    pw_df_alltime.insert(1,'pwrat',0)
    for i in range(len(pw_df_alltime)):
        pw_df_alltime.iloc[i,1] = np.nanmedian(pw_df_alltime.iloc[i,2::2]) 
    ################################################################3
    #mysql记录
    insertPwTimeAll(pw_df_alltime, algorithms_configs)
    ################################################################3
    # zuobiao.insert(0,'type',np.unique(Turbine_attr['turbineTypeID'])[i_type])
    zuobiao.insert(0,'type',algorithms_configs['typeName'])
    ###某机型风频绘制, 输出内容：风资源2
    wind_freq = turbine_efficiency_function.winddistribute(Df_all_m_all,windbin,windbinreg)
    
    fig, ax = plt.subplots(figsize=(8, 4), dpi=100)
    ax.bar(wind_freq['windbin'],wind_freq['freq'],color='royalblue',bottom = 0,width=0.2)
    ax.set_xlabel(xlabel='风速(m/s)',fontsize=10, color='#ccc')
    ax.set_ylabel('风频',fontsize=10, color='#ccc')
    ax.tick_params(labelsize=10)   
    # 设置刻度颜色
    ax.tick_params(axis='x', colors='#426977')  # 设置角度刻度颜色
    ax.tick_params(axis='y', colors='#426977')  # 设置半径刻度颜色
    # 设置刻度值颜色
    for label in ax.get_xticklabels():
        label.set_color('#DBE9F1')  # 设置x轴刻度值颜色为橙色

    for label in ax.get_yticklabels():
        label.set_color('#DBE9F1')  # 设置y轴刻度值颜色为橙色
    # 设置坐标轴边框颜色
    ax.spines['top'].set_color('#426977')    # 设置上边框颜色
    ax.spines['right'].set_color('#426977')  # 设置右边框颜色
    ax.spines['left'].set_color('#426977')  # 设置左边框颜色
    ax.spines['bottom'].set_color('#426977')  # 设置下边框颜色

    fig.savefig(path + '/' + 'windfreq.png', transparent=True,dpi=100,bbox_inches='tight')
    ########################################################
    #上传图片到minio
    picture_path = path + '/' + 'windfreq.png'
    url_picture = upload(picture_path, algorithms_configs)
    #mysql记录
    insertWindFrequencyPicture(algorithms_configs, url_picture)
    ########################################################
    #某机型月平均空气密度及风速，输出内容：风资源3
    altitude_farm= np.nanmean(zuobiao['Z'])
    month_data, picture_path = turbine_efficiency_function.monthdata(Df_all_m_all,altitude_farm,path)
    algorithms_configs['zuobiao_all'] = pd.concat([algorithms_configs['zuobiao_all'], zuobiao])#.append(zuobiao)#全场1min数据
    ########################################################
    #上传图片到minio
    url_picture = upload(picture_path, algorithms_configs)
    #mysql记录
    insertAirDensityPicture(algorithms_configs, url_picture)
    ########################################################
    ###某机型湍流强度计算，输出内容：风资源4
    wind_ti_all = turbine_efficiency_function.wind_ti(Df_all_m_all, windbin, 6)        
    fig = plt.figure(figsize=(10,8),dpi=100)  
    with plt.style.context('ggplot'):  
        plt.plot(wind_ti_all['windbin'],wind_ti_all['ti'],'-o',color='cornflowerblue',markersize=5)           
        plt.grid()
        #plt.ylim(-2,25)
        plt.xlabel('风速(m/s)',fontsize=20, color='#ccc')
        plt.ylabel('湍流',fontsize=20, color='#ccc')
        plt.tick_params(which='both',labelcolor='#ccc', width=0,color='#426977', labelsize=20,gridOn=True,grid_color='#426977',direction ='in',right=True)
        plt.gca().spines["left"].set_color('#426977')
        plt.gca().spines["bottom"].set_color('#426977')
        plt.gca().spines["right"].set_color('#426977')
        plt.gca().spines["top"].set_color('#426977')
    fig.savefig(path + '/' +'湍流曲线'+'.png',dpi=100, transparent=True, bbox_inches='tight')
    ########################################################
    #上传图片到minio
    picture_path = path + '/' +'湍流曲线'+'.png'
    url_picture = upload(picture_path, algorithms_configs)
    #mysql记录
    insertTurbulencePicture(algorithms_configs, url_picture)
    ########################################################
    

############################################需3个月数据，每10天执行一次
    ############某机型控制性能分析###########
    fenduan_all = pd.DataFrame()
    err_result_all = pd.DataFrame()
    turbine_cp_all = pd.DataFrame()
    turbine_cp_all['windbin'] = windbin
    for num in range(len(Turbine_attr_type)):
        
        turbine_name = wtids[num]
        Pitch_Min = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Pitch_Min'].values[0]
        Rotspd_Connect = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Rotspd_Connect'].values[0]
        Rotspd_Rate = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Rotspd_Rate'].values[0]
        
        print(str(turbine_name + '控制性能分析'))
        Df_all_m = Df_all_m_all[Df_all_m_all['wtid'] == turbine_name]
        Df_all = Df_all_all[Df_all_all['wtid'] == turbine_name]
        
        
        #########无叶片2、3角度，防止报错
        if((['pitch2'] in Df_all.columns.values)==False):
            Df_all['pitch2'] = Df_all['pitch1']
            Df_all_m['pitch2','nanmean'] = Df_all_m['pitch1','nanmean']
            print('桨距角2不存在')
        if((['pitch3'] in Df_all.columns.values)==False):
            Df_all['pitch3'] = Df_all['pitch1']
            Df_all_m['pitch3','nanmean'] = Df_all_m['pitch1','nanmean']
            print('桨距角3不存在')
        
        dirbin = np.arange(-35.0,35.0,0.2)
        dirbin1 = np.arange(-35.1,35.1,0.2) 
        windbin1 = np.arange(3.75,7.75,0.5)
        windbin2 = np.arange(4.0,7.5,0.5)
        
        ###远景机组,计算处也得修改
        if ManufacturerID=='EN':
            #Df_all['wdir0'] = signal.medfilt(Df_all['wdir0'],5)
            Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir0'],0.5,5,11,0.1,1,1)
            #Df_all['wdir0'] = signal.medfilt(Df_all['wdir'],5)
            #Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean']           
        
        ##金风机组机组####,计算处也得修改
        if ManufacturerID=='GW':
            if((['wdir0'] in Df_all.columns.values)==False)&((['wdir25'] in Df_all.columns.values)==True):
                Df_all['wdir0'] = Df_all['wdir25'] - 180.0
                Df_all_m['wdir0','nanmean'] = Df_all_m['wdir25','nanmean'] - 180.0
            elif((['wdir0'] in Df_all.columns.values)==False)&((['wdir'] in Df_all.columns.values)==True):
                Df_all['wdir0'] = Df_all['wdir'] - 180.0
                Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
                Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir0'],0.2,1,11,0.1,1,1)#1:低通滤波器，2：中值滤波，3：wiener滤波，4：巴特沃斯滤波器
                Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean'] - 180.0  
            elif((['wdir0'] in Df_all.columns.values)==False)&((['wdir'] in Df_all.columns.values)==False)&((['wdirs'] in Df_all.columns.values)==True):
                Df_all['wdir0'] = signal.medfilt(Df_all['wdirs'],5)
                Df_all_m['wdir0','nanmean'] = Df_all_m['wdirs','nanmean']
            elif((['wdir0'] in Df_all.columns.values)==False)&((['wdir'] in Df_all.columns.values)==False)&((['wdirs'] in Df_all.columns.values)==False):
                Df_all['wdir0'] = 0.0
                Df_all_m['wdir0','nanmean'] = 0.0
            else:
                Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir0'],0.5,1,11,0.1,1,1)
                #Df_all['wdir00'] = Df_all['wdir25']-180.0
                #Df_all['wdir0'] = signal.medfilt(Df_all['wdir00'],1)  
            
        
        ##明阳机组####,计算处也得修改
        if (ManufacturerID=='MY')|(ManufacturerID=='My'):
            #Df_all['wdir'] = Df_all['wdir'].fillna(method='ffill')
            Df_all['wdir0'] = Df_all['wdir']
            #Df_all['wdir0'] = Df_all['wdir'] - Df_all['yaw']
            Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
            #Df_all['wdir0'] = Df_all['wdir']
            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir0'],0.2,5,11,0.5,1,1)#1:低通滤波器，2：中值滤波，3：wiener滤波，4：巴特沃斯滤波器
            Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean']
            #Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean'] - Df_all_m['yaw','nanmean']
                    
        #####三一机组、湘电机组,计算处也得修改
        if (ManufacturerID=='SE')|(ManufacturerID=='SI')|(ManufacturerID=='XE'):
            if((['wdir0'] in Df_all.columns.values)==False):
                Df_all['wdir0'] = Df_all['wdir'] - Df_all['yaw']
                Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
                Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean'] - Df_all_m['yaw','nanmean']
            else:
                Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir0'],0.5,5,11,0.5,1,1)
            
            #Df_all['wdir0'] = Df_all['wdir'] - 180.0
            #Df_all['wdir0'] = signal.medfilt(Df_all['wdir0'],5)
            #Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean'] - 180.0  
        
        ###海装机组、运达机组,计算处也得修改
        if (ManufacturerID=='H1')|(ManufacturerID=='HZ')|(ManufacturerID=='WD'):
            #Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
            Df_all['wdir'] = Df_all['wdir'].fillna(0)
            Df_all['wdir0'] = Df_all['wdir'] - 180.0
            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir0'],0.2,5,11,0.5,1,1)
            Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean'] - 180.0
        
        ###中车机组,计算处也得修改
        if ManufacturerID=='WT':
            if((['wdir0'] in Df_all.columns.values)==False):
                Df_all['wdir0'] = Df_all['wdir'].fillna(0)
                Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean']
            else:
                Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir'],0.2,5,11,0.5,1,1)   #许继不滤波            
        
        ###东气机组,计算处也得修改
        if ManufacturerID=='FD':
            Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir'],0.2,5,11,0.5,1,1)
            Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean']
            
        ###联合动力机组,计算处也得修改
        if ManufacturerID=='UP':
            Df_all['wdir'] = Df_all['wdir'].fillna(0)
            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir'],0.2,5,11,0.5,1,1)
            Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean']
        
        ###上气机组,计算处也得修改
        if (ManufacturerID=='W2')|(ManufacturerID=='W23')|(ManufacturerID=='W4'):
            Df_all['wdir'] = Df_all['wdir'].fillna(0)
            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir'],0.2,5,11,0.5,1,1)
            Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean']
            ###联合动力机组,计算处也得修改
        if ManufacturerID=='XW':
            Df_all['wdir'] = Df_all['wdir'].fillna(0)
            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir'],0.2,5,11,0.5,1,1)
            Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean']
            
            ###太重机组,计算处也得修改
        if ManufacturerID=='TZ':
            Df_all['wdir0'] = Df_all['wdir'] - 180.0
            Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
            Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean'] - 180.0
            #Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir0'],0.2,5,11,0.5,1,1)
            #Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean']
                
            
        
        #10min数据清洗
        try:
            Df_all_m_clear = turbine_efficiency_function.data_min_clear(Df_all_m,state,Rotspd_Connect,Rotspd_Rate,Pwrat_Rate,Pitch_Min,neighbors_num=20,threshold=3)
            df_all_clear = Df_all_m_clear[Df_all_m_clear['clear'] == 2]#1为干净值
        except Exception:
            Df_all_m_clear = Df_all_m
            Df_all_m_clear['clear'] = 2
            Df_all_m_clear['pitchlim'] = 0
            df_all_clear = Df_all_m_clear
        
        if len(df_all_clear)<100:
            Df_all_m_clear = Df_all_m
            Df_all_m_clear['clear'] = 2
            Df_all_m_clear['pitchlim'] = 0
            df_all_clear = Df_all_m_clear
        
        altitude = Turbine_attr_type.loc[Turbine_attr_type['name']==turbine_name,'altitude'].values[0]
        #df_all_clear['rho'] = 1.293*(10.0**(-(altitude+hub_high)/(18400.0*(1.0+0.003674*df_all_clear['exltmp','nanmean']))))/(1.0+0.003674*df_all_clear['exltmp','nanmean'])
        df_all_clear['rho'] = (101325.0*(1.0-0.0065*(altitude+hub_high)/(df_all_clear['exltmp','nanmean']+273.15))**5.255584)/(287.05*(df_all_clear['exltmp','nanmean']+273.15))
        df_all_clear['cp'] =  2000.0*df_all_clear['pwrat','nanmean'] / df_all_clear['rho'] / (np.pi*rotor_radius*rotor_radius) / df_all_clear['wspd','nanmean']**3
        df_all_clear['kopt'] = 1000.0*df_all_clear['pwrat','nanmean'] / (df_all_clear['rotspd','nanmean']*0.10471)**3
        
        ########
        rho_temp = np.nanmean(df_all_clear['rho'])
        if((['rotspdzz'] in Df_all.columns.values)==False):
            Df_all['rotspdzz'] = Df_all['rotspd']                
        temp = Df_all[(Df_all['pwrat']>0.0)&(Df_all['rotspd']>Rotspd_Connect*0.9)&(Df_all['rotspdzz']>0.5)]
        temp['gearsscale'] = temp['rotspd'] / temp['rotspdzz']
        chilun = np.nanmean(temp['gearsscale'])
        
        temp = df_all_clear[(df_all_clear['rotspd','nanmean']>=1.05*Rotspd_Connect)&(df_all_clear['rotspd','nanmean']<=0.95*Rotspd_Rate)]
        turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'kopt'] = np.nanmean(temp['kopt'])
        Cp = np.nanmean(temp['cp'])
        
        #######按如下方法计算设计KOPT和风速、空气密度、叶轮直径均无关系，需要知道整机厂家设计时的Cp和叶尖速比
        temp = df_all_clear[(df_all_clear['rotspd','nanmean']>=0.95*0.5*(Rotspd_Connect+Rotspd_Rate))&(df_all_clear['rotspd','nanmean']<=1.05*0.5*(Rotspd_Connect+Rotspd_Rate))]
        lamde = rotor_radius*np.nanmean(temp['rotspd','nanmean'])*0.1047/np.nanmean(temp['wspd','nanmean'])/chilun
        turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'koptsheji'] = 0.5*3.14*rho_temp*Cp*(rotor_radius**5)/(lamde**3)/(chilun**3)
        turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Cp'] = Cp
        turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'lamde'] = lamde
        #########通用图形绘制###########
        state_temp = pd.Series((np.unique(Df_all_m['state','mymode'])))
        state_temp = state_temp.dropna()
        camp20 = plt.get_cmap('tab20')
        fig = plt.figure(figsize=(10,8),dpi=100)  
        #fig = plt.xkcd()#扭曲卡通风格
        plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):
            #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')
            for ic in range(len(state_temp)):
                temp = Df_all_m[Df_all_m['state','mymode']==state_temp[ic]]
                plt.scatter(temp['wspd','nanmean'],temp['pwrat','nanmean'],color=camp20(ic),s=55,alpha=1,label=state_temp[ic])
            plt.grid()
            #plt.xlim(0,25)
            #plt.ylim(0,Pwrat_Rate*1.1)
            plt.xlabel('风速(m/s)',fontsize=14)
            plt.ylabel('功率(kW)',fontsize=14)
            plt.legend(loc=0,fontsize=14)
        fig.savefig(path + '/' +str(turbine_name) + '_fengsu_gonglv_state.png', transparent=True,dpi=100)
        
        state_temp = pd.Series((np.unique(Df_all_m['statety','mymode'])))
        state_temp = state_temp.dropna()
        fig = plt.figure(figsize=(10,8),dpi=100)  
        #fig = plt.xkcd()#扭曲卡通风格
        plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):
            #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')
            for ic in range(len(state_temp)):
                temp = Df_all_m[Df_all_m['statety','mymode']==state_temp[ic]]
                plt.scatter(temp['wspd','nanmean'],temp['pwrat','nanmean'],color=camp20(ic),s=55,alpha=1,label=state_temp[ic])
            plt.grid()
            #plt.xlim(0,25)
            #plt.ylim(0,Pwrat_Rate*1.1)
            plt.xlabel('风速(m/s)',fontsize=14)
            plt.ylabel('功率(kW)',fontsize=14)
            plt.legend(loc=0,fontsize=14)
        fig.savefig(path + '/' +str(turbine_name) + '_fengsu_gonglv_statety.png', transparent=True,dpi=100)
        
        
        fig = plt.figure(figsize=(10,8),dpi=100)  
        plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):  
            #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')          
            plt.scatter(Df_all_m_clear['wspd','nanmean'],Df_all_m_clear['pwrat','nanmean'],c=Df_all_m_clear['clear'],cmap='jet',s=15,alpha=1)
            plt.grid()
            plt.xlim(0,25)
            plt.ylim(0,Pwrat_Rate*1.2)
            plt.xlabel('风速(m/s)',fontsize=14)
            plt.ylabel('功率(kW)',fontsize=14)
            plt.colorbar()
        fig.savefig(path + '/' +str(turbine_name) + '_fengsu_gonglv_clear.png', transparent=True,dpi=100)
        
        
        fig = plt.figure(figsize=(10,8),dpi=100)  
        plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):  
            #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')          
            plt.scatter(Df_all_m_clear['wspd','nanmean'],Df_all_m_clear['pwrat','nanmean'],c=Df_all_m_clear.index.month,cmap='jet',s=15,alpha=1)
            plt.grid()
            plt.xlim(0,25)
            plt.xlabel('风速(m/s)',fontsize=14)
            plt.ylabel('功率(kW)',fontsize=14)
            plt.colorbar(label='月份',ticks=np.unique(Df_all_m_clear.index.month))
        fig.savefig(path + '/' +str(turbine_name) + '_fengsu_gonglv_time.png', transparent=True,dpi=100)
        
        
        fig = plt.figure(figsize=(10,8),dpi=100)  
        plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):  
            #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')          
            plt.scatter(Df_all_m_clear['rotspd','nanmean']*1,Df_all_m_clear['pwrat','nanmean'],c=Df_all_m_clear['clear'],cmap='jet',s=15,alpha=1)
            plt.grid()
            #plt.xlim(Rotspd_Connect*0.8,Rotspd_Rate*1.1)
            plt.ylim(0,Pwrat_Rate*1.2)
            plt.xlabel('转速(rpm)',fontsize=14)
            plt.ylabel('功率(kW)',fontsize=14)
            plt.colorbar()
        fig.savefig(path + '/' +str(turbine_name) + '_zhuansu_gonglv_clear.png', transparent=True,dpi=100)
        
        fig = plt.figure(figsize=(10,8),dpi=100)  
        plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):  
            #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')          
            plt.scatter(Df_all_m_clear['rotspd','nanmean']*1,Df_all_m_clear['pwrat','nanmean'],c=Df_all_m_clear.index.month,cmap='jet',s=15,alpha=1)
            plt.grid()
            plt.xlim(Rotspd_Connect*0.8,Rotspd_Rate*1.1)
            plt.ylim(0,Pwrat_Rate*1.2)
            plt.xlabel('转速(rpm)',fontsize=14)
            plt.ylabel('功率(kW)',fontsize=14)
            plt.colorbar(label='月份',ticks=np.unique(Df_all_m_clear.index.month))
        fig.savefig(path + '/' +str(turbine_name) + '_zhuansu_gonglv_time.png', transparent=True,dpi=100)
                    
        
        fig = plt.figure(figsize=(10,8),dpi=100)  
        plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):  
            #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')          
            plt.scatter(Df_all_m_clear['pwrat','nanmean'],Df_all_m_clear['pitch1','nanmean'],c=Df_all_m_clear['clear'],cmap='jet',s=15,alpha=1)
            plt.plot(Df_all_m_clear['pwrat','nanmean'],Df_all_m_clear['pitchlim'],'o',color='red',markersize=2,alpha=0.8)
            plt.grid()
            plt.xlim(0,Pwrat_Rate*1.2)
            plt.ylim(-2,25)
            plt.xlabel('功率(kW)',fontsize=14)
            plt.ylabel('桨距角(°)',fontsize=14)
            plt.colorbar()
        fig.savefig(path + '/' +str(turbine_name) + '_gonglv_pitch_clear.png', transparent=True,dpi=100)
        
        fig = plt.figure(figsize=(10,8),dpi=100)  
        plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):  
            #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')          
            plt.scatter(Df_all_m_clear['pwrat','nanmean'],Df_all_m_clear['pitch1','nanmean'],c=Df_all_m_clear.index.month,cmap='jet',s=15,alpha=1)
            plt.grid()
            plt.ylim(-2,25)
            plt.xlabel('功率(kW)',fontsize=14)
            plt.ylabel('桨距角(°)',fontsize=14)
            plt.colorbar(label='月份',ticks=np.unique(Df_all_m_clear.index.month))
        fig.savefig(path + '/' +str(turbine_name) + '_gonglv_pitch_time.png', transparent=True,dpi=100)
        
        fig = plt.figure(figsize=(10,8),dpi=100)  
        plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):  
            #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')          
            plt.scatter(Df_all_m_clear['wspd','nanmean'],Df_all_m_clear['pitch1','nanmean'],c=Df_all_m_clear['clear'],cmap='jet',s=15,alpha=1)
            plt.grid()
            plt.ylim(-2,25)
            plt.xlabel('风速(m/s)',fontsize=14)
            plt.ylabel('桨距角(°)',fontsize=14)
            plt.colorbar()
        fig.savefig(path + '/' +str(turbine_name) + '_wspd_pitch_clear.png', transparent=True,dpi=100)
        
        fig = plt.figure(figsize=(10,8),dpi=100)  
        plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):  
            #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')          
            plt.scatter(Df_all['wspd'],Df_all['pitch1'],color='red',s=15,alpha=1)
            plt.scatter(Df_all['wspd'],Df_all['pitch2'],color='g',s=15,alpha=0.5)
            plt.scatter(Df_all['wspd'],Df_all['pitch3'],color='cornflowerblue',s=15,alpha=0.2)
            plt.grid()
            plt.ylim(-2,25)
            plt.xlabel('风速(m/s)',fontsize=14)
            plt.ylabel('桨距角(°)',fontsize=14)
        fig.savefig(path + '/' +str(turbine_name) + '_wspd_pitch_clear123.png', transparent=True,dpi=100)
        
        fig = plt.figure(figsize=(10,8),dpi=100)  
        plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):  
            #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')          
            plt.scatter(Df_all_m_clear['wspd','nanmean'],Df_all_m_clear['pitch1','nanmean'],c=Df_all_m_clear.index.month,cmap='jet',s=15,alpha=1)
            plt.grid()
            plt.ylim(-2,25)
            plt.xlabel('风速(m/s)',fontsize=14)
            plt.ylabel('桨距角(°)',fontsize=14)
            plt.colorbar(label='月份',ticks=np.unique(Df_all_m_clear.index.month))
        fig.savefig(path + '/' +str(turbine_name) + '_wspd_pitch_time.png', transparent=True,dpi=100)
        
        ###机组Cp值展示
        turbine_cp = turbine_efficiency_function.turbine_Cp(df_all_clear, windbin, num)
        turbine_cp.rename(columns = {'cp':turbine_name},inplace = True)
        turbine_cp_all = pd.merge(turbine_cp_all,turbine_cp,how='outer',on='windbin')
        
        ###机组自身限功率,90001包括电网限电、中控限功率（风场发现机组状况不良主动限功率运行）
        ###图形不显示
        mycolor = ['blue','tomato']
        mylabels = [90002,90001]            
        #temp = Df_all_m[(Df_all_m['statel']==5)&((Df_all_m['clear']==8)|(Df_all_m['clear']==7)|(Df_all_m['clear']==6))]            
        temp = Df_all_m[(Df_all_m['statel','nanmean']==90002)|(Df_all_m['statel','nanmean']==90001)]
        temp['groupp'] = temp['statel','nanmean'].map({90002:'blue',90001:'tomato'})
        fig = plt.figure(figsize=(10,8),dpi=100)  
        plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):  
            plt.scatter(temp['wspd','nanmean'],temp['pwrat','nanmean'],c=temp['groupp'],cmap='jet',s=15,alpha=1)
            plt.grid()
            plt.xlim(0,20)
            plt.ylim(0,Pwrat_Rate*1.2)
            plt.xlabel('风速(m/s)',fontsize=14)
            plt.ylabel('功率(kW)',fontsize=14)
            for k in range(len(mycolor)):
                plt.scatter([],[],c=mycolor[k],label=mylabels[k]) 
            plt.legend(loc=4,fontsize=10)
        fig.savefig(path + '/' +str(turbine_name) + '_90002——90001.png', transparent=True,dpi=100)     
        
        
        ###########分段统计#########
        (data_fenduan,fenduan) = turbine_efficiency_function.FenDuan(df_all_clear,Pwrat_Rate,Rotspd_Connect,Rotspd_Rate,turbine_name)
        fenduan_all = pd.concat([fenduan_all, fenduan])#.append(fenduan)
        
        ###图形不展示
        fig = plt.figure(figsize=(10,8),dpi=100)  
        plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):  
            #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')          
            plt.scatter(data_fenduan['rotspd','nanmean'],data_fenduan['pwrat','nanmean'],c=data_fenduan['labelfen'],cmap='jet',s=15,alpha=1)
            plt.grid()
            plt.xlim(Rotspd_Connect*0.8,Rotspd_Rate*1.1)
            plt.ylim(0,Pwrat_Rate*1.1)
            plt.xlabel('转速',fontsize=14)
            plt.ylabel('功率',fontsize=14)
            plt.colorbar()
        fig.savefig(path + '/' +str(turbine_name) + '_分段1.png',dpi=100,transparent=True, bbox_inches='tight')
        
        fig = plt.figure(figsize=(10,8),dpi=100)  
        plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):  
            #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')          
            plt.scatter(data_fenduan['wspd','nanmean'],data_fenduan['pwrat','nanmean'],c=data_fenduan['labelfen'],cmap='jet',s=15,alpha=1)
            plt.grid()
            #plt.ylim(-2,25)
            plt.xlabel('风速',fontsize=14)
            plt.ylabel('功率',fontsize=14)
            plt.colorbar()
        fig.savefig(path + '/' +str(turbine_name) + '_分段2.png', transparent=True,dpi=100, bbox_inches='tight')
        
        #额定功率异常
        if turbine_efficiency_function.Pwrat_Rate_loss(Df_all_m_clear,Pwrat_Rate):
            turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'power_rate_err'] = 1
            temp = Df_all_m_clear[Df_all_m_clear['clear','']<=8]
            fig = plt.figure(figsize=(10,8),dpi=100)  
            plt.title(str(turbine_name))    
            with plt.style.context('ggplot'):  
                plt.scatter(temp['wspd','nanmean'],temp['pwrat','nanmean'],c=temp.index.month,cmap='jet',s=15)
                plt.grid()
                #plt.ylim(-3,25)
                plt.xlabel('风速(m/s)',fontsize=20)
                plt.ylabel('功率(kW)',fontsize=20)
                plt.colorbar(label='月份',ticks=np.unique(temp.index.month))
                plt.tick_params(which='both',labelcolor='#869AA7', width=0,color='#869AA7', labelsize=20,gridOn=True,grid_color='#869AA7',direction ='in',right=True)
                plt.gca().spines["left"].set_color('#869AA7')
                plt.gca().spines["bottom"].set_color('#869AA7')
                plt.gca().spines["right"].set_color('#869AA7')
                plt.gca().spines["top"].set_color('#869AA7')
            fig.savefig(path + '/' +str(turbine_name) + '_额定功率异常_时间.png',dpi=100, transparent=True, bbox_inches='tight')
        
        #####变桨控制分析##########
        #桨距角不平衡
        if turbine_efficiency_function.Pitch_Nobalance_loss(df_all_clear,Df_all):
            turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'pitch_balance_err'] = 1
            fig = plt.figure(figsize=(10,8),dpi=100)  
            plt.title(str(turbine_name),fontsize=20,color='#869AA7')    
            with plt.style.context('ggplot'):  
                plt.scatter(df_all_clear['wspd','nanmean'],df_all_clear['pitch1','nanmean'],color='green',s=10)         
                plt.scatter(df_all_clear['wspd','nanmean'],df_all_clear['pitch2','nanmean'],color='yellow',s=10)
                plt.scatter(df_all_clear['wspd','nanmean'],df_all_clear['pitch3','nanmean'],color='red',s=10)
                plt.grid()
                plt.xlim(0.001,20)
                plt.ylim(-3,20)
                plt.xlabel('风速(m/s)',fontsize=20, color='#869AA7')
                plt.ylabel('桨距角(°)',fontsize=20, color='#869AA7')
                plt.tick_params(which='both',labelcolor='#869AA7', width=0,color='#869AA7', labelsize=20,gridOn=True,grid_color='#869AA7',direction ='in',right=True)
                plt.gca().spines["left"].set_color('#869AA7')
                plt.gca().spines["bottom"].set_color('#869AA7')
                plt.gca().spines["right"].set_color('#869AA7')
                plt.gca().spines["top"].set_color('#869AA7')
            fig.savefig(path + '/' +str(turbine_name) + '_pitch_balance_err.png',dpi=100, transparent=True, bbox_inches='tight')

        
        #最小桨距角异常data=all，输出内容：变桨控制分析：最小桨距角异常
        #Df_all_m_clear[Df_all_m_clear['clear'] == 2]
        if turbine_efficiency_function.Pitch_Min_loss(Df_all_m_clear[Df_all_m_clear['clear'] <= 7],Pitch_Min,Pwrat_Rate,Rotspd_Connect) > 0:
            turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'pitch_min_err'] = 1
            
            df_all_clear_pitch_min = Df_all_m_clear[Df_all_m_clear['clear'] <= 7]
            fig = plt.figure(figsize=(10,8),dpi=100)  
            plt.title(str(turbine_name), color='#ccc')    
            with plt.style.context('ggplot'):  
                plt.scatter(df_all_clear_pitch_min['wspd','nanmean'],df_all_clear_pitch_min['pitch1','nanmean'],color='red',s=15)
                plt.grid()
                plt.ylim(-2,25)
                plt.xlabel('风速(m/s)',fontsize=20, color='#ccc')
                plt.ylabel('桨距角(°)',fontsize=20, color='#ccc')
                plt.tick_params(which='both',labelcolor='#ccc', width=0,color='#426977', labelsize=20,gridOn=True,grid_color='#426977',direction ='in',right=True)
                plt.gca().spines["left"].set_color('#426977')
                plt.gca().spines["bottom"].set_color('#426977')
                plt.gca().spines["right"].set_color('#426977')
                plt.gca().spines["top"].set_color('#426977')
            fig.savefig(path + '/' +str(turbine_name) + '_最小桨距角异常.png',dpi=100, transparent=True, bbox_inches='tight')
            
            ########################################################
            #上传图片到minio
            picture_path = path + '/' +str(turbine_name) + '_最小桨距角异常.png'
            url_picture = upload(picture_path, algorithms_configs)
            #mysql记录
            insertPitchAnglePicture(algorithms_configs, url_picture, turbine_name)
            ########################################################
            
        #变桨控制异常, 输出内容：变桨控制分析：桨变动作异常
        if (turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'pitch_min_err'].values == 0):
            df_all_clear_temp = df_all_clear[df_all_clear['wspd','nanmean']<15]
            (curve_area,x_test,y_pred) = turbine_efficiency_function.Pitch_Control_loss(df_all_clear_temp,Pitch_Min,Pwrat_Rate,Rotspd_Connect,Rotspd_Rate,mytol=0.1,myreg=0.02)
            if (curve_area > 0)&(len(df_all_clear_temp[(df_all_clear_temp['pwrat','nanmean']>=0.6*Pwrat_Rate)&(df_all_clear_temp['pwrat','nanmean']<=0.95*Pwrat_Rate)])>50):
                turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'pitch_control_err'] = 1
                fig = plt.figure(figsize=(10,8),dpi=100)  
                plt.title(str(turbine_name), color='#ccc')    
                with plt.style.context('ggplot'):  
                    #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')          
                    plt.scatter(df_all_clear['pwrat','nanmean'],df_all_clear['pitch1','nanmean'],color='cornflowerblue',s=10,alpha=1)
                    plt.plot(x_test.reshape(-1,1)*Pwrat_Rate,y_pred,color='red',linewidth=2)
                    plt.grid()
                    plt.ylim(-2,25)
                    plt.xlabel('功率(kW)',fontsize=20, color='#ccc')
                    plt.ylabel('桨距角(°)',fontsize=20, color='#ccc')
                    plt.tick_params(which='both',labelcolor='#ccc', width=0,color='#426977', labelsize=20,gridOn=True,grid_color='#426977',direction ='in',right=True)
                    plt.gca().spines["left"].set_color('#426977')
                    plt.gca().spines["bottom"].set_color('#426977')
                    plt.gca().spines["right"].set_color('#426977')
                    plt.gca().spines["top"].set_color('#426977')
                fig.savefig(path + '/' +str(turbine_name) + '_pitch_control_err.png',dpi=100, transparent=True, bbox_inches='tight')
                ########################################################
                #上传图片到minio
                picture_path = path + '/' +str(turbine_name) + '_pitch_control_err.png'
                url_picture = upload(picture_path, algorithms_configs)
                #mysql记录
                insertPitchActionPicture(algorithms_configs, url_picture, turbine_name)
                ########################################################
                
        ###########转矩控制分析############
        #最佳Cp段转矩控制异常异常, 输出内容：变桨控制分析：转矩控制异常
        if (turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'power_rate_err'].values == 0):
            (kopt_err,data_temp) = turbine_efficiency_function.Torque_Cp_kopt_loss(df_all_clear,Pwrat_Rate,Rotspd_Connect,Rotspd_Rate,mytol=0.1,myreg=0.02)
            if (kopt_err==1):
                turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'torque_kopt_err'] = 1

                cp0 = np.nanmean(data_temp[data_temp['kopt_err']==0]['cp'])
                cp1 = np.nanmean(data_temp[data_temp['kopt_err']==1]['cp'])
                if (cp0 != np.nan)&(cp1 != np.nan):
                    turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'torque_kopt_loss'] = 0.1*np.abs(cp0 - cp1) / np.nanmax([cp0,cp1])
                else:
                    turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'torque_kopt_loss'] = -999999
                #转矩控制异常图形输出:输出内容：转矩控制异常
                if len(data_temp[data_temp['kopt_err']==0])>=len(data_temp[data_temp['kopt_err']==1]):
                    data_temp['group'] = data_temp['kopt_err'].map({0:'cornflowerblue',1:'red'})
                else:
                    data_temp['group'] = data_temp['kopt_err'].map({1:'cornflowerblue',0:'red'})
                fig = plt.figure(figsize=(10,8),dpi=100)  
                plt.title(str(turbine_name), color='#ccc')    
                with plt.style.context('ggplot'):  
                    plt.scatter(data_temp['rotspd','nanmean'],data_temp['pwrat','nanmean'],c=data_temp['group'],s=15)
                    plt.grid()
                    #plt.ylim(-3,25)
                    plt.xlabel('转速(rpm)',fontsize=20, color='#ccc')
                    plt.ylabel('功率(kW)',fontsize=20, color='#ccc')
                    plt.tick_params(which='both',labelcolor='#ccc', width=0,color='#426977', labelsize=20,gridOn=True,grid_color='#426977',direction ='in',right=True)
                    plt.gca().spines["left"].set_color('#426977')
                    plt.gca().spines["bottom"].set_color('#426977')
                    plt.gca().spines["right"].set_color('#426977')
                    plt.gca().spines["top"].set_color('#426977')
                fig.savefig(path + '/' +str(turbine_name) + '_转矩kopt控制异常.png',dpi=100, transparent=True, bbox_inches='tight')
                ########################################################
                #上传图片到minio
                picture_path = path + '/' +str(turbine_name) + '_转矩kopt控制异常.png'
                url_picture = upload(picture_path, algorithms_configs)
                #mysql记录
                insertTorqueControlPicture(algorithms_configs, url_picture, turbine_name)
                ########################################################
                ##界面不展示
                fig = plt.figure(figsize=(10,8),dpi=100)  
                plt.title(str(turbine_name))    
                with plt.style.context('ggplot'):  
                    plt.scatter(data_temp['wspd','nanmean'],data_temp['pwrat','nanmean'],c=data_temp['kopt_err'],cmap='jet',s=15)
                    plt.grid()
                    #plt.ylim(-3,25)
                    plt.xlabel('风速(wspd)',fontsize=14)
                    plt.ylabel('功率(kW)',fontsize=14)
                    #plt.colorbar()
                fig.savefig(path + '/' +str(turbine_name) + '_torqueerr_风速_功率.png', transparent=True,dpi=100)
                
                ##界面不展示
                fig = plt.figure(figsize=(10,8),dpi=100)  
                plt.title(str(turbine_name))    
                with plt.style.context('ggplot'):  
                    plt.scatter(data_temp['rotspd','nanmean'],data_temp['pwrat','nanmean'],c=data_temp.index.month,cmap='jet',s=15)
                    #plt.scatter(df_all_clear['rotspd','nanmean'],df_all_clear['pwrat','nanmean'],c=df_all_clear['kopt_err'],cmap='jet',s=15)
                    plt.grid()
                    #plt.ylim(-3,25)
                    plt.xlabel('转速(rpm)',fontsize=14)
                    plt.ylabel('功率(kW)',fontsize=14)
                    plt.colorbar(label='月份',ticks=np.unique(data_temp.index.month))
                fig.savefig(path + '/' +str(turbine_name) + '_torqueerr_时间.png', transparent=True,dpi=100)
            
            #额定功率段转矩控制异常异常
            else:
                (rate_kopt_err,rotspd_power_nihe) = turbine_efficiency_function.Torque_Rotspd_Rate_loss(df_all_clear,Pwrat_Rate,Rotspd_Rate,Rotspd_Connect)
                if (rate_kopt_err == 1):
                    turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'torque_rate_err'] = 1
                    
                    fig = plt.figure(figsize=(10,8),dpi=100)  
                    plt.title(str(turbine_name))    
                    with plt.style.context('ggplot'):  
                        plt.scatter(df_all_clear['rotspd','nanmean'],df_all_clear['pwrat','nanmean'],color='cornflowerblue',s=15)
                        plt.plot(rotspd_power_nihe['rotspd'],rotspd_power_nihe['pwrat'],color='darkorange',markersize=3,linewidth=3.0)
                        plt.grid()
                        #plt.ylim(-3,25)
                        plt.xlabel('转速(rpm)',fontsize=20)
                        plt.ylabel('功率(kW)',fontsize=20)
                        plt.tick_params(which='both',labelcolor='#869AA7', width=0,color='#869AA7', labelsize=20,gridOn=True,grid_color='#869AA7',direction ='in',right=True)
                        plt.gca().spines["left"].set_color('#869AA7')
                        plt.gca().spines["bottom"].set_color('#869AA7')
                        plt.gca().spines["right"].set_color('#869AA7')
                        plt.gca().spines["top"].set_color('#869AA7')
                    fig.savefig(path + '/' +str(turbine_name) + '_额定转速段转矩控制异常.png',dpi=100, transparent=True, bbox_inches='tight')
        
        ###########偏航控制分析############
        #偏航控制误差过大，输出内容：控制分析：偏航控制误差偏大
        
        leiji = turbine_efficiency_function.Yaw_Control_loss(df_all_clear)
        if leiji.iloc[-1,1] < 0.8:
            turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'yaw_leiji_err'] = 1
            
        fig, ax = plt.subplots(figsize=(10, 8), dpi=100)
        plt.title(str(turbine_name),fontsize=20,color='#ccc') 
        wdir_temp = df_all_clear[np.abs(df_all_clear['wdir0','nanmean']<=40)]
        #wdir_temp['wdir0','nanmean'].plot(kind = 'kde',label = '密度图',color='black')
        ax.hist(df_all_clear['wdir0','nanmean'],bins=40,alpha=1,density=True,cumulative=False,histtype='bar',color='cornflowerblue',range = (-40,40))
        ax.set_xlabel('偏航误差角度',fontsize=20, color='#ccc')
        ax.set_ylabel('频率分布',fontsize=20, color='#ccc')
        ax.tick_params(which='both',labelcolor='#ccc', width=0,color='#426977', labelsize=20,gridOn=True,grid_color='#426977',direction ='in',right=True)
        ax1 = ax.twinx()
        ax1.plot(leiji['yaw'],leiji['leiji'],'-',color='darkorange',markersize=3,linewidth=1.5)
        ax1.set_ylabel('偏航误差±10度累计概率',fontsize=20, color='#ccc')
        ax1.tick_params(which='both',labelcolor='#ccc', width=0,color='#426977', labelsize=20,gridOn=False,grid_color='#426977',direction ='in',right=True)
        ax1.set_ylim(0,1)
        plt.gca().spines["left"].set_color('#426977')
        plt.gca().spines["bottom"].set_color('#426977')
        plt.gca().spines["right"].set_color('#426977')
        plt.gca().spines["top"].set_color('#426977')
        plt.gca().set_xlim(-40,40)
        fig.savefig(path + '/' +str(turbine_name) + '_wdir0.png',dpi=100, transparent=True, bbox_inches='tight')
        ########################################################
        #上传图片到minio
        picture_path = path + '/' +str(turbine_name) + '_wdir0.png'
        url_picture = upload(picture_path, algorithms_configs)
        #mysql记录
        insertNavigationBiasControlPicture(algorithms_configs, url_picture, turbine_name)
        ########################################################
        #偏航对风误差计算，额定功率异常不计算, 输出内容：控制分析：偏航对风误差偏大
        if (turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'power_rate_err'].values == 0):
            
        ###测试           
        #dirbin = np.arange(-20.0,20.0,0.2)
        #dirbin1 = np.arange(-20.1,20.1,0.2) 
        #windbin1 = np.arange(3.75,8.25,0.5)
        #windbin2 = np.arange(4.0,8.0,0.5)

        #for num in range(len(Turbine_attr_type)):
            #print(turbine_name)
            #turbine_name = wtids[num]
            #Df_all = Df_all_all[Df_all_all['wtid'] == turbine_name]
            #Df_all['wdir0'] = Df_all['wdir'] - Df_all['yaw']
            #Df_all['wdir0'] = Df_all['wdir'] - 180.0
            #Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
            #Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir0'],0.2,9,11,0.3,1,1)#1:低通滤波器，2：中值滤波，3：wiener滤波，4：巴特沃斯滤波器
            
            try:
                restart = True
                request_num = 0
                medfilt_num = 1
                filter1st_tao = 0.3
                choose_num = 2
                while restart and request_num < 8:
                    if request_num == 0:
                        res_df = turbine_efficiency_function.data_clear(Df_all,Pitch_Min,Pwrat_Rate,Rotspd_Rate,Rotspd_Connect,rotor_radius,altitude,state)#剔除数据后数据量太少无法有效拟合
                        if len(res_df)<100:
                            res_df = Df_all
                        #res_df = data
                        err_result = turbine_efficiency_function.winddir_err_before_new(res_df,dirbin,windbin2,dirbin1,windbin1,path,turbine_name,request_num)
                    if err_result == -999999:
                        Df_all = Df_all_all[Df_all_all['wtid'] == turbine_name]

                        ###远景机组
                        if ManufacturerID=='EN':
                            #Df_all['wdir0'] = signal.medfilt(Df_all['wdir0'],5)
                            Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
                            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir0'],filter1st_tao,medfilt_num,11,0.1,1,choose_num)
                            #Df_all['wdir0'] = signal.medfilt(Df_all['wdir'],5)
                            #Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean']           

                        ##金风机组机组####
                        if ManufacturerID=='GW':
                            if((['wdir0'] in Df_all.columns.values)==False)&((['wdir25'] in Df_all.columns.values)==True):
                                Df_all['wdir0'] = Df_all['wdir25'] - 180.0
                                Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
                                Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir0'],filter1st_tao,medfilt_num,11,0.1,1,choose_num)
                            elif((['wdir0'] in Df_all.columns.values)==False)&((['wdir'] in Df_all.columns.values)==True):
                                Df_all['wdir0'] = Df_all['wdir'] - 180.0
                                Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
                                Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir0'],filter1st_tao,medfilt_num,11,0.1,1,choose_num)#1:低通滤波器，2：中值滤波，3：wiener滤波，4：巴特沃斯滤波器
                            elif((['wdir0'] in Df_all.columns.values)==False)&((['wdir'] in Df_all.columns.values)==False)&((['wdirs'] in Df_all.columns.values)==True):
                                Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdirs'],filter1st_tao,medfilt_num,11,0.1,1,choose_num)
                            elif((['wdir0'] in Df_all.columns.values)==False)&((['wdir'] in Df_all.columns.values)==False)&((['wdirs'] in Df_all.columns.values)==False):
                                Df_all['wdir0'] = 0.0
                            else:
                                Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdirs'],filter1st_tao,medfilt_num,11,0.1,1,choose_num)
                            
                        ##明阳机组####
                        if (ManufacturerID=='MY')|(ManufacturerID=='My'):
                            #Df_all['wdir'] = Df_all['wdir'].fillna(0)
                            #Df_all['wdir0'] = Df_all['wdir'] - Df_all['yaw']
                            Df_all['wdir0'] = Df_all['wdir']
                            Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
                            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir'],filter1st_tao,medfilt_num,11,0.5,1,choose_num)#1:低通滤波器，2：中值滤波，3：wiener滤波，4：巴特沃斯滤波器
                            Df_all_m['wdir0','nanmean'] = Df_all_m['wdir','nanmean']
                                    
                        #####三一机组、湘电机组
                        if (ManufacturerID=='SE')|(ManufacturerID=='SI')|(ManufacturerID=='XE'):
                            if((['wdir0'] in Df_all.columns.values)==False):
                                Df_all['wdir0'] = Df_all['wdir'] - Df_all['yaw']
                                Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
                            else:
                                Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
                                
                            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdirs'],filter1st_tao,medfilt_num,11,0.1,1,choose_num)

                        ###海装机组、运达机组
                        if (ManufacturerID=='H1')|(ManufacturerID=='HZ')|(ManufacturerID=='WD'):
                            Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
                            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir0'],filter1st_tao,medfilt_num,11,0.1,1,choose_num)

                        ###中车机组
                        if ManufacturerID=='WT':
                            if((['wdir0'] in Df_all.columns.values)==False):
                                #Df_all['wdir0'] = Df_all['wdir'] - Df_all['yaw']
                                Df_all['wdir0'] = Df_all['wdir'].fillna(0)
                            else:
                                Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
                                
                            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir'],filter1st_tao,medfilt_num,11,0.1,1,choose_num)

                        ###东气机组
                        if ManufacturerID=='FD':
                            Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
                            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir'],filter1st_tao,medfilt_num,11,0.1,1,choose_num)
                            
                        ###联合动力机组
                        if ManufacturerID=='FD':
                            Df_all['wdir'] = Df_all['wdir'].fillna(0)
                            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir'],filter1st_tao,medfilt_num,11,0.1,1,choose_num)
                            
                        ###太重机组
                        if ManufacturerID=='FD':
                            Df_all['wdir0'] = Df_all['wdir'] - 180.0
                            Df_all['wdir0'] = Df_all['wdir0'].fillna(0)
                            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir'],filter1st_tao,medfilt_num,11,0.1,1,choose_num)
                            
                        ###上气机组,计算处也得修改
                        if (ManufacturerID=='W2')|(ManufacturerID=='W23')|(ManufacturerID=='W4'):
                            Df_all['wdir'] = Df_all['wdir'].fillna(0)
                            Df_all['wdir0'] = turbine_efficiency_function.pass_filter(Df_all['wdir'],filter1st_tao,medfilt_num,11,0.1,1,choose_num)
                            
                        res_df = turbine_efficiency_function.data_clear(Df_all,Pwrat_Rate,Rotspd_Rate,Rotspd_Connect,rotor_radius,altitude,state)#剔除数据后数据量太少无法有效拟合
                        if len(res_df)<100:
                            res_df = Df_all
                        #res_df = data
                        err_result,picture_path = turbine_efficiency_function.winddir_err_before_new(res_df,dirbin,windbin2,dirbin1,windbin1,path,turbine_name,request_num)
                        ########################################################
                        #上传图片到minio
                        url_picture = upload(picture_path, algorithms_configs)
                        #mysql记录
                        insertNavigationBiasDirectionPicture(algorithms_configs, url_picture, turbine_name)
                        ########################################################    
                        medfilt_num = medfilt_num + 2                                                       
                        if request_num >=5:
                            choose_num = 1
                        elif request_num >=6:
                            filter1st_tao = 0.6                                
                        request_num = request_num + 1
                            
                    else:
                        restart = False
                                                    
                err_result_all.loc[num,'turbine'] = turbine_name
                err_result_all.loc[num,'yawerr'] = err_result
                err_result_all.loc[num,'loss'] = (1 - np.cos(3.14159*err_result/180.0)**2)*0.75
                if np.abs(err_result)>=5.0:
                    turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'yaw_duifeng_err'] = err_result
                    turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'yaw_duifeng_loss'] = err_result_all.loc[num,'loss']
            except Exception:
                err_result_all.loc[num,'turbine'] = turbine_name
                err_result_all.loc[num,'yawerr'] = -999999
                err_result_all.loc[num,'loss'] = -999999
            
                
        #风速-功率散点异常离散(额定功率异常的不判别)
        if (turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'power_rate_err'].values == 0):
        #一定比例的散点超出实测功率曲线正负5%
        #data为清洗后的数据，功率曲线不用补全
            pw_df_temp = pw_df_alltime.loc[:,['windbin',turbine_name]]
            pw_df_temp.columns = pd.MultiIndex.from_tuples([('windbin', ''), (turbine_name, '')])
            pw_df_temp = pw_df_temp.dropna()
            df_all_clear['windbin'] = pd.cut(df_all_clear['wspd','nanmean'],windbinreg,labels=windbin)
            # df_all_clear.reset_index(level=0,inplace=True)
            df_all_clear = pd.merge(df_all_clear,pw_df_temp,how='inner',on='windbin')
            df_all_clear.set_index(('localtime',''),inplace= True) 
            (data_temp,pw_df_limit) =  turbine_efficiency_function.Wind_Power_Dissociation(df_all_clear,pw_df_temp,turbine_name)
            if len(data_temp)>0:
                turbine_err_all.loc[turbine_err_all['wtid']==turbine_name,'wspd_power_err'] = 1
                
                fig = plt.figure(figsize=(10,8),dpi=100)  
                plt.title(str(turbine_name))    
                with plt.style.context('ggplot'):  
                    #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')          
                    plt.scatter(data_temp['wspd','nanmean'],data_temp['pwrat','nanmean'],c=data_temp.index.month,cmap='jet',s=15,alpha=1)
                    plt.plot(pw_df_limit['windbin'],pw_df_limit[turbine_name],'-o',color='limegreen',markersize=5,linewidth=2.5)
                    plt.plot(pw_df_limit['windbin'],pw_df_limit['pwrat_up'],'--',color='darkorange',markersize=5,linewidth=2.5)
                    plt.plot(pw_df_limit['windbin'],pw_df_limit['pwrat_down'],'--',color='darkorange',markersize=5,linewidth=2.5)
                    plt.grid()
                    #plt.xlim(0,25)
                    plt.xlabel('风速(m/s)',fontsize=20)
                    plt.ylabel('功率(kW)',fontsize=20)
                    plt.colorbar(label='月份',ticks=np.unique(data_temp.index.month))
                    plt.tick_params(which='both',labelcolor='#869AA7', width=0,color='#869AA7', labelsize=20,gridOn=True,grid_color='#869AA7',direction ='in',right=True)
                    plt.gca().spines["left"].set_color('#869AA7')
                    plt.gca().spines["bottom"].set_color('#869AA7')
                    plt.gca().spines["right"].set_color('#869AA7')
                    plt.gca().spines["top"].set_color('#869AA7')
                fig.savefig(path + '/' +str(turbine_name) + '_风速功率散点异常.png',dpi=100, transparent=True, bbox_inches='tight')
                
        ###发电机转速与叶轮转速不平衡
        #Df_all = Df_all_all[Df_all_all['wtid'] == 'F3']
        if((['rotspdzz'] in Df_all.columns.values)==True):
            temp = Df_all[(Df_all['pwrat']>0.0)&(Df_all['rotspd']>Rotspd_Connect*0.9)&(Df_all['rotspdzz']>0.5)]
            temp['gearsscale'] = temp['rotspd'] / temp['rotspdzz']
            fig = plt.figure(figsize=(10,8),dpi=100)  
            plt.title(str(turbine_name))    
            with plt.style.context('ggplot'):  
                plt.scatter(temp['rotspd'],temp['rotspdzz'],c=temp.index.month,cmap='jet',s=15,alpha=1)
                plt.grid()
                plt.xlim(Rotspd_Connect*0.8,Rotspd_Rate*1.1)
                plt.ylim(5,15)
                plt.ylabel('主轴转速(rpm)',fontsize=14)
                plt.xlabel('发电机转速(rpm)',fontsize=14)
                plt.colorbar(label='月份',ticks=np.unique(temp.index.month))
            fig.savefig(path + '/' +str(turbine_name) + '_主轴_发电机_转速.png', transparent=True,dpi=100, bbox_inches='tight')
                
            #temp['gearsscale'].nlargest(10)
            #aaa = temp.loc[temp['gearsscale'].nsmallest(200).index,:]#nsmallest(20)
            #plt.scatter(aaa['wspd'],aaa['pitch1'])
        
        #except Exception:
            #Df_all_m_clear = Df_all_m
            #df_all_clear = Df_all_m
            #print(str(turbine_name)+'数据异常！！！！！！！！！！！！！！')
            
    #########绘制各机组CP曲线 
    turbine_num = 16
    pnum = len(wtids)//turbine_num
    rem = len(wtids)%turbine_num
    if pnum > 0: 
        rem_pw = turbine_num
        for j_pw in range(pnum+1):
            camp20 = plt.get_cmap('tab20')
            fig = plt.figure(figsize=(10,8),dpi=100)  
            #plt.title(str(turbine_name))    
            with plt.style.context('ggplot'):  
                if j_pw >= pnum:
                    rem_pw = rem
                for i in range(rem_pw):
                    plt.plot(turbine_cp_all['windbin'],turbine_cp_all[wtids[i+j_pw*turbine_num]]/1.1585,'-o',color=camp20(i),markersize=5,label=wtids[i+j_pw*turbine_num])
                    
                plt.grid()
                plt.ylim(0,1)
                plt.xlabel('风速',fontsize=20)
                plt.ylabel('Cp(风能利用系数)',fontsize=20)
                plt.legend(loc=4,fontsize=14)
                plt.tick_params(which='both',labelcolor='#869AA7', width=0,color='#869AA7', labelsize=20,gridOn=True,grid_color='#869AA7',direction ='in',right=True)
                plt.gca().spines["left"].set_color('#869AA7')
                plt.gca().spines["bottom"].set_color('#869AA7')
                plt.gca().spines["right"].set_color('#869AA7')
                plt.gca().spines["top"].set_color('#869AA7')
            fig.savefig(path + '/' +'Cp曲线'+str(j_pw) +'.png',dpi=100, transparent=True, bbox_inches='tight')
    else: 
        camp20 = plt.get_cmap('tab20')
        fig = plt.figure(figsize=(10,8),dpi=100)  
        #plt.title(str(turbine_name))    
        with plt.style.context('ggplot'):  
            #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')          
            for i in range(rem):
                plt.plot(turbine_cp_all['windbin'],turbine_cp_all[wtids[i]]/1.1585,'-o',color=camp20(i),markersize=5,label=wtids[i])
                
            plt.grid()
            plt.ylim(0,1)
            plt.xlabel('风速',fontsize=20)
            plt.ylabel('Cp(风能利用系数)',fontsize=20)
            plt.legend(loc=4,fontsize=14)
            plt.tick_params(which='both',labelcolor='#869AA7', width=0,color='#869AA7', labelsize=20,gridOn=True,grid_color='#869AA7',direction ='in',right=True)
            plt.gca().spines["left"].set_color('#869AA7')
            plt.gca().spines["bottom"].set_color('#869AA7')
            plt.gca().spines["right"].set_color('#869AA7')
            plt.gca().spines["top"].set_color('#869AA7')
        fig.savefig(path + '/' +'Cp曲线'+str(pnum+1) +'.png',dpi=100,transparent=True, bbox_inches='tight')

    #################################################################################
    #全风机类型
    #################################################################################
    if algorithms_configs['typeProcess'] == len(np.unique(algorithms_configs['Turbine_attr']['turbineTypeID'])) - 1:
        #变量赋值
        Df_all_m_all_alltype = algorithms_configs['Df_all_m_all_alltype']
        #全场全机型
        ###全场风频绘制
        wind_freq = turbine_efficiency_function.winddistribute(Df_all_m_all_alltype,windbin,windbinreg)
        wind_max = np.nanmax(Df_all_m_all_alltype['wspd','nanmax'])
        wind_mean = np.nanmean(Df_all_m_all_alltype['wspd','nanmean'])
        
        fig, ax = plt.subplots(figsize=(8, 4), dpi=100)
        ax.bar(wind_freq['windbin'],wind_freq['freq'],color='royalblue',bottom = 0,width=0.2)
        ax.set_xlabel(xlabel='风速(m/s)',fontsize=10, color='#ccc')
        ax.set_ylabel('风频',fontsize=10, color='#ccc')
        ax.tick_params(which='both',labelcolor='#ccc', width=0,color='#426977', labelsize=10,gridOn=False,grid_color='#426977',direction ='in',right=True)   
        fig.savefig(os.path.dirname(path) + '/' + 'windfreq.png', transparent=True,dpi=100,bbox_inches='tight')
        ########################################################
        #上传图片到minio
        picture_path = os.path.dirname(path) + '/' + 'windfreq.png'
        url_picture = upload(picture_path, algorithms_configs)
        #mysql记录
        insertAllWindFrequencyPicture(algorithms_configs, url_picture)
        ########################################################
        
        #全场月平均空气密度及风速
        altitude_farm = np.nanmean(algorithms_configs['zuobiao_all']['Z'])
        month_data, filename = turbine_efficiency_function.monthdata(Df_all_m_all_alltype,altitude_farm,os.path.dirname(path))
        ########################################################
        #上传图片到minio
        picture_path = filename
        url_picture = upload(picture_path, algorithms_configs)
        #mysql记录
        insertAllAirDensityPicture(algorithms_configs, url_picture)
        ########################################################
        

        #全场湍流
        wind_ti_alltype = turbine_efficiency_function.wind_ti(Df_all_m_all_alltype, windbin, 6)
        fig = plt.figure(figsize=(10,8),dpi=100)  
        with plt.style.context('ggplot'):  
            plt.plot(wind_ti_alltype['windbin'],wind_ti_alltype['ti'],'-o',color='cornflowerblue',markersize=5)           
            plt.grid()
            #plt.ylim(-2,25)
            plt.xlabel('风速(m/s)',fontsize=20, color='#ccc')
            plt.ylabel('湍流',fontsize=20, color='#ccc')
            plt.tick_params(which='both',labelcolor='#ccc', width=0,color='#426977', labelsize=20,gridOn=True,grid_color='#426977',direction ='in',right=True)
            plt.gca().spines["left"].set_color('#426977')
            plt.gca().spines["bottom"].set_color('#426977')
            plt.gca().spines["right"].set_color('#426977')
            plt.gca().spines["top"].set_color('#426977')
        fig.savefig(os.path.dirname(path) + '/' +'湍流曲线'+'.png',dpi=100, transparent=True, bbox_inches='tight')
        ########################################################
        #上传图片到minio
        picture_path = os.path.dirname(path) + '/' +'湍流曲线'+'.png'
        url_picture = upload(picture_path, algorithms_configs)
        #mysql记录
        insertAllTurbulencePicture(algorithms_configs, url_picture)
        ########################################################