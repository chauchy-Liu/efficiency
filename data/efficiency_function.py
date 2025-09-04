import pandas as pd
from pylab import mpl
import numpy as np
import statistics as st
from scipy import signal,integrate
import matplotlib.pyplot as plt
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression,Ridge
from sklearn import preprocessing
#import matplotlib.cm as cm
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.mixture import GaussianMixture
from sklearn.mixture import BayesianGaussianMixture
from sklearn.cluster import DBSCAN,SpectralClustering,AgglomerativeClustering
from scipy.stats import sem    
from sklearn.cluster import OPTICS  
#from sklearn.covariance import EllipticEnvelope
from sklearn.svm import OneClassSVM, LinearSVR, SVR
from sklearn.tree import DecisionTreeRegressor
import matplotlib.cm as cm
from sklearn.ensemble import GradientBoostingRegressor
import xgboost as xgb
from sklearn.metrics import accuracy_score
from utils.display_util import get_os
from configs.config import algConfig

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


def thresholdfun_orig(data,threshold):
    temp_all = pd.DataFrame()    
    wind_bin = np.arange(2.0,np.ceil(np.nanmax(data['wspd'])),0.5)
    for m in range(len(wind_bin)):
        temp = data[(data['wspd']>=wind_bin[m]-0.25) & (data['wspd']<wind_bin[m]+0.25)]
        pwrat_mean = np.mean(temp['pwrat'])
        pwrat_std = np.std(temp['pwrat'])
        temp_new = temp[((temp['pwrat']-pwrat_mean)/pwrat_std < threshold) & ((temp['pwrat']-pwrat_mean)/pwrat_std > -threshold)]
        temp_all = pd.concat([temp_all,temp_new])#.append(temp_new)
    return temp_all

def thresholdfun_orig1(data,neighbors_num=50):
    X_train = pd.DataFrame()
    X_train['pwrat'] = data['pwrat']
    X_train['pitch'] = data['pitch1']    
    clf = LocalOutlierFactor(n_neighbors=neighbors_num,p=1,n_jobs=-1)
    #clf = IsolationForest(n_estimators=50,contamination=0.25,random_state=1)
    y_pred = clf.fit_predict(X_train)
    data['y_pred'] = y_pred
    temp_all = data[data['y_pred']==1]
    return temp_all

def pwratcurve_rho_alltime(data,windbin,num):
    pc_df_rho = pd.DataFrame()
    for i in range(len(windbin)):
        temp = data[(data['wspd_rho']>=windbin[i]-0.25) & (data['wspd_rho']<windbin[i]+0.25)]
        if len(temp) >= num:
            pc_df_rho.loc[i,'windbin'] = windbin[i]
            pc_df_rho.loc[i,'pwrat'] = np.mean(temp['pwrat','nanmean'])
            pc_df_rho.loc[i,'wspd_mean'] = np.mean(temp['wspd_rho'])
            pc_df_rho.loc[i,'rotspd_mean'] = np.mean(temp['rotspd','nanmean'])
            pc_df_rho.loc[i,'pwrat_std'] = np.std(temp['pwrat','nanmean'])   
            pc_df_rho.loc[i,'count'] = len(temp)
        #pc_df = pc_df.append(pc_df)
    return pc_df_rho


def pwratcurve_rho(data,turbine_type,turbine_name,pw_startTime,pw_endTime,windbin,num):
    pc_df_rho = pd.DataFrame()
    
    data_temp = data[data['type']==turbine_type]
    data_temp = data_temp.loc[(data_temp.index>=pw_startTime) & (data_temp.index<=pw_endTime),[turbine_name,str(turbine_name+'_'+'wspd')]]
    data_temp.rename(columns = {turbine_name:'pwrat',str(turbine_name+'_'+'wspd'):'wspd_rho'},inplace = True) 
    for i in range(len(windbin)):
        temp = data_temp[(data_temp['wspd_rho']>=windbin[i]-0.25) & (data_temp['wspd_rho']<windbin[i]+0.25)]
        if len(temp) >= num:
            pc_df_rho.loc[i,'windbin'] = windbin[i]
            pc_df_rho.loc[i,'pwrat'] = np.mean(temp['pwrat'])
            pc_df_rho.loc[i,'count'] = len(temp)
        #pc_df = pc_df.append(pc_df)
    return pc_df_rho

def pwrat_wind(data,pwratbin,num):
    pwrat_wind = pd.DataFrame()
    for i in range(len(pwratbin)):
        temp = data[(data['pwrat','nanmean']>=pwratbin[i]-50) & (data['pwrat','nanmean']<pwratbin[i]+50)]
        if len(temp) >= num:
            pwrat_wind.loc[i,'pwratbin'] = pwratbin[i]
            pwrat_wind.loc[i,'pwrat'] = np.mean(temp['pwrat','nanmean'])
            pwrat_wind.loc[i,'wspd'] = np.mean(temp['wspd','nanmean'])
            pwrat_wind.loc[i,'rotspd'] = np.mean(temp['rotspd','nanmean'])
            pwrat_wind.loc[i,'wind_std'] = np.std(temp['wspd','nanmean'])
            pwrat_wind.loc[i,'pwrat_std'] = np.std(temp['pwrat','nanmean'])            
        #pc_df = pc_df.append(pc_df)
    return pwrat_wind

def monthdata(datatemp,altitude,path):#月数据分析！！！！！！！
    month_data = pd.DataFrame()
    datatemp['rho'] = (101325.0*(1.0-0.0065*altitude/(datatemp['exltmp','nanmean']+273.15))**5.255584)/(287.05*(datatemp['exltmp','nanmean']+273.15))
    for i in np.unique(datatemp.index.month.values):
        data = datatemp[datatemp.index.month.values==i]
        year = np.unique(data.index.year.values)
        index = str('%.4i' %year+'年'+'%.2i' %i+'月')
        month_data.loc[index,'rho'] = np.mean(data['rho'])
        month_data.loc[index,'wspd'] = np.mean(data['wspd','nanmean'])
    month_data = month_data.dropna()
    month_data = month_data.sort_index()
    fig, ax = plt.subplots(figsize=(8, 4), dpi=100)
    ax.plot(month_data.index,month_data['rho'],'o-',color='r',label='空气密度')
    ax.set_ylabel('空气密度(kg/m3)', color='#ccc')
    
    ax1 = ax.twinx()
    ax1.plot(month_data.index,month_data['wspd'],'o-',color='b',label='风速')
    ax1.set_ylabel('风速(m/s)', color='#ccc')
    
    ax.grid()

    # 设置刻度颜色
    ax.tick_params(axis='x', colors='#426977')  # 设置角度刻度颜色
    ax.tick_params(axis='y', colors='#426977')  # 设置半径刻度颜色
    ax1.tick_params(axis='x', colors='#426977')  # 设置角度刻度颜色
    ax1.tick_params(axis='y', colors='#426977')  # 设置半径刻度颜色
    # 设置刻度值颜色
    for label in ax.get_xticklabels():
        label.set_color('#DBE9F1')  # 设置x轴刻度值颜色为橙色

    for label in ax.get_yticklabels():
        label.set_color('#DBE9F1')  # 设置y轴刻度值颜色为橙色
    for label in ax1.get_xticklabels():
        label.set_color('#DBE9F1')  # 设置x轴刻度值颜色为橙色

    for label in ax1.get_yticklabels():
        label.set_color('#DBE9F1')  # 设置y轴刻度值颜色为橙色
    # 设置坐标轴边框颜色
    ax.spines['top'].set_color('#426977')    # 设置上边框颜色
    ax.spines['right'].set_color('#426977')  # 设置右边框颜色
    ax.spines['left'].set_color('#426977')  # 设置左边框颜色
    ax.spines['bottom'].set_color('#426977')  # 设置下边框颜色
    ax1.spines['top'].set_color('#426977')    # 设置上边框颜色
    ax1.spines['right'].set_color('#426977')  # 设置右边框颜色
    ax1.spines['left'].set_color('#426977')  # 设置左边框颜色
    ax1.spines['bottom'].set_color('#426977')  # 设置下边框颜色
    # 设置经纬线颜色
    ax.xaxis.grid(color='#426977')
    ax.yaxis.grid(color='#426977')
    ax1.xaxis.grid(color='#426977')
    ax1.yaxis.grid(color='#426977')
    # 设置图例
    legend = ax.legend(loc=2, framealpha=0)
    for text in legend.get_texts():
        text.set_color('#ccc')  # 设置图例标签颜色
    legend = ax1.legend(loc=1, framealpha=0)
    for text in legend.get_texts():
        text.set_color('#ccc')  # 设置图例标签颜色
    fig.savefig(str(path+'/'+'month.png'),transparent=True, bbox_inches ='tight')
    return month_data, str(path+'/'+'month.png')

def pass_filter(data,cutoff_freq,medfilt_freq,wiener_freq,cutoff_freq_butter,order,choice_num):#1:低通滤波器，2：中值滤波，3：wiener滤波，4：巴特沃斯滤波器
    filtered_data = np.copy(data)
    if choice_num == 1:
        for i in range(1,len(data)):
            filtered_data[i] = (1-cutoff_freq)*filtered_data[i-1]+cutoff_freq*data[i]            
    if choice_num == 2:
        filtered_data = signal.medfilt(data,medfilt_freq)
    if choice_num == 3:
        filtered_data = signal.wiener(data,wiener_freq)
    if choice_num == 4:
        #cutoff_freq_butter = 0.002/0.5*(1/60)
        b,a = signal.butter(order,cutoff_freq_butter,btype='low',analog=False)
        #filtered_data = signal.lfilter(b,a,data)
        filtered_data = signal.filtfilt(b,a,data)
    return filtered_data    


def data_clear(data,Pitch_Min,Pwrat_Rate,Rotspd_Rate,Rotspd_Connect,rotor,altitude,state):
    
    res_df = data[(data['state']==state)]
    pwrat_range = np.arange(0,Pwrat_Rate*1.2+10,20.0)#######数据点太少容易出错
    pwrat_bin = np.arange(10,Pwrat_Rate*1.2,20.0)
    res_df['pwbin'] = pd.cut(res_df['pwrat'],pwrat_range,labels=pwrat_bin)
    res_df['pitchlim'] = Pitch_Min
    for i in range(len(pwrat_bin)):
        res_df.loc[(res_df['pwbin']==pwrat_bin[i]),'pitchlim'] = np.mean(res_df[(res_df['pwrat']>=pwrat_bin[i]-10)&(res_df['pwrat']<pwrat_bin[i]+10)]['pitch1'].nsmallest(5))
    
    res_df = res_df[abs(res_df['wdir0'])<=35.0] 
    res_df = res_df[res_df['wspd']<=50.0]
    res_df = res_df[(res_df['pwrat']>0) & (res_df['pwrat']<Pwrat_Rate*0.9)]
    res_df = res_df[(res_df['rotspd']>=Rotspd_Connect*0.95) & (res_df['rotspd']<=Rotspd_Rate*1.1)]
    #res_df = res_df[(res_df['pwrat']<=0.8*power_rate) & (res_df['pitch1']<=2.5)]
    #res_df['rho'] = (101325.0*(1.0-0.0065*altitude/(res_df['exltmp']+273.15))**5.255584)/(287.05*(res_df['exltmp']+273.15))
    #res_df['wspd_f'] = (res_df['wspd'])*(res_df['rho']/1.225)**(1.0/3.0)
    res_df['cp'] = 1000.0*res_df['pwrat']/(0.5*1.225*np.pi*rotor*rotor*(res_df['wspd']**3))
    
    #res_df = thresholdfun_orig(res_df,3)
    #thresholdfun_orig1(data,neighbors_num=50)
    res_df = res_df[(res_df['pitch1']<=2.0+res_df['pitchlim'])]
    
    fig = plt.figure(figsize=(10,8),dpi=100)  
    #plt.title(str(turbine_name))    
    with plt.style.context('ggplot'):  
        #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red') 
        #plt.scatter(res_df['wspd'],res_df['pwrat'],c=res_df['cp'],cmap='jet',s=15) 
        plt.scatter(res_df['pwrat'],res_df['pitch1'],s=15)
        plt.scatter(res_df['pwrat'],res_df['pitchlim'],s=15)
        
        plt.colorbar()
    
    #res_df = res_df[(res_df['cp']<1.2)&(res_df['cp']>0.2)]   
    return res_df


def winddir_err_before_new(data,dirbin,windbin2,dirbin1,windbin1,path,turbine_name,request_num):
    
    idx = pd.IndexSlice
    yaw_err = pd.DataFrame() 
    yaw_err_minmax = pd.DataFrame() 
    X_train = pd.DataFrame()          

    data['wdir0cut1'] = pd.cut(data['wdir0'],dirbin1,right=False,labels=dirbin)
    data['wspdcut1'] = pd.cut(data['wspd'],windbin1,right=False,labels=windbin2)
    yaw_err = data.pivot_table(['pwrat','wspd'],index=['wspdcut1','wdir0cut1'],aggfunc='mean')
    yaw_err = yaw_err.dropna()
    
    cm_sub = np.linspace(0.0,1.0,15)
    colors = [cm.rainbow(x) for x in cm_sub]
    
    for i in range(len(windbin2)):    
        if (len(data[(data['wspd']>=windbin2[i]-0.25)&(data['wspd']<windbin2[i]+0.25)]))>100:
            yaw_err_temp = yaw_err.loc[idx[windbin2[i],:]]
            yaw_err_temp = yaw_err_temp.reset_index(level='wdir0cut1')
            min_max_scaler = preprocessing.StandardScaler()
            train_minmax = min_max_scaler.fit_transform(yaw_err_temp['pwrat'].values.reshape(-1, 1))
            yaw_err_temp['pwrat_scaler'] = train_minmax
            
            clf = LocalOutlierFactor(n_neighbors=50)
            #clf = IsolationForest(n_estimators=50,contamination=0.25,random_state=1)
            y_pred = clf.fit_predict(yaw_err_temp)
            yaw_err_temp['y_pred'] = y_pred
            '''
            fig = plt.figure(figsize=(10,8),dpi=100)  
            with plt.style.context('ggplot'):            
                plt.scatter(yaw_err_temp['wdir0cut1'],yaw_err_temp['pwrat_scaler'],c=yaw_err_temp['y_pred'],cmap='jet',s=20) 
            '''
            yaw_err_minmax = pd.concat([yaw_err_minmax,yaw_err_temp])#.append(yaw_err_temp)
            
    yaw_err_minmax = yaw_err_minmax[yaw_err_minmax['y_pred']==1]
    yaw_err_minmax = yaw_err_minmax.reset_index(drop=True)
    aa = np.zeros(len(yaw_err_minmax),dtype=float)
    for j in range(len(yaw_err_minmax)):
        aa[j] = yaw_err_minmax['wdir0cut1'][j]
    
    X_train['wdir0'] = aa
    X_train['pwrat_scaler'] = yaw_err_minmax.loc[:,'pwrat_scaler']
    
    #clf = OneClassSVM(nu=0.25,gamma=0.25)
    #clf = LocalOutlierFactor(n_neighbors=50)
    clf = IsolationForest(n_estimators=50,contamination=0.3,random_state=1)
    y_pred = clf.fit_predict(X_train)
    X_train['y_pred'] = y_pred
    X_train_last = X_train[X_train['y_pred']==1]
    '''
    fig = plt.figure(figsize=(10,8),dpi=100)  
    with plt.style.context('ggplot'):            
        plt.scatter(X_train['wdir0'],X_train['pwrat_scaler'],c=X_train['y_pred'],cmap='jet',s=20)
    '''    
    
    X_train_mean = X_train_last.groupby('wdir0',as_index=False).mean()
    
    min_max_scaler = preprocessing.MinMaxScaler(feature_range=(np.cos(np.pi*np.max(np.abs(X_train_mean['wdir0']))/180.0)**2, 1.0))
    train_minmax = min_max_scaler.fit_transform(X_train_mean['pwrat_scaler'].values.reshape(-1, 1))
    X_train_mean['pwrat_scaler_minmax'] = train_minmax
            
    poly_model = make_pipeline(PolynomialFeatures(2),LinearRegression())#二次方拟合,金风3次方拟合更好
    #poly_model = DecisionTreeRegressor(max_depth=3,min_samples_split=15)#不能用
    #poly_model = SVR(kernel='poly',degree=2,coef0=2,C=100,epsilon=1e-6)#效果与现行二次差不多
    #poly_model.fit(np.cos(aa[:,np.newaxis]*0.01745),yaw_err_temp['pwrat_f'])#cos拟合
    #yfit = poly_model.predict(np.cos(aa[:,np.newaxis]*0.01745))
    poly_model.fit((X_train_mean.loc[:,'wdir0'].values.reshape(-1,1)*0.0175),X_train_mean.loc[:,'pwrat_scaler_minmax'])
    yfit = poly_model.predict((np.unique(X_train_mean['wdir0']).reshape(-1,1)*0.0175))
    
    #poly_model.fit(aa.reshape(-1,1)*0.0175,yaw_err_minmax.loc[:,'pwrat_scaler'])
    #yfit1 = poly_model.predict(dirbin.reshape(-1,1)*0.0175)
         
    err_result = float('%.2f' %np.unique(X_train_mean['wdir0'])[np.argmax(yfit)])
    err_result_min = float('%.2f' %np.unique(X_train_mean['wdir0'])[np.argmin(yfit)])
    #print((np.max(yfit)-np.min(yfit))/(1 - np.cos(3.14159*(err_result_min - err_result)/180.0)))
    
    if (((np.max(yfit)-np.min(yfit))>0.4*(1 - np.cos(3.14159*(err_result_min - err_result)/180.0)))&((yfit[0]+yfit[-1])*0.5<yfit[round(len(yfit)/2)])):#0.3~0.4
        err_result_get = err_result
        # plt.rcParams['axes.unicode_minus'] = False
        # mpl.rcParams['font.sans-serif'] = ['SimHei'] 
        fig = plt.figure(figsize=(10,8),dpi=100)
        plt.subplot(1,1,1)    
        plt.title(str(turbine_name)+'对风偏差角度：'+str('%.1f' %err_result_get)+'，理论损失电量：'+str('{:.2%}'.format((1 - np.cos(3.14159*err_result_get/180.0)**2)*0.75)),fontsize=16, color='#ccc')
        with plt.style.context('ggplot'):            
            plt.scatter(X_train_mean['wdir0'],X_train_mean['pwrat_scaler_minmax'],color='blue',s=10)
            #plt.scatter(yaw_err_temp['wdir0cut1'],yaw_err_temp['pwrat','post'],color='red',s=20)
            #color使用标准色条，c使用变量赋值
            plt.plot(np.unique(X_train_mean['wdir0']),yfit,color='red')
            #plt.plot(dirbin,yfit1,color='green')
            plt.plot(np.full((len(X_train_mean),1),np.unique(X_train_mean['wdir0'])[np.argmax(yfit)]),X_train_mean['pwrat_scaler_minmax'],'--',color='black')
            #plt.plot(np.full((len(aa),1),dirbin[np.argmax(yfit1)]),yaw_err_minmax['pwrat_scaler'],'--',color='green')
            plt.grid()
            plt.xlabel('偏航偏差角度(°)',fontsize=14, color='#ccc')
            plt.ylabel('发电性能',fontsize=14, color='#ccc')
            plt.tick_params(which='both',labelcolor='#ccc', width=0,color='#426977', labelsize=14,gridOn=True,grid_color='#426977',direction ='in',right=True)
            #plt.text(0,min(train_minmax),str('%.1f' %windbin2[i]+'m/s'))
            #plt.colorbar()
            # 设置坐标轴边框颜色
            plt.gca().spines['top'].set_color('#426977')    # 设置上边框颜色
            plt.gca().spines['right'].set_color('#426977')  # 设置右边框颜色
            plt.gca().spines['left'].set_color('#426977')  # 设置左边框颜色
            plt.gca().spines['bottom'].set_color('#426977')  # 设置下边框颜色
        plt.subplots_adjust(top=0.95,bottom=0.08,left=0.08,right=0.95,hspace =0.10, wspace =0.1) #调整边距      
        #plt.margins(0,0)
        fig.savefig(str(path+'/'+str(turbine_name)+'_'+str(request_num)+'_yawerror.png'), transparent=True, dpi=100,bbox_inches='tight')
        filename = str(path+'/'+str(turbine_name)+'_'+str(request_num)+'_yawerror.png')
    else:
        err_result_get = -999999
        
        fig = plt.figure(figsize=(10,8),dpi=100)
        plt.subplot(1,1,1)    
        plt.title(str(turbine_name)+'对风偏差角度：'+str('%.1f' %err_result_get)+'，理论损失电量：'+str('{:.2%}'.format((1 - np.cos(3.14159*err_result_get/180.0)**2)*0.75)),fontsize=16, color='#ccc')
        with plt.style.context('ggplot'):            
            plt.scatter(X_train_mean['wdir0'],X_train_mean['pwrat_scaler_minmax'],color='blue',s=10)
            #plt.scatter(yaw_err_temp['wdir0cut1'],yaw_err_temp['pwrat','post'],color='red',s=20)
            #color使用标准色条，c使用变量赋值
            plt.plot(np.unique(X_train_mean['wdir0']),yfit,color='red')
            #plt.plot(dirbin,yfit1,color='green')
            plt.plot(np.full((len(X_train_mean),1),np.unique(X_train_mean['wdir0'])[np.argmax(yfit)]),X_train_mean['pwrat_scaler_minmax'],'--',color='black')
            #plt.plot(np.full((len(aa),1),dirbin[np.argmax(yfit1)]),yaw_err_minmax['pwrat_scaler'],'--',color='green')
            plt.grid()
            plt.xlabel('偏航偏差角度(°)',fontsize=14, color='#ccc')
            plt.ylabel('发电性能',fontsize=14, color='#ccc')
            plt.tick_params(which='both',labelcolor='#ccc', width=0,color='#426977', labelsize=14,gridOn=True,grid_color='#426977',direction ='in',right=True)
            # 设置坐标轴边框颜色
            plt.gca().spines['top'].set_color('#426977')    # 设置上边框颜色
            plt.gca().spines['right'].set_color('#426977')  # 设置右边框颜色
            plt.gca().spines['left'].set_color('#426977')  # 设置左边框颜色
            plt.gca().spines['bottom'].set_color('#426977')  # 设置下边框颜色
            #plt.text(0,min(train_minmax),str('%.1f' %windbin2[i]+'m/s'))
            #plt.colorbar()
        plt.subplots_adjust(top=0.95,bottom=0.08,left=0.08,right=0.95,hspace =0.10, wspace =0.1) #调整边距      
        #plt.margins(0,0)
        fig.savefig(str(path+'/'+str(turbine_name)+'_'+str(request_num)+'_yawerror00001.png'), transparent=True,dpi=100,bbox_inches='tight')
        filename = str(path+'/'+str(turbine_name)+'_'+str(request_num)+'_yawerror00001.png')
        
    return err_result_get, filename

#err_result = winddir_err_before_new(res_df,dirbin,windbin2,dirbin1,windbin1,path,turbine_name)
def thresholdfun_pwrat(raw_df,threshold,clear):
    temp_all = raw_df[raw_df['clear'] != clear ]    
    wind_bin = np.arange(2.0,np.ceil(np.nanmax(raw_df['wspd','nanmean'])),0.5)
    for m in range(len(wind_bin)):
        temp = raw_df[(raw_df['wspd','nanmean']>=wind_bin[m]-0.25) & (raw_df['wspd','nanmean']<wind_bin[m]+0.25)&(raw_df['clear']==clear)]
        pwrat_mean = np.nanmean(temp['pwrat','nanmean'])
        pwrat_std = np.nanstd(temp['pwrat','nanmean'])
        temp.loc[((temp['pwrat','nanmean']-pwrat_mean)/pwrat_std < threshold) & ((temp['pwrat','nanmean']-pwrat_mean)/pwrat_std > -threshold),'clear'] = clear-1
        temp_all = pd.concat([temp_all,temp])#.append(temp)
    return temp_all


def thresholdfun_wspd(raw_df,power_rate,threshold,clear):
    temp_all = raw_df[raw_df['clear'] != clear ]    
    pw_bin = np.arange(2.0,np.ceil(np.nanmax(raw_df['pwrat','nanmean'])),power_rate*0.2)
    for m in range(len(pw_bin)):
        temp = raw_df[(raw_df['pwrat','nanmean']>=pw_bin[m]-power_rate*0.1) & (raw_df['pwrat','nanmean']<pw_bin[m]+power_rate*0.1)&(raw_df['clear']==clear)]
        wspd_mean = np.mean(temp['wspd','nanmean'])
        wspd_std = np.std(temp['wspd','nanmean'])
        temp.loc[((temp['wspd','nanmean']-wspd_mean)/wspd_std < threshold) & ((temp['wspd','nanmean']-wspd_mean)/wspd_std > -threshold),'clear'] = clear-1
        temp_all = pd.concat([temp_all,temp])#.append(temp)
    return temp_all


def thresholdfun_pitch(raw_df,threshold,clear):
    temp_all = raw_df[raw_df['clear'] != clear ]    
    wind_bin = np.arange(2.0,np.ceil(np.nanmax(raw_df['wspd','nanmean'])),0.5)
    for m in range(len(wind_bin)):
        temp = raw_df[(raw_df['wspd','nanmean']>=wind_bin[m]-0.25) & (raw_df['wspd','nanmean']<wind_bin[m]+0.25)&(raw_df['clear']==clear)]
        pitch_mean = np.mean(temp['pitch1','nanmean'])
        pitch_std = np.std(temp['pitch1','nanmean'])
        temp.loc[((temp['pitch1','nanmean']-pitch_mean)/pitch_std < threshold) & ((temp['pitch1','nanmean']-pitch_mean)/pitch_std > -threshold),'clear'] = clear-1
        temp_all = pd.concat([temp_all,temp])
    return temp_all

def thresholdfun_rotspd(raw_df,neighbors_num,clear):
    temp_all = raw_df[(raw_df['clear'] != clear)]
    temp_clear = raw_df[(raw_df['clear'] == clear)]
    #temp_clear = temp_clear.dropna(axis=0,subset=[('pwrat','nanmean')],inplace=True)
    #temp_clear = temp_clear.dropna(axis=0,subset=[('pitch1','nanmean')],inplace=True)
    X_train = pd.DataFrame()
    X_train['pwrat'] = temp_clear['pwrat','nanmean']
    X_train['pitch'] = temp_clear['pitch1','nanmean']    
    clf = LocalOutlierFactor(n_neighbors=neighbors_num,p=1,n_jobs=-1)
    #clf = IsolationForest(n_estimators=50,contamination=0.25,random_state=1)
    y_pred = clf.fit_predict(X_train)
    temp_clear['y_pred'] = y_pred
    temp_clear.loc[temp_clear['y_pred']==1,'clear'] = clear-1
    temp_all = pd.concat([temp_all,temp_clear])
    return temp_all

def thresholdfun_pwrat_out(raw_df,neighbors_num,clear):
    temp_all = raw_df[raw_df['clear'] != clear ]
    temp_clear = raw_df[raw_df['clear'] == clear ]
    X_train = pd.DataFrame()
    X_train['wspd'] = temp_clear['wspd','nanmean']
    X_train['pwrat'] = temp_clear['pwrat','nanmean']    
    clf = LocalOutlierFactor(n_neighbors=neighbors_num,p=1,n_jobs=-1)
    #clf = IsolationForest(n_estimators=50,contamination=0.25,random_state=1)
    y_pred = clf.fit_predict(X_train)
    temp_clear['y_pred'] = y_pred
    temp_clear.loc[temp_clear['y_pred']==1,'clear'] = clear-1
    temp_all = pd.concat([temp_all,temp_clear])
    return temp_all

    #风频计算(所有机组)
def winddistribute(data,windbin,windbinreg):
    windfreq_s = pd.DataFrame()
    temp = data['wspd','nanmean']
    windfreq = pd.value_counts(pd.cut(temp,windbinreg,right=False),normalize=True,sort=False)
    windcount = pd.value_counts(pd.cut(temp,windbinreg,right=False),normalize=False,sort=False)
    windfreq_s = pd.DataFrame({'windbin':windbin,'freq':windfreq,'count':windcount})
    return windfreq_s

    #湍流计算(单台机组)
def wind_ti(data,windbin,num):
    wind_ti = pd.DataFrame()
    data_temp = data[[('wspd','nanmean'),('wspd','nanstd')]]
    data_temp['ti'] = data_temp['wspd','nanstd'] / data_temp['wspd','nanmean']
    for i in range(len(windbin)):
        temp = data_temp[(data_temp['wspd','nanmean']>=windbin[i]-0.25) & (data_temp['wspd','nanmean']<windbin[i]+0.25)]
        if len(temp) >= num:
            wind_ti.loc[i,'windbin'] = windbin[i]
            wind_ti.loc[i,'ti'] = np.mean(temp['ti'])
    return wind_ti

    #Cp计算(单台机组)
def turbine_Cp(data,windbin,num):
    turbine_cp = pd.DataFrame()
    data_temp = data[[('wspd','nanmean'),('cp','')]]
    for i in range(len(windbin)):
        temp = data_temp[(data_temp['wspd','nanmean']>=windbin[i]-0.25) & (data_temp['wspd','nanmean']<windbin[i]+0.25)]
        if len(temp) >= num:
            turbine_cp.loc[i,'windbin'] = windbin[i]
            turbine_cp.loc[i,'cp'] = np.mean(temp[('cp','')])
    return turbine_cp

def mymodenew(data):
    result = data.value_counts()
    if len(result)>0:
        return result.index[0]
    else:
        return np.nan
    
def mymode(data):
    if len(data)>0:
        return st.mode(data)
    else:
        return np.nan
    
def mymean(data):
    arr = np.array(data)
    arr = np.delete(arr,[np.argmax(arr),np.argmin(arr)])
    mymean = np.nanmean(arr)
    return mymean



#10min数据清洗
def data_min_clear(data,state,Rotspd_Connect,Rotspd_Rate,Pwrat_Rate,Pitch_Min,neighbors_num=20,threshold=3): 
    
    pwrat_coeff1 = 0.92
    pwrat_coeff2 = 0.3
    pitch_add = 2.0
    pitch_rate = 12.5
    storm_wind = 25.0#金风6.0MW以上机组设置为15,15m/s后降功率运行
    data = data.dropna(axis=0,thresh=len(data.columns)*0.4)  
    data = data[(data['wspd','nanmean']<30)&(data['wspd','nanmean']>0)] 
    data = data[(data['pwrat','nanmean']>0)&(data['pwrat','nanmean']<Pwrat_Rate*1.2)]
    data = data[(data['pitch1','nanmean']>-5)&(data['pitch1','nanmean']<95)]  
    #data = data[(data['pitch2','nanmean']>-5)&(data['pitch2','nanmean']<95)]
    #data = data[(data['pitch3','nanmean']>-5)&(data['pitch3','nanmean']<95)]
    
    data['clear'] = 10
    #data['pitchlim'] = (data['pwrat','nanmean'] - pwrat_coeff2*Pwrat_Rate)*(pitch_rate-pitch_add-Pitch_Min) / (Pwrat_Rate*(pwrat_coeff1-pwrat_coeff2)) + (Pitch_Min+pitch_add)
    
    pwrat_range = np.arange(0,Pwrat_Rate*1.2+10,20.0)#######数据点太少容易出错
    pwrat_bin = np.arange(10,Pwrat_Rate*1.2,20.0)
    data['pwbin'] = pd.cut(data['pwrat','nanmean'],pwrat_range,labels=pwrat_bin)
    data['pitchlim'] = Pitch_Min
    for i in range(len(pwrat_bin)):
        data.loc[(data['pwbin']==pwrat_bin[i]),'pitchlim'] = np.mean(data[(data['pwrat','nanmean']>=pwrat_bin[i]-10)&(data['pwrat','nanmean']<pwrat_bin[i]+10)]['pitch1','nanmean'].nsmallest(5))
        
    
    
    #风机发电状态
    data.loc[(data['state','mymode']==state),'clear'] = 9
    #data.loc[((data['state','mymode']==8)|(data['state','mymode']==9)|(data['state','mymode']==10)),'clear'] = 9
    #风机转速、功率正常阈值内
    data.loc[(data['rotspd','nanmean']>Rotspd_Connect*0.9)&(data['rotspd','nanmean']<Rotspd_Rate*1.1)&(data['pwrat','nanmean']>0)&(data['pwrat','nanmean']<Pwrat_Rate*1.2)&(data['clear']==9),'clear'] = 8
    #非限功率状态
    data.loc[(data['state','mymode']==state)&(data['clear']==8),'clear'] = 7
    #data.loc[((data['statety','mymode']==71)|(data['statety','mymode']==80)|(data['statety','mymode']==60))&(data['clear']==8),'clear'] = 7
    
    #无超过正常范围的变桨
    data.loc[((data['pwrat','nanmean']>Pwrat_Rate*pwrat_coeff1)|
              ((data['pwrat','nanmean']<=Pwrat_Rate*pwrat_coeff1)&(data['pitch1','nanmean']<=data['pitchlim']+pitch_add)&(data['wspd','nanmean']<=storm_wind))|
              ((data['pwrat','nanmean']<=Pwrat_Rate*pwrat_coeff1)&(data['pitch1','nanmean']<=data['pitchlim']+25)&(data['wspd','nanmean']>storm_wind)))&
             (data['clear']==7),'clear'] = 6
    
    try:
        data_clear = thresholdfun_rotspd(data,neighbors_num,6)
    except Exception:
        data.loc[(data['clear']==6),'clear'] = 5
        data_clear = data
    data_clear = thresholdfun_pitch(data_clear,threshold,5)    
    data_clear = thresholdfun_pwrat(data_clear,threshold,4)
    #data_clear = thresholdfun_wspd(data_clear,Rotspd_Rate,threshold*30000,3)###容易导致额定功率以上踢掉
    
    try:
        data_clear = thresholdfun_pwrat_out(data_clear,neighbors_num,3)
    except Exception:
        data.loc[(data['clear']==3),'clear'] = 2
        data_clear = data
    
    return data_clear


    

#额定功率异常
def Pwrat_Rate_loss(data,Pwrat_Rate):
    temp = data[data['clear','']<=8]
    if ((np.nanmean(temp.loc[(temp['pitch1','nanmean']>=5),('pwrat','nanmean')].nlargest(10))<0.95*Pwrat_Rate)):
        return True
    else:
        False

#最佳Cp段转矩控制异常异常
def Torque_Cp_kopt_loss(data,Pwrat_Rate,Rotspd_Connect,Rotspd_Rate,mytol=0.1,myreg=0.02):
    data['kopt_err'] = 1
    kopt_err = 0
    #gmm = GaussianMixture(n_components=2,covariance_type='full',random_state=0,tol=0.01,reg_covar=0.000005)
    gmm = BayesianGaussianMixture(n_components=2, covariance_type="full",random_state=0,tol=mytol,reg_covar=myreg)
    #gmm = DBSCAN(eps=0.1,min_samples=10)
    #gmm = SpectralClustering(n_clusters=2,assign_labels='discretize',eigen_solver='arpack',affinity='nearest_neighbors',n_neighbors=500,random_state=0,n_jobs=-1)
    temp = data[(data['rotspd','nanmean']>Rotspd_Connect*1.1)&(data['rotspd','nanmean']<Rotspd_Rate*0.9)]
    if len(temp)>0.2*len(data):
        xx = temp.loc[:,[('kopt')]].values
        min_max_scaler = preprocessing.StandardScaler()
        train_minmax = min_max_scaler.fit_transform(xx)
        #labels = gmm.fit(train_minmax).labels_
        labels = gmm.fit(train_minmax).predict(train_minmax)
        #labels = gmm.fit(xx).predict(xx)
        temp['kopt_err'] = labels
        #data.loc[temp.index.values,'kopt_err'] = labels
        kopt_err_temp = np.abs(np.nanmean(temp[temp['kopt_err']==1]['kopt']) - np.nanmean(temp[temp['kopt_err']==0]['kopt']))/np.nanmean(temp[temp['kopt_err']==1]['kopt'])
    
        if ((temp['kopt_err'].value_counts().iloc[-1] / len(temp) > 0.08)&(kopt_err_temp > 0.20)):#0.18
            kopt_err = 1
            return kopt_err,temp
        else:
            return kopt_err,temp
    else:
        return kopt_err,temp


            
#额定功率段转矩控制异常异常
def Torque_Rotspd_Rate_loss(data,Pwrat_Rate,Rotspd_Rate,Rotspd_Connect):
    rate_kopt_err = 0
    if np.abs(np.nanmean(data.loc[data['pwrat','nanmean'].nlargest(20).index,('rotspd','nanmean')]) - Rotspd_Rate)/Rotspd_Rate>0.03:
        Rotspd_Rate = np.nanmean(data.loc[data['pwrat','nanmean'].nlargest(20).index,('rotspd','nanmean')])
    temp = data[(data['rotspd','nanmean']>Rotspd_Rate*0.97)&(data['pwrat','nanmean']<=Pwrat_Rate)]
    temp_kopt = data[np.abs(data['rotspd','nanmean']-(Rotspd_Connect+Rotspd_Rate)*0.5)/((Rotspd_Connect+Rotspd_Rate)*0.5)<=0.1]
    kopt_temp = np.nanmean(temp_kopt['pwrat','nanmean'] / (temp_kopt['rotspd','nanmean']**3))
    rotspd_power_nihe = pd.DataFrame()
    if len(temp)>100:
        rotspd_small = np.nanmean(temp.loc[temp['pwrat','nanmean'].nsmallest(20).index,('rotspd','nanmean')])
        rotspd_large = np.nanmean(temp.loc[temp['pwrat','nanmean'].nlargest(20).index,('rotspd','nanmean')])    
        if (np.abs(rotspd_large - rotspd_small)/(0.5*(rotspd_large + rotspd_small))>0.01)&((np.nanmean(temp['pwrat','nanmean'].nlargest(20)) - np.nanmean(temp['pwrat','nanmean'].nsmallest(20)))<0.15*Pwrat_Rate):
            rate_kopt_err = 1
            rotspd_power_nihe['rotspd'] = np.arange(Rotspd_Connect,Rotspd_Rate+(Rotspd_Rate-Rotspd_Connect)*0.0001,(Rotspd_Rate-Rotspd_Connect)*0.0001)
            rotspd_power_nihe['pwrat'] = kopt_temp*rotspd_power_nihe['rotspd']**3
            new_row = pd.DataFrame({'rotspd':[Rotspd_Connect],'pwrat':[0]})
            rotspd_power_nihe = pd.concat([rotspd_power_nihe,new_row],ignore_index=True)#.append(new_row,ignore_index=True)
            new_row = pd.DataFrame({'rotspd':[Rotspd_Rate],'pwrat':[Pwrat_Rate]})
            rotspd_power_nihe = pd.concat([rotspd_power_nihe,new_row],ignore_index=True)
            rotspd_power_nihe = rotspd_power_nihe.sort_values(by='pwrat',ascending=True)
            return rate_kopt_err,rotspd_power_nihe
        else:
            return rate_kopt_err,rotspd_power_nihe
    else:
        return rate_kopt_err,rotspd_power_nihe


            
#变桨控制异常(额定功率5度)
def Pitch_Control_loss(data,Pitch_Min,Pwrat_Rate,Rotspd_Connect,Rotspd_Rate,mytol=0.1,myreg=0.02):  
    train_data = data.loc[:,[('pwrat','nanmean'),('pitch1','nanmean')]].dropna()         
    x_train = train_data['pwrat','nanmean'].values.reshape(-1,1)
    y_train = train_data['pitch1','nanmean'].values.reshape(-1,1)
    clf = GradientBoostingRegressor(n_estimators=20,random_state=0)
    clf.fit(x_train,y_train)
    
    x_test = np.arange(0.1,1.0+0.01,0.01)
    y_pred =  clf.predict(x_test.reshape(-1,1)*Pwrat_Rate) 

    if Pitch_Min >= 0.0:             
        curve_area = integrate.trapz(y_pred,x_test)
        triangle_area = 0.5*(Pitch_Min+5.0+Pitch_Min)*(1.0-0.6)
    else:
        curve_area = integrate.trapz(y_pred-np.mean(sorted(y_pred)[:5]),x_test)
        triangle_area = 0.5*(5.0-Pitch_Min)*(1.0-0.6)
            
    if curve_area > triangle_area:
        return curve_area,x_test,y_pred
    else:
        return 0,x_test,y_pred

#偏航控制误差过大
def Yaw_Control_loss(data):   
    a1,a2,a3 = plt.hist(data['wdir0','nanmean'],bins=100,alpha=1,cumulative=True,density=True,range = (-40,40))
    indexs=[]
    #a2=a2.tolist()
    for i,value in enumerate(a2):
        if i<=len(a2)-2:
            index=(a2[i]+a2[i+1])/2
            indexs.append(index)
    leiji = pd.DataFrame({'yaw':indexs,'leiji':a1})
    leiji = leiji[np.abs(leiji['yaw'])<=10.0]
    leiji['leiji'] = leiji['leiji'] - np.full(len(leiji),leiji.iloc[0,1])
    return leiji

        
#最小桨距角异常data=all
def Pitch_Min_loss(data,Pitch_Min,Pwrat_Rate,Rotspd_Connect):
    Pitch_Min_loss = 0
    day = pd.date_range(np.min(data.index),np.max(data.index),freq="7d").to_list()#.strftime('%Y-%m-%d %H:%M:%S').to_list()#7天为一周期判断
    temp_all = data[(data['rotspd','nanmean']>Rotspd_Connect)&(data['pwrat','nanmean']<Pwrat_Rate*0.35)]
    #temp_err = data[(data['rotspd','nanmean']>Rotspd_Connect*1.1)&(data['pwrat','nanmean']<Pwrat_Rate*0.3)&(np.abs(data['pitch1','nanmean'])>2.5)&(np.abs(data['pitch1','nanmin'])>2.5)]
    
    for i in range(len(day)-1):
        # temp = temp_all.loc[day[i]:day[i+1],:] 
        temp = temp_all[(temp_all.index >= day[i]) & (temp_all.index < day[i+1])]
        if len(temp) > 20:               
            if(np.abs(np.nanmean(temp['pitch1','nanmean'].nsmallest(10)))>2.0)|(np.abs(np.nanmean(temp['pitch2','nanmean'].nsmallest(10)))>2.0)|(np.abs(np.nanmean(temp['pitch3','nanmean'].nsmallest(10)))>2.0):
                Pitch_Min_loss = Pitch_Min_loss + 1
        #print(str(i)+'/'+str(Pitch_Min_loss)+'/'+str(len(temp))+'/'+str(np.abs(np.nanmean(temp['pitch1','nanmean'].nsmallest(10)))))
    '''
    if len(temp_err)/len(temp_all)>0.2:
        Pitch_Min_loss = 1
    '''    
    return Pitch_Min_loss

    

#桨距角不平衡
def Pitch_Nobalance_loss(data,data_m):
    if((['pitch1'] in data_m.columns.values)==True)&((['pitch2'] in data_m.columns.values)==True)&((['pitch3'] in data_m.columns.values)==True):
        if((np.mean(np.abs(data['pitch1','nanmean'] - data['pitch2','nanmean']))>0.8)|
           (np.mean(np.abs(data['pitch2','nanmean'] - data['pitch3','nanmean']))>0.8)|
           (np.mean(np.abs(data['pitch3','nanmean'] - data['pitch1','nanmean']))>0.8)):
            return True
        else:
            return False
    else:
        return False
    

#分段统计
def FenDuan(data,Pwrat_Rate,Rotspd_Connect,Rotspd_Rate,turbine_name):
    fenduan = pd.DataFrame()
    '''
    #gmm = GaussianMixture(n_components=3,covariance_type='full',random_state=0,tol=0.1,reg_covar=0.01)
    gmm = BayesianGaussianMixture(n_components=3, covariance_type="full",random_state=0,tol=10,reg_covar=0.2)
    #temp = df_all_clear[df_all_clear['kopt_err']==1]
    xx = data.loc[:,[('rotspd','nanmean'),('pwrat','nanmean'),('pitch1','nanmean'),('kopt','')]].values
    #xx = data.loc[:,[('rotspd','nanmean'),('pwrat','nanmean'),('kopt','')]].values
    min_max_scaler = preprocessing.StandardScaler()
    train_minmax = min_max_scaler.fit_transform(xx)
    labels = gmm.fit(train_minmax).predict(train_minmax)
    #labels = gmm.fit(xx).predict(xx)
    data['label'] = labels
    data['labels_temp'] = 99999
    order_rotspd = np.empty((1,3),dtype=float)
    
    labels_temp = np.nanmean(data[data['label']==0]['rotspd','nanmean'])
    data.loc[((data['label']==0)),'labels_temp'] = labels_temp
    order_rotspd[0,0] = labels_temp
    labels_temp = np.nanmean(data[data['label']==1]['rotspd','nanmean'])
    data.loc[((data['label']==1)),'labels_temp'] = labels_temp
    order_rotspd[0,1] = labels_temp
    labels_temp = np.nanmean(data[data['label']==2]['rotspd','nanmean'])
    data.loc[((data['label']==2)),'labels_temp'] = labels_temp
    order_rotspd[0,2] = labels_temp
    order_rotspd.sort()
    data['labelfen'] = 0
    data.loc[((data['labels_temp']<=order_rotspd[0,0])),'labelfen'] = 1
    data.loc[((data['labels_temp']>order_rotspd[0,0])&(data['labels_temp']<order_rotspd[0,2])),'labelfen'] = 2
    data.loc[((data['labels_temp']>=order_rotspd[0,2])),'labelfen'] = 3
    data.loc[((data['pwrat','nanmean']>Pwrat_Rate*0.95)),'labelfen'] = 4
    '''
    if np.abs(np.nanmean(data.loc[data['pwrat','nanmean'].nlargest(20).index,('rotspd','nanmean')]) - Rotspd_Rate)/Rotspd_Rate>0.03:
        Rotspd_Rate = np.nanmean(data.loc[data['pwrat','nanmean'].nlargest(20).index,('rotspd','nanmean')])
    data.loc[((data['rotspd','nanmean']<Rotspd_Connect*1.02)),'labelfen'] = 1
    data.loc[((data['rotspd','nanmean']>=Rotspd_Connect*1.02)&(data['rotspd','nanmean']<Rotspd_Rate*0.98)),'labelfen'] = 2
    data.loc[((data['rotspd','nanmean']>=Rotspd_Rate*0.98)&(data['pwrat','nanmean']<=Pwrat_Rate*0.97)),'labelfen'] = 3
    data.loc[((data['pwrat','nanmean']>Pwrat_Rate*0.97)),'labelfen'] = 4
    
    fenduan.loc[turbine_name,'count_connect'] = len(data[data['labelfen']==1]) / len(data)
    fenduan.loc[turbine_name,'count_cp'] = len(data[data['labelfen']==2]) / len(data)
    fenduan.loc[turbine_name,'count_sprate'] = len(data[data['labelfen']==3]) / len(data)
    fenduan.loc[turbine_name,'count_pwrate'] = len(data[data['labelfen']==4]) / len(data)
        
    fenduan.loc[turbine_name,'eny_connect'] = np.sum(data[data['labelfen']==1]['pwrat','nanmean']) / np.sum(data['pwrat','nanmean'])
    fenduan.loc[turbine_name,'eny_cp'] = np.sum(data[data['labelfen']==2]['pwrat','nanmean']) / np.sum(data['pwrat','nanmean'])
    fenduan.loc[turbine_name,'eny_sprate'] = np.sum(data[data['labelfen']==3]['pwrat','nanmean']) / np.sum(data['pwrat','nanmean'])
    fenduan.loc[turbine_name,'eny_pwrate'] = np.sum(data[data['labelfen']==4]['pwrat','nanmean']) / np.sum(data['pwrat','nanmean'])
    return data,fenduan
    
#风向玫瑰图
def WindRose(data,path):
    windrose = pd.DataFrame()
    turbine_name = np.unique(data['wtid'])[0]
    if(('yaw','nanmean') in data.columns)==True:
        windrose['wdir'] = data['yaw','nanmean']
    else:
        windrose['wdir'] = data['wdir','nanmean']
        
    windrose['wspd'] = data['wspd','nanmean']    
    windmax = np.ceil(np.nanmax(windrose['wspd']))
    if windmax > 50:
        windmax = 50.0
    windbin_rose = np.array([0,2,4,6,8,10,12,14,16,25])
    wdirbin = np.array(np.arange(0,361,22.5))
    windrose['windbin'] = pd.cut(windrose['wspd'],windbin_rose)
    windrose['wdirbin'] = pd.cut(windrose['wdir'],wdirbin)
    count_rose = windrose['wspd'].groupby([windrose['windbin'],windrose['wdirbin']]).count()
    rosedata = count_rose.unstack()
    
    theta = np.linspace(0,2*np.pi,16,endpoint=False)
    width = np.pi*1.5/16
    labels = list(['N','','NE','','E','','ES','','S','','WS','','W','','WN',''])
    fig=plt.figure(figsize=(10,8),dpi=100)
    ax = fig.add_axes([0.1,0.1,0.7,0.7],projection='polar')
    ax1 = fig.add_axes([0.8,0.1,0.03,0.7])
    colors = ['blue','orange','forestgreen','tomato','violet','m','yellow','gray','black']
    cmap = mpl.colors.ListedColormap(colors)
    norm = mpl.colors.BoundaryNorm(windbin_rose, cmap.N)
    
    for i in range(1,len(rosedata.index)):
        idx = rosedata.index[i]
        rad = rosedata.loc[idx]
        ax.bar(theta,rad,width=width,bottom=100,label=idx,tick_label=labels,color=colors[i])
    ax.set_theta_zero_location('N')
    ax.set_theta_direction(-1)
    ax.set_title(turbine_name+'风向玫瑰图',fontsize=20, color='#ccc')
    ax.tick_params(labelsize=15)
    # 设置经纬线颜色
    ax.xaxis.grid(color='#426977')  # 设置极坐标的径向网格线颜色
    ax.yaxis.grid(color='#426977')  # 设置极坐标的圆
    ax.spines['polar'].set_color('#426977')  # 设置极坐标轴的颜色
    # 设置刻度颜色
    ax.tick_params(axis='x', colors='#DBE9F1')  # 设置角度刻度颜色
    ax.tick_params(axis='y', colors='#DBE9F1')  # 设置半径刻度颜色

    #ax.set_yticks([200,500,1000,1500])
    cb = mpl.colorbar.ColorbarBase(ax1,cmap=cmap,norm=norm)
    cb.ax.tick_params(labelsize=14, labelcolor='#DBE9F1')
    cb.ax.yaxis.set_tick_params(color='#426977')  # 设置刻度线颜色为绿色
    fig.savefig(path + '/' +str(turbine_name) + '风向玫瑰图.png', transparent=True,dpi=100,bbox_inches='tight')    
    return path + '/' +str(turbine_name) + '风向玫瑰图.png'



###告警统计
def Turbine_Warning(data,turbine_name,fault_code,state_code):                   
    
    turbine_warning = pd.DataFrame()
    #fault_type = fault_code[(fault_code['type']=='故障停机')]  

    data_warning = data.dropna(axis=0,subset=[('fault','mymode')])
    data_warning['snum'] = data_warning['state','nanmean']
    #列分级
    columns = []
    for elem in state_code.columns.to_list():
        if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
            pass
        else:
            columns.append((elem, ''))
    if len(columns) > 0:
        state_code.columns = pd.MultiIndex.from_tuples(columns)
    data_warning.reset_index(level=0,inplace=True)
    data_warning = pd.merge(data_warning,state_code,how='left',on='snum')
    data_warning.set_index(('localtime',''),inplace= True)
    data_warning['fnum'] = data_warning['fault','mymode']
    data_warning['flt1'] = data_warning['fnum'].shift(periods=1,axis=0)
    data_warning['shift'] = data_warning['flt1']-data_warning['fnum']
    data_warning = data_warning[data_warning[('fault','mymode')]!=0]

    if len(data_warning) > 0: 
        data_warning.reset_index(level=0,inplace=True)
        #data_fault = pd.merge(data_fault,fault_type,how='left',on='fnum')
        #列分级
        columns = []
        for elem in fault_code.columns.to_list():
            if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
                pass
            else:
                columns.append((elem, ''))
        if len(columns) > 0:
            fault_code.columns = pd.MultiIndex.from_tuples(columns)
        data_warning = pd.merge(data_warning,fault_code,how='left',on='fnum')#远景机组正常发电状态故障码不为0,右结合故障统计可能不齐全
        data_warning.set_index(('localtime',''),inplace= True)
        data_warning = data_warning[((data_warning['state_type']=='正常发电'))]#&(data_fault['pwrat','nanmean']<10.0)可能导致只发生一次的故障统计不到
        fault_wtid = np.unique(data_warning['fnum'])    
        
        for j in range(len(fault_wtid)):
            temp = data_warning[(data_warning['fault','mymode']==fault_wtid[j])]#&(Df_all_m_fault['statel','mymode']==90003)]#90003可能是故障状态，90004是小风停机状态
            #temp1 = Df_all_m_fault[(Df_all_m_fault['fault','mymode']==fault_wtid[j])]
            turbine_warning.loc[j,'fault'] = fault_wtid[j]
            turbine_warning.loc[j,'count'] = len(temp[(temp[('shift')]!=0)&(temp['fnum']!=0)])
            turbine_warning.loc[j,'time'] = len(temp)/6.0
            turbine_warning.loc[j,'wspd'] = np.nanmean(temp['wspd','nanmean'])
            if ((fault_wtid[j] in fault_code['fnum'].values)==True):
                turbine_warning.loc[j,'fault_describe'] = fault_code[fault_code['fnum']==fault_wtid[j]]['fname'].values[0]
            else:
                turbine_warning.loc[j,'fault_describe'] = fault_wtid[j]
        turbine_warning.insert(0, 'wtid', turbine_name)
    if len(turbine_warning)>0:  
        turbine_warning = turbine_warning[turbine_warning['count']>0]
    return turbine_warning
  

###单机故障损失计算(分仓、功率曲线合并后的数据框---未知故障解释的停机全部归到故障停机中)
def Turbine_Fault_Loss(data,turbine_name,pw_df_temp,fault_code,state_code):                   
    
    fault_loss = pd.DataFrame()
    #fault_type = fault_code[(fault_code['type']=='故障停机')]  
    
    data_fault = data.dropna(axis=0,subset=[('fault','mymode')])
    #data_fault = data_fault[data_fault['fault','mymode']<3000]#金风3000以上为告警
    #列分级
    columns = []
    for elem in pw_df_temp.columns.to_list():
        if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
            pass
        else:
            columns.append((elem, ''))
    if len(columns) > 0:
        pw_df_temp.columns = pd.MultiIndex.from_tuples(columns)
    data_fault.reset_index(level=0,inplace=True)
    data_fault = pd.merge(data_fault,pw_df_temp,how='left',on='windbin')
    data_fault['snum'] = data_fault['state','mymode']
    #列分级
    columns = []
    for elem in state_code.columns.to_list():
        if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
            pass
        else:
            columns.append((elem, ''))
    if len(columns) > 0:
        state_code.columns = pd.MultiIndex.from_tuples(columns)
    data_fault = pd.merge(data_fault,state_code,how='left',on='snum')
    data_fault.set_index(('localtime',''),inplace= True)

    data_fault['fnum'] = data_fault['fault','mymode']
    data_fault['flt1'] = data_fault['fnum'].shift(periods=1,axis=0)
    data_fault['shift'] = data_fault['flt1']-data_fault['fnum']
    data_fault = data_fault[data_fault[('fault','mymode')]!=0]
    #data_fault = data_fault[data_fault[('pwrat','nanmean')]<=0]

    if len(data_fault) > 0: 
        data_fault.reset_index(level=0,inplace=True)
        #data_fault = pd.merge(data_fault,fault_type,how='left',on='fnum')
        #列分级
        columns = []
        for elem in fault_code.columns.to_list():
            if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
                pass
            else:
                columns.append((elem, ''))
        if len(columns) > 0:
            fault_code.columns = pd.MultiIndex.from_tuples(columns)
        data_fault = pd.merge(data_fault,fault_code,how='left',on='fnum')#远景机组正常发电状态故障码不为0,右结合故障统计可能不齐全
        data_fault.set_index(('localtime',''),inplace= True)
        data_fault = data_fault[((data_fault['state_type']=='故障停机'))]#&(data_fault['pwrat','nanmean']<10.0)可能导致只发生一次的故障统计不到
        fault_wtid = np.unique(data_fault['fnum'])    
        
        for j in range(len(fault_wtid)):
            temp = data_fault[(data_fault['fault','mymode']==fault_wtid[j])]#&(Df_all_m_fault['statel','mymode']==90003)]#90003可能是故障状态，90004是小风停机状态
            #temp1 = Df_all_m_fault[(Df_all_m_fault['fault','mymode']==fault_wtid[j])]
            fault_loss.loc[j,'fault'] = fault_wtid[j]
            fault_loss.loc[j,'count'] = len(temp[(temp['shift']!=0)&(temp['fnum']!=0)])
            fault_loss.loc[j,'time'] = len(temp)/6.0
            fault_loss.loc[j,'loss'] = np.nansum(temp[turbine_name])/6.0#kwh
            fault_loss.loc[j,'wspd'] = np.nanmean(temp['wspd','nanmean'])
            if ((fault_wtid[j] in fault_code['fnum'].values)==True):
                fault_loss.loc[j,'fault_describe'] = fault_code[fault_code['fnum']==fault_wtid[j]]['fname'].values[0]
            else:
                fault_loss.loc[j,'fault_describe'] = fault_wtid[j]
                
            if ((fault_wtid[j] in fault_code['fnum'].values)==True):
                fault_loss.loc[j,'fsyst'] = fault_code[fault_code['fnum']==fault_wtid[j]]['fsyst'].values[0]
            else:
                fault_loss.loc[j,'fsyst'] = '其它'
        fault_loss.insert(0, 'wtid', turbine_name)
    if len(fault_loss)>0:  
        fault_loss = fault_loss[fault_loss['count']>=0]
    return fault_loss
    
####限电停机时刻补充，只能通过厂家限功率状态位补充，如果没有厂家限功率状态，根据统一状态码修改程序statey=80表示限功率
def Grid_limit_stop(data,limpw_state,state,n_hours): 
    data = data.sort_index()
    df = data.copy()
    df['limpw_state'] = df['limpw','mymode']
    #df.loc[(df['limpw','mymode']==limpw_state)|(df['fault','mymode']!=0)|((df['limpw','mymode']!=limpw_state)&(df['state','mymode']==state)),'limpw_state'] = 0
    trigger_times = df[df['limpw_state']!=limpw_state].index.tolist()
    
    # 步骤1：找到第一个limpw=0的索引
    if len(trigger_times)<=0:
        return df  # 没有触发点直接返回
    else:        
        for trigger_time in trigger_times:
            start_time = trigger_time - pd.Timedelta(hours=n_hours)
            window_data = df.loc[start_time:trigger_time]
            
            ###排除出发点自身，仅检查之前的数据
            window_data = window_data.loc[window_data.index < trigger_time]
            
            if len(window_data)>=3:                
            # 步骤2：检查前n行条件
                condition_met = (
                    (window_data['limpw','mymode'] == limpw_state).all() and
                    (window_data['state','mymode'] == state).all() and
                    ((window_data['fault','mymode'] == 0)|(window_data['fault','mymode'] == 309001)).all() and  ###远景机组正常发电故障码为309001
                    (df.loc[trigger_time,('state','nanmean')]!=state)  ###触发时刻机组状态发生变化，否则正常发电时限功率解除也会被重新标记为限功率
                )
            else:
                condition_met = False
            
            # 步骤3：条件满足时执行修改
            if condition_met:
                # 获取初始状态值
                initial_state = df.loc[trigger_time, ('state','mymode')]
                initial_fault = df.loc[trigger_time, ('fault','mymode')]
                
                # 创建变化标记（从触发点开始后续的变化点）
                change_mask = (df['state','mymode'] != initial_state) | (df['fault','mymode'] != initial_fault)
                post_trigger = change_mask.loc[trigger_time:]
                
                # 找到第一个变化点的位置
                if post_trigger.any():
                    end_idx = post_trigger.idxmax()
                else:
                    end_idx = df.index[-1] + pd.Timedelta(second=60)  # 没有变化则修改到最后
                
                # 应用修改
                modify_mask = (df.index >= trigger_time)&(df.index < end_idx)&(df['limpw_state']!=limpw_state)
                df.loc[modify_mask, 'limpw_state'] = limpw_state
        
        df['limpw','mymode'] = df['limpw_state']
    
    return df
    
###单机电网限电损失(不包括限功率停机，只有限电降功率运行),如果执行Grid_limit_stop则可包括限功率停机，但数据中需要有厂家限功率状态位
def Grid_Limit_Loss(data,turbine_name,pw_df_temp,fault_code,limpw_state): 
    limgrid_loss = pd.DataFrame()    
    data_limgrid = pd.DataFrame()
    
    data_limgrid = data.dropna(axis=0,subset=[('fault','mymode')])
    data_limgrid.reset_index(level=0,inplace=True)
    data_limgrid = pd.merge(data_limgrid,pw_df_temp,how='left',on='windbin')
    data_limgrid.set_index(('localtime',''),inplace= True)  

    data_limgrid['fnum'] = data_limgrid['fault','mymode']
    data_limgrid.reset_index(level=0,inplace=True)
    data_limgrid = pd.merge(data_limgrid,fault_code,how='left',on='fnum')
    data_limgrid.set_index(('localtime',''),inplace= True)
    
    data_limgrid = data_limgrid[(data_limgrid['statety','mymode']==80)|((data_limgrid['type']=='电网限电')|(data_limgrid['statel','mymode']==90001))|(data_limgrid['limpw','mymode']==limpw_state)]###修改马集
    
    if len(data_limgrid) > 0:      
        #limgrid_loss.loc[num,'turbine'] = turbine_name
        limgrid_loss.loc[turbine_name,'loss'] = np.nansum(data_limgrid[turbine_name] - data_limgrid['pwrat','nanmean'])/6.0
        if limgrid_loss.loc[turbine_name,'loss']<=0:
            limgrid_loss.loc[turbine_name,'loss'] = 0.1
        limgrid_loss.loc[turbine_name,'time'] = len(data_limgrid)/6.0
        limgrid_loss.loc[turbine_name,'wspd'] = np.nanmean(data_limgrid['wspd','nanmean'])
    limgrid_loss['wtid'] = limgrid_loss.index

    return limgrid_loss

    
###单机电网限电损失(不包括限功率停机，只有限电降功率运行)
# def Grid_Limit_Loss(data,turbine_name,pw_df_temp,fault_code,state_code): 
#     limgrid_loss = pd.DataFrame()    
#     data_limgrid = pd.DataFrame()
    
#     data_limgrid = data[data['limpw','mymode']==4] #4：限电, 5:正常, 应填：4
#     #列分级
#     columns = []
#     for elem in pw_df_temp.columns.to_list():
#         if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
#             pass
#         else:
#             columns.append((elem, ''))
#     if len(columns) > 0:
#         pw_df_temp.columns = pd.MultiIndex.from_tuples(columns)
#     data_limgrid.reset_index(level=0,inplace=True)
#     data_limgrid = pd.merge(data_limgrid,pw_df_temp,how='left',on='windbin')
#     data_limgrid.set_index(('localtime',''),inplace= True)  

    
#     if len(data_limgrid) > 0:      
#         #limgrid_loss.loc[num,'turbine'] = turbine_name
#         limgrid_loss.loc[turbine_name,'loss'] = np.nansum(data_limgrid[turbine_name] - data_limgrid['pwrat','nanmean'])/6.0
#         if limgrid_loss.loc[turbine_name,'loss']<=0:
#             limgrid_loss.loc[turbine_name,'loss'] = 0.1
#         limgrid_loss.loc[turbine_name,'time'] = len(data_limgrid)/6.0
#         limgrid_loss.loc[turbine_name,'wspd'] = np.nanmean(data_limgrid['wspd','nanmean'])
#     limgrid_loss['wtid'] = limgrid_loss.index
#     return limgrid_loss

###单机计划停机损失
def Stop_Loss(data,turbine_name,pw_df_temp,fault_code,limpw_state):     
    stop_loss = pd.DataFrame()
    data_stop = pd.DataFrame()
    temp = pw_df_temp[pw_df_temp['pwrat']>=10.0]
    
    #fault_type = fault_code[(fault_code['type']=='服务状态')|(fault_code['type']=='用户停机')|(fault_code['type']=='计划停机')]
    data_stop = data.dropna(axis=0,subset=[('fault','mymode')])
    data_stop.reset_index(level=0,inplace=True)
    data_stop = pd.merge(data_stop,pw_df_temp,how='left',on='windbin')
    data_stop.set_index(('localtime',''),inplace= True)

    data_stop['fnum'] = data_stop['fault','mymode']
    data_stop.reset_index(level=0,inplace=True)
    data_stop = pd.merge(data_stop,fault_code,how='left',on='fnum')
    data_stop.set_index(('localtime',''),inplace= True)
    
    data_stop = data_stop[((data_stop['statety','mymode']==10)|(data_stop['statety','mymode']==20)|          ##统一状态计划停机信息
                          (data_stop['type']=='服务状态')|(data_stop['type']=='用户停机')|(data_stop['type']=='计划停机')|   ##机组故障码计划停机信息
                          ((data_stop['fault','nanmean']==0)&(data_stop['pitch1','nanmean']>=85)))
                          &(data_stop['wspd','nanmean']>=np.min(temp['windbin'])+1.0)&(data_stop['pwrat','nanmean']<10.0)
                          &(data_stop['limpw','mymode']!=limpw_state)]  ###其它自行判断的计划停机信息
    
    if len(data_stop) > 0:
        #stop_loss.loc[turbine_name,'turbine'] = turbine_name
        stop_loss.loc[turbine_name,'loss'] = np.nansum(data_stop[turbine_name])/6.0
        stop_loss.loc[turbine_name,'time'] = len(data_stop)/6.0
        stop_loss.loc[turbine_name,'wspd'] = np.nanmean(data_stop['wspd','nanmean'])
        stop_loss.loc[turbine_name,'exltmp'] = np.nanmean(data_stop['exltmp','nanmean'])
    stop_loss['wtid'] = stop_loss.index
    return stop_loss

############
###单机计划停机损失
# def Stop_Loss(data,turbine_name,pw_df_temp,state_code):     
#     stop_loss = pd.DataFrame()
#     data_stop = pd.DataFrame()
#     temp = pw_df_temp[pw_df_temp['pwrat']>=10.0]
    
#     #fault_type = fault_code[(fault_code['type']=='服务状态')|(fault_code['type']=='用户停机')|(fault_code['type']=='计划停机')]
#     data_stop = data.dropna(axis=0,subset=[('state','mymode')])
#     #列分级
#     columns = []
#     for elem in pw_df_temp.columns.to_list():
#         if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
#             pass
#         else:
#             columns.append((elem, ''))
#     if len(columns) > 0:
#         pw_df_temp.columns = pd.MultiIndex.from_tuples(columns)
#     data_stop.reset_index(level=0,inplace=True)
#     data_stop = pd.merge(data_stop,pw_df_temp,how='left',on='windbin')
#     data_stop.set_index(('localtime',''),inplace= True)

#     data_stop['snum'] = data_stop['state','mymode']
#     #列分级
#     columns = []
#     for elem in state_code.columns.to_list():
#         if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
#             pass
#         else:
#             columns.append((elem, ''))
#     if len(columns) > 0:
#         state_code.columns = pd.MultiIndex.from_tuples(columns)
#     data_stop.reset_index(level=0,inplace=True)
#     data_stop = pd.merge(data_stop,state_code,how='left',on='snum')
#     data_stop.set_index(('localtime',''),inplace= True)
    
#     data_stop = data_stop[(data_stop['state_type']=='服务状态')|(data_stop['state_type']=='用户停机')|   ##机组故障码计划停机信息
#                           ((data_stop['fault','nanmean']==0)&(data_stop['pitch1','nanmean']>=85)
#                           &(data_stop['wspd','nanmean']>=np.min(temp['windbin']))&(data_stop['pwrat','nanmean']<10.0))]  ###其它自行判断的计划停机信息
#     data_stop = data_stop[data_stop['limpw','mymode']!=4] #非限电
#     if len(data_stop) > 0:
#         #stop_loss.loc[turbine_name,'turbine'] = turbine_name
#         stop_loss.loc[turbine_name,'loss'] = np.nansum(data_stop[turbine_name])/6.0
#         stop_loss.loc[turbine_name,'time'] = len(data_stop)/6.0
#         stop_loss.loc[turbine_name,'wspd'] = np.nanmean(data_stop['wspd','nanmean'])
#         stop_loss.loc[turbine_name,'exltmp'] = np.nanmean(data_stop['exltmp','nanmean'])
#     stop_loss['wtid'] = stop_loss.index
#     return stop_loss
    


#单机电网故障损失
def Grid_Fault_Loss(data,turbine_name,pw_df_temp,fault_code,state_code):
    faultgrid_loss = pd.DataFrame()
    #fault_type = fault_code[fault_code['type']=='电网故障']  
    
    data_grid = data.dropna(axis=0,subset=[('fault','mymode')])
    #列分级
    columns = []
    for elem in pw_df_temp.columns.to_list():
        if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
            pass
        else:
            columns.append((elem, ''))
    if len(columns) > 0:
        pw_df_temp.columns = pd.MultiIndex.from_tuples(columns)
    data_grid.reset_index(level=0,inplace=True)
    data_grid = pd.merge(data_grid,pw_df_temp,how='left',on='windbin')
    data_grid.set_index(('localtime',''),inplace= True)

    data_grid['fnum'] = data_grid['fault','mymode']
    data_grid['snum'] = data_grid['state','nanmean']
    data_grid['flt1'] = data_grid['fnum'].shift(periods=1,axis=0)
    data_grid['shift'] = data_grid['flt1']-data_grid['fnum']
    data_grid = data_grid[data_grid[('fault','mymode')]!=0]
    
    
    data_grid.reset_index(level=0,inplace=True)
    #列分级
    columns = []
    for elem in fault_code.columns.to_list():
        if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
            pass
        else:
            columns.append((elem, ''))
    if len(columns) > 0:
        fault_code.columns = pd.MultiIndex.from_tuples(columns)
    data_grid = pd.merge(data_grid,fault_code,how='left',on='fnum')
    #列分级
    columns = []
    for elem in state_code.columns.to_list():
        if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
            pass
        else:
            columns.append((elem, ''))
    if len(columns) > 0:
        state_code.columns = pd.MultiIndex.from_tuples(columns)
    data_grid = pd.merge(data_grid,state_code,how='left',on='snum')
    data_grid.set_index(('localtime',''),inplace= True)
    
    data_grid = data_grid.dropna(axis=0,subset=[('state_type','')])
    
    data_grid = data_grid[data_grid['state_type']=='电网故障']

    if len(data_grid) > 0: 
        
        fault_wtid = np.unique(data_grid['fnum'])    
        
        for j in range(len(fault_wtid)):
            temp = data_grid[data_grid['fault','mymode']==fault_wtid[j]]
            #temp1 = fault_all[fault_all['fault']==fault_wtid[j]]
            faultgrid_loss.loc[j,'fault'] = fault_wtid[j]
            faultgrid_loss.loc[j,'count'] = len(temp[(temp['shift']!=0)&(temp['fnum']!=0)])
            faultgrid_loss.loc[j,'time'] = len(temp)/6.0
            faultgrid_loss.loc[j,'loss'] = np.nansum(temp[turbine_name])/6.0#kwh
            faultgrid_loss.loc[j,'wspd'] = np.nanmean(temp['wspd','nanmean'])
            if ((fault_wtid[j] in fault_code['fnum'].values)==True):
                faultgrid_loss.loc[j,'fault_describe'] = fault_code[fault_code['fnum']==fault_wtid[j]]['fname'].values[0]
            else:
                faultgrid_loss.loc[j,'fault_describe'] = fault_wtid[j]
        faultgrid_loss.insert(0, 'wtid', turbine_name)
    return faultgrid_loss

#单机技术待命损失
def Turbine_Technology_Loss(data,turbine_name,pw_df_temp,fault_code,state_code):
    Technology_loss = pd.DataFrame()
    #fault_type = fault_code[fault_code['type']=='技术待命']  
    
    data_technology = data.dropna(axis=0,subset=[('fault','mymode')])
    #列分级
    columns = []
    for elem in pw_df_temp.columns.to_list():
        if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
            pass
        else:
            columns.append((elem, ''))
    if len(columns) > 0:
        pw_df_temp.columns = pd.MultiIndex.from_tuples(columns)
    data_technology.reset_index(level=0,inplace=True)
    data_technology = pd.merge(data_technology,pw_df_temp,how='left',on='windbin')
    data_technology.set_index(('localtime',''),inplace= True)

    data_technology['fnum'] = data_technology['fault','mymode']
    data_technology['flt1'] = data_technology['fnum'].shift(periods=1,axis=0)
    data_technology['shift'] = data_technology['flt1']-data_technology['fnum']
    
    data_technology['snum'] = data_technology['statety','mymode']
    data_technology['snum1'] = data_technology['snum'].shift(periods=1,axis=0)
    data_technology['shiftss'] = data_technology['snum1']-data_technology['snum']
    
    data_technology = data_technology[data_technology[('fault','mymode')]!=0]
    data_technology.reset_index(level=0,inplace=True)
    #列分级
    columns = []
    for elem in fault_code.columns.to_list():
        if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
            pass
        else:
            columns.append((elem, ''))
    if len(columns) > 0:
        fault_code.columns = pd.MultiIndex.from_tuples(columns)
    data_technology = pd.merge(data_technology,fault_code,how='left',on='fnum')
    #列分级
    columns = []
    for elem in state_code.columns.to_list():
        if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
            pass
        else:
            columns.append((elem, ''))
    if len(columns) > 0:
        state_code.columns = pd.MultiIndex.from_tuples(columns)
    data_technology = pd.merge(data_technology,state_code,how='left',on='snum')
    data_technology.set_index(('localtime',''),inplace= True)
    
    data_technology = data_technology.dropna(axis=0,subset=[('state_type','')])
    
    data_technology = data_technology[(data_technology['state_type']=='技术待命')]
    #data_technology = data_technology[(data_technology['statety','mymode']==60)|(data_technology['type']=='技术待命')]

    if len(data_technology) > 0: 
        
        fault_wtid = np.unique(data_technology['fnum'])    
        
        for j in range(len(fault_wtid)):
            temp = data_technology[data_technology['fault','mymode']==fault_wtid[j]]
            #temp1 = fault_all[fault_all['fault']==fault_wtid[j]]
            Technology_loss.loc[j,'fault'] = fault_wtid[j]
            #Technology_loss.loc[j,'count'] = len(temp[(temp['shift']!=0)&(temp['fnum']!=0)])
            Technology_loss.loc[j,'count'] = len(temp[(temp['shift']!=0)|(temp['shiftss']!=0)])
            Technology_loss.loc[j,'time'] = len(temp)/6.0
            Technology_loss.loc[j,'loss'] = np.nansum(temp[turbine_name])/6.0#kwh
            Technology_loss.loc[j,'wspd'] = np.nanmean(temp['wspd','nanmean'])
            if ((fault_wtid[j] in fault_code['fnum'].values)==True):
                Technology_loss.loc[j,'fault_describe'] = fault_code[fault_code['fnum']==fault_wtid[j]]['fname'].values[0]
            else:
                Technology_loss.loc[j,'fault_describe'] = fault_wtid[j]
    Technology_loss.insert(0, 'wtid', turbine_name)
    return Technology_loss


##单机自限电损失输入
def Turbine_Limit_Loss(data,turbine_name,pw_df_temp,Pitch_Min,Pwrat_Rate,Rotspd_Connect,state, statetyNormal): 
    limturbine_loss = pd.DataFrame()
    data_limt = data[(data['limpw','nanmean']==statetyNormal)&(data['state','nanmean']==state)] #4：限电, 5:正常, 应填：5
    #列分级
    columns = []
    for elem in pw_df_temp.columns.to_list():
        if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
            pass
        else:
            columns.append((elem, ''))
    if len(columns) > 0:
        pw_df_temp.columns = pd.MultiIndex.from_tuples(columns)
    data_limt.reset_index(level=0,inplace=True)
    data_limt = pd.merge(data_limt,pw_df_temp,how='left',on='windbin')
    data_limt.set_index(('localtime',''),inplace= True)
    
    pw_df_lim = pd.DataFrame()
    #pw_df_lim['windbin'] = pw_df_median['windbin']+1.0  #不能用中位数，如果某台机组功率曲线特别差，则很有可能大部分散点都在下边界以下，导致计算结果不对
    #pw_df_lim['pwrat'] = pw_df_median['pwrat']*0.95
    
    pw_df_lim['windbin'] = pw_df_temp['windbin']+1.0
    pw_df_lim['pwrat_lim'] = pw_df_temp[turbine_name]*0.95
    #列分级
    columns = []
    for elem in pw_df_lim.columns.to_list():
        if '(' in str(elem) and ',' in str(elem) and ')' in str(elem):
            pass
        else:
            columns.append((elem, ''))
    if len(columns) > 0:
        pw_df_lim.columns = pd.MultiIndex.from_tuples(columns)
    data_limt = pd.merge(data_limt,pw_df_lim,how='outer',on='windbin')#,suffixes=('_org','_lim'))
    #出图展示data_limt风速功率散点、风机功率曲线、功率曲线下线
    '''
    fig = plt.figure(figsize=(10,8),dpi=100)  
    plt.title(str(turbine_name))    
    with plt.style.context('ggplot'):  
        plt.scatter(data_limt['wspd','nanmean'],data_limt['pwrat','nanmean'],color='cornflowerblue',s=10) 
        plt.scatter(data_limt['windbin'],data_limt[turbine_name],color='r')
        plt.scatter(pw_df_lim['windbin'],pw_df_lim['pwrat_lim'],color='g')
        plt.grid()
        #plt.ylim(-3,25)
        plt.xlabel('风速(m/s)',fontsize=14)
        plt.ylabel('功率(kW)',fontsize=14)
        #plt.colorbar()
    #fig.savefig(path + '/' +str(turbine_name) + '_限电2.png',dpi=100)
    '''
    #data_limt_lim = data_limt[data_limt[('clear','')]==7]
    data_limt_lim = data_limt[(data_limt['pwrat','nanmean']<data_limt['pwrat_lim'])&(data_limt['pwrat','nanmean']<Pwrat_Rate*0.8)&(data_limt['pwrat','nanmean']>100.0)&(data_limt['pitch1','nanmin']>Pitch_Min+2.5)&(data_limt['rotspd','nanmin']>Rotspd_Connect*1.05)]
    
    #print(len(data_limt_lim)/len(data_limt))
    if len(data_limt_lim)/len(data_limt) > 0.05:
        limturbine_loss.loc[turbine_name,'loss'] = np.nansum(data_limt_lim[turbine_name] - data_limt_lim['pwrat','nanmean'])/6.0
        limturbine_loss.loc[turbine_name,'time'] = len(data_limt_lim)/6.0
        limturbine_loss.loc[turbine_name,'wspd'] = np.nanmean(data_limt_lim['wspd','nanmean'])
    limturbine_loss['wtid'] = limturbine_loss.index
    return data_limt,limturbine_loss



#风速-功率散点异常离散(额定功率异常的不判别)
#一定比例的散点超出实测功率曲线正负5%
#data为清洗后的数据，功率曲线不用补全
#data_clear['windbin'] = pd.cut(data_clear['wspd','nanmean'],windbinreg,labels=windbin)
#data_clear.reset_index(level=0,inplace=True)
#data_clear = pd.merge(data_clear,pw_df,how='outer',on='windbin')
#data_clear.set_index(('localtime',''),inplace= True) 
def Wind_Power_Dissociation(data,pw_df,turbine_name):
    pw_df_up = pd.DataFrame()
    pw_df_up['windbin'] = pw_df['windbin']-1.0
    pw_df_up['pwrat_up'] = pw_df[turbine_name]*1.05    
    pw_df_down = pd.DataFrame()
    pw_df_down['windbin'] = pw_df['windbin']+1.0
    pw_df_down['pwrat_down'] = pw_df[turbine_name]*0.95 
    #列表分级
    pw_df_up.columns = pd.MultiIndex.from_tuples([('windbin', ''), ('pwrat_up', '')])
    pw_df_down.columns = pd.MultiIndex.from_tuples([('windbin', ''), ('pwrat_down', '')])
    pw_df_limit = pd.merge(pw_df,pw_df_up,how='inner',on='windbin')
    pw_df_limit = pd.merge(pw_df_limit,pw_df_down,how='inner',on='windbin')
    
    data.reset_index(level=0,inplace=True)
    data = pd.merge(data,pw_df_up,how='inner',on='windbin')
    data = pd.merge(data,pw_df_down,how='inner',on='windbin')
    data.set_index(('localtime',''),inplace= True)
    
    data['dissociation'] = 0
    data.loc[((data['pwrat','nanmean']>data['pwrat_up'])|(data['pwrat','nanmean']<data['pwrat_down'])),'dissociation'] = 1
    
    if len(data[data['dissociation']==1])/len(data)>0.2:
        return data,pw_df_limit
    else:
        data = pd.DataFrame()
        return data,pw_df_limit
    
##时间可利用率计算(包括发电及无故障待机时间)
def Time_Avail(start_time,end_time,fault_loss):
    time_avail = 1.0 - np.nansum(fault_loss['time'])/(pd.to_datetime(end_time,format='%Y-%m-%d') - pd.to_datetime(start_time,format='%Y-%m-%d')).total_seconds()/3600.0/len(np.unique(fault_loss['wtid']))
    return time_avail

#能量可利用率
def Eny_Avail(eny,fault_loss,limgrid_loss,limturbine_loss,faultgrid_loss,stop_loss,Technology_loss):
    eny_avail = eny / (eny+fault_loss+faultgrid_loss+limgrid_loss+limturbine_loss+stop_loss+Technology_loss)  
    return eny_avail
        
#MTBT计算
def MTBT_Calculate(start_time,end_time,fault_loss):
    if len(fault_loss) > 0:
        fault_count = np.sum(fault_loss['count'])
        MTBT = len(np.unique(fault_loss['wtid'])) * (pd.to_datetime(end_time,format='%Y-%m-%d') - pd.to_datetime(start_time,format='%Y-%m-%d')).total_seconds()/3600.0 / fault_count
    else:
        MTBT = len(np.unique(fault_loss['wtid'])) * (pd.to_datetime(end_time,format='%Y-%m-%d') - pd.to_datetime(start_time,format='%Y-%m-%d')).total_seconds()/3600.0
    return MTBT

#无故障时间计算
def NotFault_Time(start_time,end_time,fault_loss,faultgrid_loss):
    notfault_time = 0
    temp = pd.concat([fault_loss,faultgrid_loss])#.append(faultgrid_loss)
    day_range = pd.date_range(start_time,end_time,freq="24H",normalize=True).strftime('%Y-%m-%d %H:%M:%S').to_list()
    
    notfault_time = len(day_range) - len(np.unique(temp.index))
    return notfault_time


def abnormal_detect(data,turbine_camp,altitude,hub_high,rotor_radius,turbine_param_all,state,Pwrat_Rate,path,wtids_ses):
    data_all = pd.DataFrame()
    #('wspd','nanmean'),('wspd','nanstd'),('wdir','nanmean'),
    columns_names = [('wtid',''),('pwrat','nanmean'),('rotspd','nanmean'),('accx','nanmean'),('accy','nanmean'),
                     ('pitch1','nanmean'),('pitch2','nanmean'),('pitch3','nanmean'),('exltmp','nanmean'),
                     ('rotspd','nanstd'),('pwrat','nanstd'),('pitch1','nanstd'),('pitch2','nanstd'),('labelfen',''),#原始数据没有，为使分段后取数正常预置
                     ('pitch3','nanstd'),('accxfil','nanmean'),('accyfil','nanmean'),
                     ('mot1cur','nanmean'),('mot2cur','nanmean'),('mot3cur','nanmean'),('mot1tmp','nanmean'),
                     ('mot2tmp','nanmean'),('mot3tmp','nanmean'),('gen1tmp','nanmean'),('gen2tmp','nanmean'),
                     ('gen3tmp','nanmean'),('gen4tmp','nanmean'),('gen5tmp','nanmean'),('gen6tmp','nanmean'),
                     ('genUtmp','nanmean'),('genVtmp','nanmean'),('genWtmp','nanmean'),('gen_zcd_tmp','nanmean'),
                     ('gen_zcnd_tmp','nanmean'),('gear_msnd_tmp','nanmean'),('gear_msde_tmp','nanmean'),('gear_oil_tmp','nanmean'),
                     ('gear_lsde_tmp','nanmean'),('gear_lsnd_tmp','nanmean'),('zztmp','nanmean')]
    #######
    #wtids = turbine_camp
    #data = Df_all_m_all
    #########
    for i in range(len(turbine_camp)):
        Df_all_m = data[data['wtid'] == turbine_camp[i]]
        turbine_name = turbine_camp[i]
        Pitch_Min = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Pitch_Min'].values[0]
        Rotspd_Connect = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Rotspd_Connect'].values[0]
        Rotspd_Rate = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Rotspd_Rate'].values[0]
        
        Df_all_m_clear = data_min_clear(Df_all_m,state,Rotspd_Connect,Rotspd_Rate,Pwrat_Rate,Pitch_Min,neighbors_num=20,threshold=3)
        df_all_clear = Df_all_m_clear[Df_all_m_clear['clear'] == 2]#1为干净值
        (data_fenduan,fenduan) = FenDuan(df_all_clear,Pwrat_Rate,Rotspd_Connect,Rotspd_Rate,turbine_name)
        
        columns_names = [col for col in columns_names if col in data_fenduan.columns]
        
        data_temp = data_fenduan.loc[:,columns_names]
        #data.columns = data.columns.droplevel(1)
        data_temp.columns = ['{}_{}'.format(i[0],i[1]) for i in data_temp.columns]
        
        #altitude = Turbine_attr_type.loc[Turbine_attr_type['name']==turbine_name,'altitude']
        #data['ti'] = data['wspd_nanstd'] / data['wspd_nanmean']    
        data_temp['rho'] = 1.293*(10.0**(-(altitude+hub_high)/(18400.0*(1.0+0.003674*data_temp['exltmp_nanmean']))))/(1.0+0.003674*data_temp['exltmp_nanmean'])
        #data['cp'] =  2000.0*data['pwrat_nanmean'] / data['rho'] / (np.pi*rotor_radius*rotor_radius) / data['wspd_nanmean']**3
        data_temp['kopt'] = 1000.0*data_temp['pwrat_nanmean'] / (data_temp['rotspd_nanmean']*0.10471)**3
        
        data_all = np.concat([data_all,data_temp])#.append(data_temp)
    
    labelfen = np.unique(data_all['labelfen_'])
    abnormal_detect = pd.DataFrame()
    for num in range(len(labelfen)):
        data_all_2 = data_all[data_all['labelfen_']==labelfen[num]] 
            
        #gmm = GaussianMixture(n_components=2,covariance_type='full',random_state=0,tol=0.01,reg_covar=0.000005)
        gmm = BayesianGaussianMixture(n_components=2, covariance_type="full",random_state=0,tol=0.01,reg_covar=0.1)
        #gmm = DBSCAN(eps=0.1,min_samples=10)
        #gmm = SpectralClustering(n_clusters=2,assign_labels='discretize',eigen_solver='arpack',affinity='nearest_neighbors',n_neighbors=500,random_state=0,n_jobs=-1)
        
        data_all_2 = data_all_2.dropna(axis=0)
        wtid = data_all_2['wtid_']
        data_all_2.drop(columns=['wtid_'],inplace=True)
        xx = data_all_2.values
        min_max_scaler = preprocessing.StandardScaler()
        train_minmax = min_max_scaler.fit_transform(xx)
        #labels = gmm.fit(train_minmax).labels_
        labels = gmm.fit(train_minmax).predict(train_minmax)       
        
        x_train = data_all_2
        y_train = labels
        dtrain = xgb.DMatrix(x_train,label=y_train)
               
        # 模型
        model = xgb.XGBClassifier(booster='gbtree',
                               n_estimators=20,  # 迭代次数
                               learning_rate=0.1,  # 步长
                               max_depth=5,  # 树的最大深度
                               min_child_weight=1,  # 决定最小叶子节点样本权重和
                               subsample=0.8,  # 每个决策树所用的子样本占总样本的比例（作用于样本）
                               colsample_bytree=0.8,  # 建立树时对特征随机采样的比例（作用于特征）典型值：0.5-1
                               #nthread=4,  # 线程数
                               seed=27,  # 指定随机种子，为了复现结果
                               # num_class=4,  # 标签类别数
                               # objective='multi:softmax',  # 多分类
                               )

        
        model.fit(x_train,y_train)
        y_pred = model.predict(x_train)
        predictions = [round(value) for value in y_pred]
        # 计算预测准确率
        accuracy = accuracy_score(y_train, predictions)
        print("Accuracy: %.2f%%" % (accuracy * 100.0))
        '''
        ##特征重要性
        xgb.plot_importance(model)
        plt.title('Feature Importance')
        plt.show
        
        weight
        xgb.plot_importance 这是我们常用的绘制特征重要性的函数方法。其背后用到的贡献度计算方法为weight。
        
        ‘weight’ - the number of times a feature is used to split the data across all trees.
        简单来说，就是在子树模型分裂时，用到的特征次数。这里计算的是所有的树。这个指标在R包里也被称为frequency2。
        
        gain
        model.feature_importances_ 这是我们调用特征重要性数值时，用到的默认函数方法。其背后用到的贡献度计算方法为gain。
        
        ‘gain’ - the average gain across all splits the feature is used in.
        gain 是信息增益的泛化概念。这里是指，节点分裂时，该特征带来信息增益（目标函数）优化的平均值。
        
        cover
        model = XGBRFClassifier(importance_type = 'cover') 这个计算方法，需要在定义模型时定义。之后再调用model.feature_importances_ 得到的便是基于cover得到的贡献度。
        
        ‘cover’ - the average coverage across all splits the feature is used in.
        cover 形象来说，就是树模型在分裂时，特征下的叶子结点涵盖的样本数除以特征用来分裂的次数。分裂越靠近根部，cover 值越大。
        # Available importance_types = ['weight', 'gain', 'cover', 'total_gain', 'total_cover']
        f = 'gain'
        xgb.XGBClassifier.get_booster().get_score(importance_type= f)
        '''
        ##特征重要性
        feature_import = pd.DataFrame()
        feature_import['score'] = model.feature_importances_
        feature_import.index = x_train.columns
                
        fig = plt.figure(figsize=(10,8),dpi=100)
        plt.title(str('feature_import')) 
        plt.subplot(1,1,1)
        with plt.style.context('ggplot'): 
            plt.barh(feature_import.index,feature_import['score'],height=0.5,color='deepskyblue')   
            plt.ylabel('features', fontsize=20,fontweight='bold')
            plt.xlabel('feature_import', fontsize=20,fontweight='bold')
        plt.savefig(path + '/' +str(str(wtids_ses)+'_分段'+str(num+1)+'_feature_import.png'), bbox_inches='tight', dpi=100)
            
        data_all_2['label'] = labels
        data_all_2['wtid'] = wtid
        
        temp_label0 = data_all_2[data_all_2['label']==0]
        abnormal_detect_label0 = pd.DataFrame()
        temp0 = temp_label0['wtid'].value_counts()/len(temp_label0)
        #abnormal_detect_label0['fenduan'] = labelfen[num]
        abnormal_detect_label0['wtid'] = temp0.index
        abnormal_detect_label0['label0'] = temp0.values
        
        
        temp_label1 = data_all_2[data_all_2['label']==1]
        abnormal_detect_label1 = pd.DataFrame()
        temp1 = temp_label1['wtid'].value_counts()/len(temp_label1)
        #abnormal_detect_label0['fenduan'] = labelfen[num]
        abnormal_detect_label1['wtid'] = temp1.index
        abnormal_detect_label1['label1'] = temp1.values
        
        abnormal_detect_label = pd.merge(abnormal_detect_label0,abnormal_detect_label1,how='outer',on='wtid')
        abnormal_detect_label['fenduan'] = labelfen[num]
       
        columns_temp = feature_import['score'].nlargest(3).index
        for ice in range(len(columns_temp)):
            fig = plt.figure(figsize=(10,8),dpi=100)
            plt.title(str('feature_import')) 
            plt.subplot(1,1,1)
            plt.title(str(wtids_ses))    
            with plt.style.context('ggplot'):
                #plt.plot(biaozhun['wspd'],biaozhun['pwrat'],color='red')
                temp0 = data_all_2[(data_all_2['wtid']==turbine_camp[0])&(data_all_2['label']==0)]
                temp1 = data_all_2[(data_all_2['wtid']==turbine_camp[0])&(data_all_2['label']==1)]
                plt.scatter(temp0['pwrat_nanmean'],temp0[columns_temp[ice]],color='red',marker='o',s=10,alpha=1,label=turbine_camp[0])
                plt.scatter(temp1['pwrat_nanmean'],temp1[columns_temp[ice]],color='red',marker='^',s=20,alpha=1)
                temp0 = data_all_2[(data_all_2['wtid']==turbine_camp[1])&(data_all_2['label']==0)]
                temp1 = data_all_2[(data_all_2['wtid']==turbine_camp[1])&(data_all_2['label']==1)]
                plt.scatter(temp0['pwrat_nanmean'],temp0[columns_temp[ice]],color='limegreen',marker='o',s=10,alpha=1,label=turbine_camp[1])
                plt.scatter(temp1['pwrat_nanmean'],temp1[columns_temp[ice]],color='limegreen',marker='^',s=20,alpha=1)
                temp0 = data_all_2[(data_all_2['wtid']==turbine_camp[2])&(data_all_2['label']==0)]
                temp1 = data_all_2[(data_all_2['wtid']==turbine_camp[2])&(data_all_2['label']==1)]
                plt.scatter(temp0['pwrat_nanmean'],temp0[columns_temp[ice]],color='dodgerblue',marker='o',s=10,alpha=1,label=turbine_camp[2])
                plt.scatter(temp1['pwrat_nanmean'],temp1[columns_temp[ice]],color='dodgerblue',marker='^',s=20,alpha=1)
                plt.grid()
                #plt.xlim(0,25)
                #plt.ylim(0,Pwrat_Rate*1.1)
                plt.xlabel('active power',fontsize=14)
                plt.ylabel(columns_temp[ice],fontsize=14)
                plt.legend(loc=0,fontsize=14) 
            plt.savefig(path + '/' +str(str(wtids_ses)+'_分段'+str(num+1)+'_'+str(columns_temp[ice])+'.png'), bbox_inches='tight', dpi=100)
            
        abnormal_detect = pd.concat([abnormal_detect,abnormal_detect_label])#.append(abnormal_detect_label)
    return abnormal_detect

def abnormal_detect_low(data,turbine_camp,altitude,hub_high,rotor_radius,turbine_param_all,state,Pwrat_Rate,path,wtids_ses):
    data_all = pd.DataFrame()
    #('wspd','nanmean'),('wspd','nanstd'),('wdir','nanmean'),
    columns_names = [('wtid',''),('pwrat','nanmean'),('rotspd','nanmean'),('accx','nanmean'),('accy','nanmean'),
                     ('pitch1','nanmean'),('pitch2','nanmean'),('pitch3','nanmean'),('exltmp','nanmean'),
                     ('rotspd','nanstd'),('pwrat','nanstd'),('pitch1','nanstd'),('pitch2','nanstd'),('labelfen',''),#原始数据没有，为使分段后取数正常预置
                     ('pitch3','nanstd'),('accxfil','nanmean'),('accyfil','nanmean'),
                     ('mot1cur','nanmean'),('mot2cur','nanmean'),('mot3cur','nanmean'),('mot1tmp','nanmean'),
                     ('mot2tmp','nanmean'),('mot3tmp','nanmean'),('gen1tmp','nanmean'),('gen2tmp','nanmean'),
                     ('gen3tmp','nanmean'),('gen4tmp','nanmean'),('gen5tmp','nanmean'),('gen6tmp','nanmean'),
                     ('genUtmp','nanmean'),('genVtmp','nanmean'),('genWtmp','nanmean'),('gen_zcd_tmp','nanmean'),
                     ('gen_zcnd_tmp','nanmean'),('gear_msnd_tmp','nanmean'),('gear_msde_tmp','nanmean'),('gear_oil_tmp','nanmean'),
                     ('gear_lsde_tmp','nanmean'),('gear_lsnd_tmp','nanmean'),('zztmp','nanmean')]
    #######
    #wtids = turbine_camp
    #data = Df_all_m_all
    #########
    for i in range(len(turbine_camp)):
        Df_all_m = data[data['wtid'] == turbine_camp[i]]
        turbine_name = turbine_camp[i]
        Pitch_Min = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Pitch_Min'].values[0]
        Rotspd_Connect = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Rotspd_Connect'].values[0]
        Rotspd_Rate = turbine_param_all.loc[turbine_param_all['wtid']==turbine_name,'Rotspd_Rate'].values[0]
        
        Df_all_m_clear = data_min_clear(Df_all_m,state,Rotspd_Connect,Rotspd_Rate,Pwrat_Rate,Pitch_Min,neighbors_num=20,threshold=3)
        df_all_clear = Df_all_m_clear[Df_all_m_clear['clear'] == 2]#1为干净值
        (data_fenduan,fenduan) = FenDuan(df_all_clear,Pwrat_Rate,Rotspd_Connect,Rotspd_Rate,turbine_name)
        
        columns_names = [col for col in columns_names if col in data_fenduan.columns]
        
        data_temp = data_fenduan.loc[:,columns_names]
        #data.columns = data.columns.droplevel(1)
        data_temp.columns = ['{}_{}'.format(i[0],i[1]) for i in data_temp.columns]
        
        #altitude = Turbine_attr_type.loc[Turbine_attr_type['name']==turbine_name,'altitude']
        #data['ti'] = data['wspd_nanstd'] / data['wspd_nanmean']    
        data_temp['rho'] = 1.293*(10.0**(-(altitude+hub_high)/(18400.0*(1.0+0.003674*data_temp['exltmp_nanmean']))))/(1.0+0.003674*data_temp['exltmp_nanmean'])
        #data['cp'] =  2000.0*data['pwrat_nanmean'] / data['rho'] / (np.pi*rotor_radius*rotor_radius) / data['wspd_nanmean']**3
        data_temp['kopt'] = 1000.0*data_temp['pwrat_nanmean'] / (data_temp['rotspd_nanmean']*0.10471)**3
        
        data_all = pd.concat([data_all,data_temp])#.append(data_temp)
    data_all.loc[data_all['labelfen_']==2,'labelfen_'] = 1 #额定转速
    data_all.loc[data_all['labelfen_']==4,'labelfen_'] = 3 #额定功率
    labelfen = np.unique(data_all['labelfen_'])
    abnormal_data = pd.DataFrame(columns=['wtid', 'device', 'picture'])
    # abnormal_data.columns = ['wtid', 'device', 'picture']
    labelfen = np.sort(labelfen)[::-1]
    for num in range(len(labelfen)):
        data_all_2 = data_all[data_all['labelfen_']==labelfen[num]] 
        wtid = data_all_2['wtid_']
        data_all_2 = data_all_2.dropna(axis=1,thresh=int(len(data_all_2)*0.1))#某列非空值数量小于总数的10%剔除该列
       
        columns_temp = data_all_2.columns
        for col in range(len(columns_temp)):
            if ((np.char.find(columns_temp[col],'tmp')!=-1)|(np.char.find(columns_temp[col],'acc')!=-1)|(np.char.find(columns_temp[col],'cur')!=-1)|(np.char.find(columns_temp[col],'kopt')!=-1)):
                columnLevel0List = columns_temp[col].split('_')
                columnLevel0 = ''
                #去掉最后一个nanmean
                for labelIndex in range(len(columnLevel0List)-1):
                    if columnLevel0 == '':
                        columnLevel0 += columnLevel0 + columnLevel0List[labelIndex]
                    else:
                        columnLevel0 += columnLevel0 + '_' + columnLevel0List[labelIndex]
                if len(columnLevel0List) == 1:
                    columnLevel0 = columns_temp[col]
                #print(columns_temp[col])
                #column = str('\'' + columns[col]+'\''+','+'\''+'nanmean'+'\'')                    
                fig  = plt.figure(figsize=(10,8),dpi=100)  
                plt.title(str(turbine_name))    
                with plt.style.context('ggplot'):  
                    temp0 = data_all_2[(data_all_2['wtid_']==turbine_camp[0])]
                    plt.scatter(temp0['pwrat_nanmean'],temp0[columns_temp[col]],color='red',marker='o',s=10,alpha=1,label=turbine_camp[0])
                    #xMeanPwrat0 = np.mean(temp0['pwrat_nanmean'])
                    yMeanPwrat0 = np.mean(temp0[columns_temp[col]])
                    #xStdPwrat0 = np.std(temp0['pwrat_nanmean'])
                    yStdPwrat0 = np.std(temp0[columns_temp[col]])
                    
                    temp0 = data_all_2[(data_all_2['wtid_']==turbine_camp[1])]
                    plt.scatter(temp0['pwrat_nanmean'],temp0[columns_temp[col]],color='limegreen',marker='o',s=10,alpha=1,label=turbine_camp[1])
                    #xMeanPwrat1 = np.mean(temp0['pwrat_nanmean'])
                    yMeanPwrat1 = np.mean(temp0[columns_temp[col]])
                    #xStdPwrat1 = np.std(temp0['pwrat_nanmean'])
                    yStdPwrat1 = np.std(temp0[columns_temp[col]])
                    
                    
                    temp0 = data_all_2[(data_all_2['wtid_']==turbine_camp[2])]
                    plt.scatter(temp0['pwrat_nanmean'],temp0[columns_temp[col]],color='dodgerblue',marker='o',s=10,alpha=1,label=turbine_camp[2])
                    #xMeanPwrat2 = np.mean(temp0['pwrat_nanmean'])
                    yMeanPwrat2 = np.mean(temp0[columns_temp[col]])
                    #xStdPwrat2 = np.std(temp0['pwrat_nanmean'])
                    yStdPwrat2 = np.std(temp0[columns_temp[col]])
                    
                    plt.grid()
                    #plt.xlim(0,25)
                    plt.xlabel('功率',fontsize=14, color='#ccc')
                    plt.ylabel(algConfig['record_pwrt_picture']['ai_chinese_name'][columnLevel0],fontsize=14, color='#ccc')
                    plt.tick_params(which='both',labelcolor='#ccc', width=0,color='#426977', labelsize=20,gridOn=True,grid_color='#426977',direction ='in',right=True)
                    plt.legend(loc=4,fontsize=14) 
                    plt.gca().spines["left"].set_color('#426977')
                    plt.gca().spines["bottom"].set_color('#426977')
                    plt.gca().spines["right"].set_color('#426977')
                    plt.gca().spines["top"].set_color('#426977')
                plt.savefig(path + '/' +str(str(wtids_ses)+'_分段'+str(num+1)+'_'+str(columns_temp[col])+'_功率.png'), transparent=True, bbox_inches='tight', dpi=100)
                
                fig  = plt.figure(figsize=(10,8),dpi=100)
                plt.title(str(turbine_name))    
                with plt.style.context('ggplot'):  
                    temp0 = data_all_2[(data_all_2['wtid_']==turbine_camp[0])]
                    plt.scatter(temp0['rotspd_nanmean'],temp0[columns_temp[col]],color='red',marker='o',s=10,alpha=1,label=turbine_camp[0])
                    #xMeanRotspd0 = np.mean(temp0['pwrat_nanmean'])
                    yMeanRotspd0 = np.mean(temp0[columns_temp[col]])
                    #xStdRotspd0 = np.std(temp0['pwrat_nanmean'])
                    yStdRotspd0 = np.std(temp0[columns_temp[col]])
                    
                    temp0 = data_all_2[(data_all_2['wtid_']==turbine_camp[1])]
                    plt.scatter(temp0['rotspd_nanmean'],temp0[columns_temp[col]],color='limegreen',marker='o',s=10,alpha=1,label=turbine_camp[1])
                    #xMeanRotspd1 = np.mean(temp0['pwrat_nanmean'])
                    yMeanRotspd1 = np.mean(temp0[columns_temp[col]])
                    #xStdRotspd1 = np.std(temp0['pwrat_nanmean'])
                    yStdRotspd1 = np.std(temp0[columns_temp[col]])
                    
                    temp0 = data_all_2[(data_all_2['wtid_']==turbine_camp[2])]
                    plt.scatter(temp0['rotspd_nanmean'],temp0[columns_temp[col]],color='dodgerblue',marker='o',s=10,alpha=1,label=turbine_camp[2])
                    #xMeanRotspd2 = np.mean(temp0['pwrat_nanmean'])
                    yMeanRotspd2 = np.mean(temp0[columns_temp[col]])
                    #xStdRotspd2 = np.std(temp0['pwrat_nanmean'])
                    yStdRotspd2 = np.std(temp0[columns_temp[col]])
                    
                    plt.grid()
                    #plt.xlim(0,25)
                    plt.xlabel('转速',fontsize=14, color='#ccc')
                    plt.ylabel(algConfig['record_pwrt_picture']['ai_chinese_name'][columnLevel0],fontsize=14, color='#ccc')
                    plt.tick_params(which='both',labelcolor='#ccc', width=0,color='#426977', labelsize=20,gridOn=True,grid_color='#426977',direction ='in',right=True)
                    plt.legend(loc=4,fontsize=14) 
                    plt.gca().spines["left"].set_color('#426977')
                    plt.gca().spines["bottom"].set_color('#426977')
                    plt.gca().spines["right"].set_color('#426977')
                    plt.gca().spines["top"].set_color('#426977')
                plt.savefig(path + '/' +str(str(wtids_ses)+'_分段'+str(num+1)+'_'+str(columns_temp[col])+'_转速.png'), transparent=True, bbox_inches='tight', dpi=100)

                abnormal_turbine = None
                flag = None
                multiStd = 3
                ############################################
                #功率图
                ############################################
                #y中心点排序
                arrMeanPwrat = np.array([yMeanPwrat0, yMeanPwrat1, yMeanPwrat2])
                arrStdPwrat = np.array([yStdPwrat0, yStdPwrat1, yStdPwrat2])
                sortIndexMeanPwrat = np.argsort(arrMeanPwrat)
                #y交集大小，没有交集时负数表示，负数越大距离越远
                interTopMid = (arrMeanPwrat[sortIndexMeanPwrat[1]]+multiStd*arrStdPwrat[sortIndexMeanPwrat[1]]) - (arrMeanPwrat[sortIndexMeanPwrat[2]]-multiStd*arrStdPwrat[sortIndexMeanPwrat[2]])
                interTopBottom = (arrMeanPwrat[sortIndexMeanPwrat[0]]+multiStd*arrStdPwrat[sortIndexMeanPwrat[0]]) - (arrMeanPwrat[sortIndexMeanPwrat[2]]-multiStd*arrStdPwrat[sortIndexMeanPwrat[2]])
                interMidBottom = (arrMeanPwrat[sortIndexMeanPwrat[0]]+multiStd*arrStdPwrat[sortIndexMeanPwrat[0]]) - (arrMeanPwrat[sortIndexMeanPwrat[1]]-multiStd*arrStdPwrat[sortIndexMeanPwrat[1]])
                
                #判断是否存在故障，即交集存在负数, 前提假设std都相似
                if ((interTopMid<0 and interTopBottom<0) or (interMidBottom<0 and interTopMid<0) or (interMidBottom<0 and interTopBottom<0)):
                    #找负数最小的两个交集后，两个交集共有的机子就是故障机
                    arrDist = np.array([interTopMid, interTopBottom, interMidBottom])
                    sortDistIndex = np.argsort(arrDist)
                    if sortDistIndex[0] == 0 and sortDistIndex[1] == 1:
                        abnormal_turbine = turbine_camp[sortIndexMeanPwrat[2]]
                    elif sortDistIndex[0] == 0 and sortDistIndex[1] == 2:
                        abnormal_turbine = turbine_camp[sortIndexMeanPwrat[1]]
                    elif sortDistIndex[0] == 1 and sortDistIndex[1] == 2:
                        abnormal_turbine = turbine_camp[sortIndexMeanPwrat[0]]
                    elif sortDistIndex[0] == 1 and sortDistIndex[1] == 0:
                        abnormal_turbine = turbine_camp[sortIndexMeanPwrat[2]]
                    elif sortDistIndex[0] == 2 and sortDistIndex[1] == 0:
                        abnormal_turbine = turbine_camp[sortIndexMeanPwrat[1]]
                    elif sortDistIndex[0] == 2 and sortDistIndex[1] == 1:
                        abnormal_turbine = turbine_camp[sortIndexMeanPwrat[0]]
                    flag = True
                ############################################
                #转速图
                ############################################ 
                if abnormal_turbine == None:
                    #y中心点排序
                    arrMeanRotspd = np.array([yMeanRotspd0, yMeanRotspd1, yMeanRotspd2])
                    arrStdRotspd = np.array([yStdRotspd0, yStdRotspd1, yStdRotspd2])
                    sortIndexMeanRotspd = np.argsort(arrMeanRotspd)
                    #y交集大小，没有交集时负数表示，负数越大距离越远
                    interTopMid = (arrMeanRotspd[sortIndexMeanRotspd[1]]+multiStd*arrStdRotspd[sortIndexMeanRotspd[1]]) - (arrMeanRotspd[sortIndexMeanRotspd[2]]-multiStd*arrStdRotspd[sortIndexMeanRotspd[2]])
                    interTopBottom = (arrMeanRotspd[sortIndexMeanRotspd[0]]+multiStd*arrStdRotspd[sortIndexMeanRotspd[0]]) - (arrMeanRotspd[sortIndexMeanRotspd[2]]-multiStd*arrStdRotspd[sortIndexMeanRotspd[2]])
                    interMidBottom = (arrMeanRotspd[sortIndexMeanRotspd[0]]+multiStd*arrStdRotspd[sortIndexMeanRotspd[0]]) - (arrMeanRotspd[sortIndexMeanRotspd[1]]-multiStd*arrStdRotspd[sortIndexMeanRotspd[1]])
                    
                    #判断是否存在故障，即交集存在负数, 前提假设std都相似
                    if ((interTopMid<0 and interTopBottom<0) or (interMidBottom<0 and interTopMid<0) or (interMidBottom<0 and interTopBottom<0)):
                        #找负数最小的两个交集后，两个交集共有的机子就是故障机
                        arrDist = np.array([interTopMid, interTopBottom, interMidBottom])
                        sortDistIndex = np.argsort(arrDist)
                        if sortDistIndex[0] == 0 and sortDistIndex[1] == 1:
                            abnormal_turbine = turbine_camp[sortIndexMeanRotspd[2]]
                        elif sortDistIndex[0] == 0 and sortDistIndex[1] == 2:
                            abnormal_turbine = turbine_camp[sortIndexMeanRotspd[1]]
                        elif sortDistIndex[0] == 1 and sortDistIndex[1] == 2:
                            abnormal_turbine = turbine_camp[sortIndexMeanRotspd[0]]
                        elif sortDistIndex[0] == 1 and sortDistIndex[1] == 0:
                            abnormal_turbine = turbine_camp[sortIndexMeanRotspd[2]]
                        elif sortDistIndex[0] == 2 and sortDistIndex[1] == 0:
                            abnormal_turbine = turbine_camp[sortIndexMeanRotspd[1]]
                        elif sortDistIndex[0] == 2 and sortDistIndex[1] == 1:
                            abnormal_turbine = turbine_camp[sortIndexMeanRotspd[0]]
                        flag = False
                ###############################
                #记录部件故障
                ###############################
                if abnormal_turbine != None and flag == True:
                    condition = (abnormal_data['wtid']==abnormal_turbine) & (abnormal_data['device'] == columnLevel0)
                    if abnormal_data.loc[condition].shape[0] == 0:
                        tmp = pd.DataFrame()
                        tmp['wtid'] = abnormal_turbine
                        tmp['device'] = columnLevel0
                        tmp['picture'] = path + '/' +str(str(wtids_ses)+'_分段'+str(num+1)+'_'+str(columns_temp[col])+'_功率.png')
                        pd.concat([abnormal_data, tmp]) 
                elif abnormal_turbine != None and flag == False:
                    condition = (abnormal_data['wtid']==abnormal_turbine) & (abnormal_data['device'] == columnLevel0)
                    if abnormal_data.loc[condition].shape[0] == 0:
                        tmp = pd.DataFrame()
                        tmp['wtid'] = abnormal_turbine
                        tmp['device'] = columnLevel0
                        tmp['picture'] = path + '/' +str(str(wtids_ses)+'_分段'+str(num+1)+'_'+str(columns_temp[col])+'_转速.png')
                        pd.concat([abnormal_data, tmp]) #loc[abnormal_turbine, columnLevel0] = path + '/' +str(str(wtids_ses)+'_分段'+str(num+1)+'_'+str(columns_temp[col])+'_功率.png')
                    
    return abnormal_data
    
    
    
    
    
    
    
    
    
    
    
    
    
