import data.efficiency_function as turbine_efficiency_function
from scipy import signal

def filter1(Df_all, Df_all_m):
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

    return Df_all, Df_all_m

def filter2(Df_all, Df_all_m, medfilt_num, filter1st_tao, choose_num):
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
    
    return Df_all, Df_all_m

import pandas as pd
import numpy as np
#故障码
fnum = pd.Series([])

#故障描述
fname = pd.Series([])

#故障部位，系统
fsyst = pd.Series([])

fault = pd.DataFrame({'fname':fname,'fnum':fnum.astype(int),'fsyst':fsyst})
fault['fsyst'] = fault['fname']

#故障索引号， 故障码（状态码）
snum = pd.Series([])

#故障组织，厂家风机状态
sname_org = pd.Series([])

#故障码原因，监控状态
sname = pd.Series([])

state = pd.DataFrame({'type_org':sname_org,'snum':snum.astype(int),'state_type':sname})

# 并网转速
Rotspd_Connect = 1074.2 
# 额定转速
Rotspd_Rate = 1755.0 
# 转矩控制系数

# 最小桨距角
Pitch_Min = -0.5

# 并网状态
state_ = 5
#限功率状态
limpw_state = 80 #统一状态替换
#正常发电状态
stateNormal = 71

# # 合计风速
wspd = pd.Series([3. , 3.5, 4. , 4.5, 5. , 5.5, 6. , 6.5, 7. , 7.5, 8. , 8.5, 9. ,
                  9.5,10. ,10.5,11. ,11.5,12. ,12.5,13. ,13.5,14. ,14.5,15. ,15.5,16. ,
                  16.5,17. ,17.5,18. ,18.5,19. ,19.5,20. ,20.5,21. ,21.5,22. ,22.5,23. ,
                  23.5,24. ,24.5,25. ])
# # 合计功率
pwrat = pd.Series([82.9268983640725,211.668043505953,380.430463874846,578.018055610271,
                   806.258679758194,1075.72335386021,1396.87062422361,1772.39038244023,
                   2204.25905148393,2689.92574885952,3175.20934602753,3631.26338609218,
                   4019.15321346442,4342.28857348341,4591.08420339705,4774.80924338475,
                   4885.72134358665,4942.70409051099,4976.12517848215,4992.72980237122,5000,
                   5000,5000,5000,5000,5000,5000,5000,5000,5000,5000,5000,5000,5000,5000,
                   4800,4600,4400,4200,4000,3800,3600,3400,3200,3000])