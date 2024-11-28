# -*- coding: utf-8 -*-
import pandas as pd
from faultcode.faultcode_MINYANG_GANSU_QINGSHUI import pwrat as  pwrat_
from faultcode.faultcode_MINYANG_GANSU_QINGSHUI import wspd as wspd_
"""
常量值
"""

# 应用风场的assetId
Wind_Farm = 'rWlbaEnM' #甘肃清水
Wind_Farm_Name = '甘肃清水' #甘肃清水


wspd = wspd_
pwrat = pwrat_

#模块附加区分名
extraModelName = None#'B'

# 告警推送url
# Alarm_Push_Url = 'https://xxxx/warn/receiveWarn' 10.67.68.188:9001 or 168.0.0.251:9001
Alarm_Push_Url = 'http://10.67.68.188:9001/api-smartisolar/sub-warn/warn/receiveWarnInfo'
# Alarm_Push_Url = 'http://10.67.68.188:9001/api/v1/standardAlarmGateway/attributes'






AccessKey = '3f207c85-64b4-476c-a23d-64624bbc0669'
SecretKey = 'f30502cc-2b51-41d0-a95b-6275d609e5bf'
OrgId = "o16323808037221371"
GW_Url = 'https://ag-spic1.eniot.io'

# 告警推送目标 db/http
# ALARM_PUSH_MODE = 'db'
ALARM_PUSH_MODE = 'http'

# # 向自己的服务器推送警告
# AL_PUSH_URL_SELF = "http://127.0.0.1:8088/iwind-edge-api/base/DataAlarm/addAlarm"
# # 向自己的服务器推送超过阈值
# OV_PUSH_URL_THR = "" #"http://127.0.0.1:8088/iwind-edge-api/base/DataAlarm/upPnState"

# 数据展示存放目录
# Path = '/opt/app/wind-algorithm-model/fileData/'#'/opt/app/wind-algorithm-model/fileData/'#'./result/Display/'#/opt/app/wind-algorithm-model/fileData
# Path = './result/Display/'#'/opt/app/wind-algorithm-model/fileData/'#'./result/Display/'#/opt/app/wind-algorithm-model/fileData

# mysql数据库配置

# DB_HOST = '10.191.2.240'#'172.17.11.95' #'127.0.0.1'
# DB_USERNAME = 'root'#'iwind2'
# DB_PORT = 9906
# DB_PASSWORD = 'U_zT-oLij23_D3Ld'
# DB_DATABASE = 'cloud_core'

DB_HOST = '127.0.0.1'#'172.17.11.95' #'127.0.0.1'
DB_USERNAME = 'root'#'iwind2'
DB_PORT = 13306
DB_PASSWORD = 'Zhang123.'
DB_DATABASE = 'cloud_core'

# AL_PORT = 8088

KKS_DEVICE = {
    "7abOi7vl" : "kksSJJFJ0000000001",
    "X4sahNnm" : "kksSJJFJ0000000002",
    "KlE0ZgCn" : "kksSJJFJ0000000003",
    "8lK7h1Hn" : "kksSJJFJ0000000004",
    "NBuLy66Q" : "kksSJJFJ0000000005",
    "agnLwAui" : "kksSJJFJ0000000006",
    "GEVBFQ6T" : "kksSJJFJ0000000007",
    "IY2BuUEV" : "kksSJJFJ0000000008",
    "o4TKefAk" : "kksSJJFJ0000000009",
    "RSPazngf" : "kksSJJFJ0000000010",
    "6Sx893Sv" : "kksSJJFJ0000000011",
    "kYduHbsq" : "kksSJJFJ0000000012",
    "yQJdcdDQ" : "kksSJJFJ0000000013",
    "ELKsH15I" : "kksSJJFJ0000000014",
    "fjCNdWFa" : "kksSJJFJ0000000015",
    "6DjPkDvu" : "kksSJJFJ0000000016",
    "xry27k3m" : "kksSJJFJ0000000017",
    "C11Pi21g" : "kksSJJFJ0000000018",
    "yQ1nDyVL" : "kksSJJFJ0000000019",
    "6fVEVWaM" : "kksSJJFJ0000000020"
}


POSITION_CONFIG = {
   'WROT.Blade1Position': "轮毂", #桨叶角度
   'WNAC.TemOut': "舱外", #舱外温度
   'WGEN.GenActivePW': "发电机",#发电机有功功率
   'WTRM.TemGeaLSND': "齿轮箱内部",#齿轮箱低速轴非驱动端轴承温度
   'WTRM.TemGeaMSND': "齿轮箱内部",#齿轮箱高速轴非驱动端轴承温度
   'WTRM.TemGeaOil': "齿轮箱内部",#齿轮箱油池温度
   'WNAC.TemNacelleCab': "机舱控制柜",#机舱控制柜温度
   'WNAC.TemNacelle': "机舱内",#舱内温度	
   'WGEN.TemGenNonDE': "发电机",#发电机非驱动端轴承温度	
   'WGEN.TemGenDriEnd': "发电机",#发电机驱动端轴承温度	
   'WGEN.TemGenStaU': "发电机",#发电机定子U相线圈温度
   'WGEN.GenSpd': "发电机",#发电机转速	
   'LOW_A_TMP': "箱变",#箱变温度检测：低压侧A相温度	
   'Vibration_Strength': "塔筒顶部",#塔筒振动强度
   'BLADE1_BLOT_ANGLE_1': "叶根螺栓",#螺栓检测：叶片1法兰1号螺栓反旋角度
   'WROT.PtCapTemBl1': "轮毂电容柜",#叶片1超级电容柜温度	
   'WROT.TemB1Mot': "变桨电机",#1号桨电机温度	
   'WROT.TemBlade1Inver': "轮毂变桨柜",#变桨驱动器1温度	
   'WNAC.WindVaneDirection': "机舱顶部",#机舱与风向夹角	
   'TOAT': "塔基",#塔底倾斜角度
   'BLD_DEFECTZ_FACTOR': "桨叶"#叶片缺陷系数	

}

# 执行时排除的模型列表
EXCEPT_MODLES = ["a", "b", "c"] #"tatong_qingjiao", ,  "luoshuansongdong", "hongwaicewen", 'WROT.Blade1Position','WNAC.WindSpeed',
# EXCEPT_MODLES = ['chilunxiang_disu_zhoucheng_temperature',
#                  'chilunxiang_gaosu_zhoucheng_temperature',
#                  'chilunxiang_sanre',
#                  'generator_houzhoucheng_temperature',
#                  'generator_qianzhoucheng_temperature',
#                  'generator_zhuzhou_rpm_not_balance',
#                  'oar_electric_capacity_temperature',
#                  'oar_machine_temperature']

#schedule
scheduleConfig = {
    # "minunte-1": ['tatong_qingjiao', 'hongwaicewen'],
    # "minunte-2":['jiegou_sunshang', 'chuandonglian'],
    # "hour":['weathercock_freeze','blade_freeze', 'hongwai_sanxiang','pianhang_duifeng_buzheng', 'generator_zhuanju_kongzhi', 'luoshuansongdong'],
    # "halfday-1":['pianhang_duifeng_buzheng'],
    # "halfday-2":['generator_zhuanju_kongzhi'],
    "day-1":["record_pwrt_picture"],
    "day-2":["record_loss_indicator"],
    # "clock-1":['blade_angle_not_balance','wind_speed_fault','capacity_reduction','chilunxiang_disu_zhoucheng_temperature','chilunxiang_gaosu_zhoucheng_temperature','chilunxiang_sanre','engine_cabinet_temperature','engine_env_temperature','generator_houzhoucheng_temperature','generator_qianzhoucheng_temperature','generator_raozu_not_balance','generator_temperature','generator_zhuzhou_rpm_not_balance','oar_electric_capacity_temperature','oar_engine_performance','oar_engine_temperature','oar_machine_temperature', 'yepian_kailie'],
    # "clock-2":['Efficiency_ana_V3']
}

# turbine number setup
turbineConfig = {
    'turbineNameList' : ['#07', '#06']#None #["#01","#10"]
}

#各算法测点配置
algConfig = {
    'record_pwrt_picture':{
        'name' : '10天调度记录功率数据和结果图片',
        # 把所需测点定义到每个算法里
        'ai_points' : [
            "WNAC.WindSpeed", "WGEN.GenActivePW", "WWPP.APProduction", "WWPP.APConsumed", "WROT.Blade1Position", "WROT.Blade2Position", "WROT.Blade3Position", "WROT.Blade1Speed", "WROT.Blade2Speed", "WROT.Blade3Speed", "WYAW.NacellePosition", "WYAW.YawSpeed", "WVIB.VibrationValid", "WVIB.VibrationV", "WVIB.VibrationVFil", "WVIB.VibrationL", "WVIB.VibrationLFil", "WGEN.GenSpdInstant", "WNAC.WindVaneDirection", "WNAC.WindDirection1", "WNAC.WindDirection", "WROT.TemB1Mot", "WROT.TemB2Mot", "WROT.TemB3Mot", "WROT.CurBlade1Motor", "WROT.CurBlade2Motor", "WROT.CurBlade3Motor", "WROT.PtCptTmpBl1", "WROT.PtCptTmpBl2", "WROT.PtCptTmpBl3", "WGEN.TemGenDriEnd", "WGEN.TemGenNonDE",
            "WNAC.TemNacelle", "WNAC.TemOut", "WGEN.GenSenTmp1", "WGEN.GenSenTmp2", "WGEN.GenSenTmp3", "WGEN.GenSenTmp4", "WGEN.GenSenTmp5", "WGEN.GenSenTmp6", "WYAW.YawMotor1RunTime", "WYAW.YawMotor2RunTime", "WYAW.YawMotor3RunTime", "WROT.VolB1Cap", "WROT.VolB2Cap", "WROT.VolB3Cap", "WGEN.GenSenMaxTmp", "WGEN.GenSpd", "WTUR.MainFaultCode", "WTRM.HubAngle", "WTRM.RotorSpd", "WNAC.TemNacelleCab", "WCNV.CVTTemWaterCoolInlet", "WCNV.CVTTemWaterCoolOutlet", "WTRM.TemMainBearing", "WYAW.YawCountSum", "WTUR.SITURAI17", "WNAC.XDNACAI01", "WNAC.WindDirectionInstant", "WTRM.RotorPDM", "WNAC.WindVaneDirectionInstant",
            "WTRM.TemGeaMSND", "WTRM.TemGeaMSDE", "WGEN.TemGenStaU", "WGEN.TemGenStaV", "WGEN.TemGenStaW", "WTRM.TemGeaOil", "WTRM.TemGeaLSDE", "WTRM.TemGeaLSND", "WTRM.TrmTmpShfBrg", "WYAW.YawOpWind5sAVG", "WNAC.WindDirection_AVG_10m", "WGEN.GenSpd_MAX_10m", "WNAC.NAC.AI05", "WTUR.LHDLTURA205", "WTUR.LHDLTURA206"
        ],
        'ai_rename' : {
            'WGEN.GenActivePW':'pwrat','WROT.Blade1Position':'pitch1','WROT.Blade2Position':'pitch2','WROT.Blade3Position':'pitch3',
            'WNAC.WindSpeed':'wspd','WNAC.WindVaneDirection':'wdir0','WNAC.WindDirection1':'wdir25','WNAC.WindDirection':'wdir',
            'WWPP.APProduction':'pwp','WWPP.APConsumed':'pwcs','WTUR.MainFaultCode':'faultmain',
            'WROT.TemB1Mot':'mot1tmp','WROT.TemB2Mot':'mot2tmp','WROT.TemB3Mot':'mot3tmp','WTRM.RotorPDM':'rotspdzz',
            'WROT.CurBlade1Motor':'mot1cur','WROT.CurBlade2Motor':'mot2cur','WROT.CurBlade3Motor':'mot3cur',
            'WROT.PtCptTmpBl1':'cp1tmp','WROT.PtCptTmpBl2':'cp2tmp','WROT.PtCptTmpBl3':'cp3tmp',
            'WROT.VolB1Cap':'cap1vol','WROT.VolB2Cap':'cap2vol','WROT.VolB3Cap':'cap3vol',
            'WNAC.TemOut':'exltmp','WNAC.TemNacelle':'nactmp','WGEN.GenSpdInstant':'rotspd','WGEN.GenSpd':'rotspd',
            'WROT.Blade1Speed':'pitch1spd','WROT.Blade2Speed':'pitch2spd','WROT.Blade3Speed':'pitch3spd',
            'WGEN.TemGenDriEnd':'gen_zcd_tmp',#发电机驱动端轴承温度
            'WGEN.TemGenNonDE':'gen_zcnd_tmp',#发电机非驱动端轴承温度
            'WGEN.GenSenMaxTmp':'genmaxtmp',
            'WGEN.GenSenTmp1':'gen1tmp','WGEN.GenSenTmp2':'gen2tmp','WGEN.GenSenTmp3':'gen3tmp',
            'WGEN.GenSenTmp4':'gen4tmp','WGEN.GenSenTmp5':'gen5tmp','WGEN.GenSenTmp6':'gen6tmp',
            'WYAW.NacellePosition':'yaw','WYAW.YawSpeed':'yawspd','WVIB.VibrationValid':'accxy',
            'WVIB.VibrationV':'accx','WVIB.VibrationVFil':'accxfil','WVIB.VibrationL':'accy','WVIB.VibrationLFil':'accyfil',
            'WYAW.YawMotor1RunTime':'yaw1time','WYAW.YawMotor2RunTime':'yaw2time','WYAW.YawMotor3RunTime':'yaw3time',
            'WTRM.HubAngle':'yaw','WTRM.RotorSpd':'rotspdzz','WNAC.TemNacelleCab':'nacelcabtmp',
            'WCNV.CVTTemWaterCoolInlet':'cvintmp','WCNV.CVTTemWaterCoolOutlet':'cvouttmp','WYAW.YawOpWind5sAVG':'wdirs',
            'WTRM.TemMainBearing':'mainbeartmp','WYAW.YawCountSum':'yawsum','WNAC.WindDirectionInstant':'wdirs','WTUR.SITURAI17':'wdirs',
            'WNAC.WindVaneDirectionInstant':'wdir0',
            'WTRM.TemGeaMSND':'gear_msnd_tmp',#齿轮箱高速轴非驱动端轴承温度
            'WTRM.TemGeaMSDE':'gear_msde_tmp',#齿轮箱高速轴驱动端轴承温度
            'WGEN.TemGenStaU':'genUtmp','WGEN.TemGenStaV':'genVtmp','WGEN.TemGenStaW':'genWtmp',
            'WTRM.TemGeaOil':'gear_oil_tmp',#齿轮箱油池温度
            'WTRM.TemGeaLSDE':'gear_lsde_tmp',#齿轮箱低速轴驱动端轴承温度
            'WTRM.TemGeaLSND':'gear_lsnd_tmp',#齿轮箱低速轴非驱动端轴承温度
            'WTRM.TrmTmpShfBrg':'zztmp',#主轴承温度
            'WNAC.WindDirection_AVG_10m':'wdir25',
            'WGEN.GenSpd_MAX_10m':'rotspd_max',
            'WNAC.NAC.AI05':'wdir0',
            'WTUR.LHDLTURA205':'limpw',
            'WTUR.LHDLTURA206':'limpw1'
        },
        'di_points' : ["WTUR.TurbineAIStatus"],
        'di_rename' : {'WTUR.TurbineAIStatus':'statel'},
        'cj_di_points': ["WTUR.TurbineSts_Map", "WTUR.TurbineSts"],
        'cj_di_rename' : {'WTUR.TurbineSts':'state','WTUR.TurbineSts_Map':'state'},
        'ty_di_points': ["WTUR.TurbineUnionSts"],
        'ty_di_rename' : {'WTUR.TurbineUnionSts':'statety'},
        'general_points' : ["WTUR.AIStatusCode", "WTUR.AIStatusCode_Map"],
        'general_rename' : {'WTUR.AIStatusCode':'fault','WTUR.AIStatusCode_Map':'fault'},
        'private_points' : {},
        'time_duration' : '20D',
        'resample_interval' : '1m', # 原始数据采样间隔
        'error_data_time_duration' : '60m', #'500m',
        'need_all_turbines' : True,
        'store_file' : True,
        'threshold': {}
    },
    'record_loss_indicator':{
        'name' : '10天调度记录功率数据和结果图片',
        # 把所需测点定义到每个算法里
        'ai_points' : [
            "WNAC.WindSpeed", "WGEN.GenActivePW", "WWPP.APProduction", "WWPP.APConsumed", "WROT.Blade1Position", "WROT.Blade2Position", "WROT.Blade3Position", "WROT.Blade1Speed", "WROT.Blade2Speed", "WROT.Blade3Speed", "WYAW.NacellePosition", "WYAW.YawSpeed", "WVIB.VibrationValid", "WVIB.VibrationV", "WVIB.VibrationVFil", "WVIB.VibrationL", "WVIB.VibrationLFil", "WGEN.GenSpdInstant", "WNAC.WindVaneDirection", "WNAC.WindDirection1", "WNAC.WindDirection", "WROT.TemB1Mot", "WROT.TemB2Mot", "WROT.TemB3Mot", "WROT.CurBlade1Motor", "WROT.CurBlade2Motor", "WROT.CurBlade3Motor", "WROT.PtCptTmpBl1", "WROT.PtCptTmpBl2", "WROT.PtCptTmpBl3", "WGEN.TemGenDriEnd", "WGEN.TemGenNonDE",
            "WNAC.TemNacelle", "WNAC.TemOut", "WGEN.GenSenTmp1", "WGEN.GenSenTmp2", "WGEN.GenSenTmp3", "WGEN.GenSenTmp4", "WGEN.GenSenTmp5", "WGEN.GenSenTmp6", "WYAW.YawMotor1RunTime", "WYAW.YawMotor2RunTime", "WYAW.YawMotor3RunTime", "WROT.VolB1Cap", "WROT.VolB2Cap", "WROT.VolB3Cap", "WGEN.GenSenMaxTmp", "WGEN.GenSpd", "WTUR.MainFaultCode", "WTRM.HubAngle", "WTRM.RotorSpd", "WNAC.TemNacelleCab", "WCNV.CVTTemWaterCoolInlet", "WCNV.CVTTemWaterCoolOutlet", "WTRM.TemMainBearing", "WYAW.YawCountSum", "WTUR.SITURAI17", "WNAC.XDNACAI01", "WNAC.WindDirectionInstant", "WTRM.RotorPDM", "WNAC.WindVaneDirectionInstant",
            "WTRM.TemGeaMSND", "WTRM.TemGeaMSDE", "WGEN.TemGenStaU", "WGEN.TemGenStaV", "WGEN.TemGenStaW", "WTRM.TemGeaOil", "WTRM.TemGeaLSDE", "WTRM.TemGeaLSND", "WTRM.TrmTmpShfBrg", "WYAW.YawOpWind5sAVG", "WNAC.WindDirection_AVG_10m", "WGEN.GenSpd_MAX_10m", "WNAC.NAC.AI05", "WTUR.LHDLTURA205", "WTUR.LHDLTURA206"
        ],
        'ai_rename' : {
            'WGEN.GenActivePW':'pwrat','WROT.Blade1Position':'pitch1','WROT.Blade2Position':'pitch2','WROT.Blade3Position':'pitch3',
            'WNAC.WindSpeed':'wspd','WNAC.WindVaneDirection':'wdir0','WNAC.WindDirection1':'wdir25','WNAC.WindDirection':'wdir',
            'WWPP.APProduction':'pwp','WWPP.APConsumed':'pwcs','WTUR.MainFaultCode':'faultmain',
            'WROT.TemB1Mot':'mot1tmp','WROT.TemB2Mot':'mot2tmp','WROT.TemB3Mot':'mot3tmp','WTRM.RotorPDM':'rotspdzz',
            'WROT.CurBlade1Motor':'mot1cur','WROT.CurBlade2Motor':'mot2cur','WROT.CurBlade3Motor':'mot3cur',
            'WROT.PtCptTmpBl1':'cp1tmp','WROT.PtCptTmpBl2':'cp2tmp','WROT.PtCptTmpBl3':'cp3tmp',
            'WROT.VolB1Cap':'cap1vol','WROT.VolB2Cap':'cap2vol','WROT.VolB3Cap':'cap3vol',
            'WNAC.TemOut':'exltmp','WNAC.TemNacelle':'nactmp','WGEN.GenSpdInstant':'rotspd','WGEN.GenSpd':'rotspd',
            'WROT.Blade1Speed':'pitch1spd','WROT.Blade2Speed':'pitch2spd','WROT.Blade3Speed':'pitch3spd',
            'WGEN.TemGenDriEnd':'gen_zcd_tmp',#发电机驱动端轴承温度
            'WGEN.TemGenNonDE':'gen_zcnd_tmp',#发电机非驱动端轴承温度
            'WGEN.GenSenMaxTmp':'genmaxtmp',
            'WGEN.GenSenTmp1':'gen1tmp','WGEN.GenSenTmp2':'gen2tmp','WGEN.GenSenTmp3':'gen3tmp',
            'WGEN.GenSenTmp4':'gen4tmp','WGEN.GenSenTmp5':'gen5tmp','WGEN.GenSenTmp6':'gen6tmp',
            'WYAW.NacellePosition':'yaw','WYAW.YawSpeed':'yawspd','WVIB.VibrationValid':'accxy',
            'WVIB.VibrationV':'accx','WVIB.VibrationVFil':'accxfil','WVIB.VibrationL':'accy','WVIB.VibrationLFil':'accyfil',
            'WYAW.YawMotor1RunTime':'yaw1time','WYAW.YawMotor2RunTime':'yaw2time','WYAW.YawMotor3RunTime':'yaw3time',
            'WTRM.HubAngle':'yaw','WTRM.RotorSpd':'rotspdzz','WNAC.TemNacelleCab':'nacelcabtmp',
            'WCNV.CVTTemWaterCoolInlet':'cvintmp','WCNV.CVTTemWaterCoolOutlet':'cvouttmp','WYAW.YawOpWind5sAVG':'wdirs',
            'WTRM.TemMainBearing':'mainbeartmp','WYAW.YawCountSum':'yawsum','WNAC.WindDirectionInstant':'wdirs','WTUR.SITURAI17':'wdirs',
            'WNAC.WindVaneDirectionInstant':'wdir0',
            'WTRM.TemGeaMSND':'gear_msnd_tmp',#齿轮箱高速轴非驱动端轴承温度
            'WTRM.TemGeaMSDE':'gear_msde_tmp',#齿轮箱高速轴驱动端轴承温度
            'WGEN.TemGenStaU':'genUtmp','WGEN.TemGenStaV':'genVtmp','WGEN.TemGenStaW':'genWtmp',
            'WTRM.TemGeaOil':'gear_oil_tmp',#齿轮箱油池温度
            'WTRM.TemGeaLSDE':'gear_lsde_tmp',#齿轮箱低速轴驱动端轴承温度
            'WTRM.TemGeaLSND':'gear_lsnd_tmp',#齿轮箱低速轴非驱动端轴承温度
            'WTRM.TrmTmpShfBrg':'zztmp',#主轴承温度
            'WNAC.WindDirection_AVG_10m':'wdir25',
            'WGEN.GenSpd_MAX_10m':'rotspd_max',
            'WNAC.NAC.AI05':'wdir0',
            'WTUR.LHDLTURA205':'limpw',
            'WTUR.LHDLTURA206':'limpw1'
        },
        'di_points' : ["WTUR.TurbineAIStatus"],
        'di_rename' : {'WTUR.TurbineAIStatus':'statel'},
        'cj_di_points': ["WTUR.TurbineSts_Map", "WTUR.TurbineSts"],
        'cj_di_rename' : {'WTUR.TurbineSts':'state','WTUR.TurbineSts_Map':'state'},
        'ty_di_points': ["WTUR.TurbineUnionSts"],
        'ty_di_rename' : {'WTUR.TurbineUnionSts':'statety'},
        'general_points' : ["WTUR.AIStatusCode", "WTUR.AIStatusCode_Map"],
        'general_rename' : {'WTUR.AIStatusCode':'fault','WTUR.AIStatusCode_Map':'fault'},
        'private_points' : {},
        'time_duration' : '2D',
        'resample_interval' : '1m', # 原始数据采样间隔
        'error_data_time_duration' : '60m', #'500m',
        'need_all_turbines' : True,
        'store_file' : True,
        'threshold': {}
    },
}
