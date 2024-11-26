import mysql.connector
from datetime import datetime
from configs import config
from collections import Counter
import json
import os
import yaml as yl
from minio import Minio
# from minio.policy import Policy, Statement, ALLOW, DENY
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

log = logging.getLogger('mysql_log')
if not log.handlers:
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(process)d - %(threadName)s - %(message)s')
    # console_handler = logging.StreamHandler()
    # console_handler.setFormatter(formatter)
    # alarm_file_handler = TimedRotatingFileHandler('logs/alarm.log', when='midnight', interval=1, backupCount=30)
    data_file_handler = logging.handlers.RotatingFileHandler(filename=os.path.join("logs","mysql"+".log"), mode='a', maxBytes=5*1024**2, backupCount=3)
    data_file_handler.setFormatter(formatter)
    log.setLevel(logging.INFO)
    # data_logger.addHandler(console_handler)
    log.addHandler(data_file_handler)
exceptAlgorithmList = ['Efficiency_ana_V3']

minio_path = os.getcwd()
minio_path = os.path.join(minio_path, 'configs', 'config.yaml')
with open(minio_path, "r") as f:
    ylv = yl.load(f.read(), Loader=yl.FullLoader)
minio_client = Minio(ylv["url"], access_key=ylv["accesskey"], secret_key=ylv["secretkey"], secure=False)
# minio_policy = Policy()
# minio_statement = Statement(
#     actions=ALLOW,
#     effect='Allow',
#     resources=[f'arn:aws:s3:::{ylv['bucketName']}/*'],
#     principals=[{'*': ['*']}]
# )
# minio_policy.statements.append(minio_statement)

minio_policy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetBucketLocation","s3:ListBucket"],"Resource":["arn:aws:s3:::%s"]},{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::%s/*"]}]}' % (ylv['bucketName'], ylv['bucketName'])


def upload(filename:str, algorithms_configs:dict):
    found = minio_client.bucket_exists(ylv['bucketName'])
    if not found:
        minio_client.make_bucket(ylv['bucketName'])
        minio_client.set_bucket_policy(ylv['bucketName'], policy=minio_policy)
    else:
        log.info("Bucket "+ylv['bucketName']+" already exists")
    #name_uuid = uuid.uuid3(uuid.NAMESPACE_DNS, filename)
    basename = os.path.basename(filename)
    dir = algorithms_configs['minio_dir']
    response = minio_client.fput_object(ylv['bucketName'], os.path.join(dir,basename), filename)
    log.info(
        filename+" is successfully uploaded as "
        "object " +os.path.join(dir,basename)+" to bucket "+ylv['bucketName']+"."
    )
    file_url = minio_client.presigned_get_object(ylv['bucketName'], os.path.join(dir,basename))
    return file_url

#################################mysql#################################
create_pw_turbine_all_table_query = f'''
    create table pw_turbine_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        pwrt float comment '功率kw*h'
    ) comment='风机功率表';
'''
create_pw_time_all_table_query = f'''
    create table pw_time_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wind_bin float comment '风速m/s',
        pwrt_mean float comment '多台风机平均功率kw*h',
        pwrt float comment '功率kw*h',
        count float comment '风仓统计频数'
    ) comment='风机功率曲线表';
'''
create_turbine_warning_all_table_query = f'''
    create table pw_turbine_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        fault text comment '故障标识',
        time_rate float comment '时间利用率',
        count float comment '故障次数'
    ) comment='单机告警';
'''
create_technology_loss_all_table_query = f'''
    create table technology_loss_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        fault float comment '故障标识',
        count float comment '故障频数',
        wspd float comment '风速m/s',
        time_rate float comment '时间利用率',
        loss float comment '损失功率',
        fault_describe text comment '故障描述',
    ) comment='技术故障损失';
'''
create_limturbine_loss_all_table_query = f'''
    create table limturbine_loss_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        time_rate float comment '时间利用率',
        loss float comment '损失功率'
    ) comment='单机限电损失';
'''
create_faultgrid_loss_all_table_query = f'''
    create table faultgrid_loss_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        time_rate float comment '时间利用率',
        loss float comment '损失功率',
        count float comment '故障次数',
        fault float comment '故障标识',
        fault_describe text comment '故障描述'
    ) comment='电网故障损失';
'''
create_stop_loss_all_table_query = f'''
    create table stop_loss_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        exltmp float comment '环境温度',
        time_rate float comment '时间利用率',
        loss float comment '损失功率'
    ) comment='计划停机损失';
'''
create_limgrid_loss_all_table_query = f'''
    create table limgrid_loss_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        time_rate float comment '时间利用率',
        loss float comment '损失功率'
    ) comment='电网限电损失';
'''
create_fault_loss_all_table_query = f'''
    create table fault_loss_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        time_rate float comment '时间利用率',
        loss float comment '损失功率',
        count float comment '故障次数',
        fault float comment '故障标识',
        fault_describe text comment '故障描述',
        fsyst text comment '系统故障'
    ) comment='单机故障损失';
'''
create_eny_wspd_all_table_query = f'''
    create table eny_wspd_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        eny float comment '风能',
        wspd float comment '风速',
        count float comment '频次',
        rate_power float comment '利用率'
    ) comment='风能表';
'''
create_wind_frequency_picture_table_query = f'''
    create table wind_frequency_picture (
        id bigint auto_increment primary key comment '主键', 
        excute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='风频图';
'''
create_wind_direction_picture_table_query = f'''
    create table wind_direction_picture (
        id bigint auto_increment primary key comment '主键', 
        excute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='风向图';
'''
create_air_density_picture_table_query = f'''
    create table air_density_picture (
        id bigint auto_increment primary key comment '主键', 
        excute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='空气密度图';
'''
create_turbulence_picture_table_query = f'''
    create table turbulence_picture (
        id bigint auto_increment primary key comment '主键', 
        excute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='湍流图';
'''
create_navigation_bias_direction_picture_table_query = f'''
    create table navigation_bias_direction_picture (
        id bigint auto_increment primary key comment '主键', 
        excute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='偏航对风图';
'''
create_navigation_bias_control_picture_table_query = f'''
    create table navigation_bias_control_picture (
        id bigint auto_increment primary key comment '主键', 
        excute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='偏航控制图';
'''
create_pitch_angle_picture_table_query = f'''
    create table pitch_angle_picture (
        id bigint auto_increment primary key comment '主键', 
        excute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='最小桨距角图';
'''
create_pitch_action_picture_table_query = f'''
    create table pitch_action_picture (
        id bigint auto_increment primary key comment '主键', 
        excute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='变桨动作图';
'''
create_torque_control_picture_table_query = f'''
    create table torque_control_picture (
        id bigint auto_increment primary key comment '主键', 
        excute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='转矩控制图';
'''
####################################################33
#提取数据
####################################################33



def selectPwTimeAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=1), end_time=datetime.now()-timedelta(days=91)):
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"提取pw_time_all数据")
    if len(typeName) > 0:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wind_bin, \
            pwrt_mean, \
            pwrt, \
            count \
            from pw_time_all \
            where farm_name=%s AND type_name=%s AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, typeName, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    else:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wind_bin, \
            pwrt_mean, \
            pwrt, \
            count \
            from pw_time_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None:
        pass #return pd.DataFrame()
    else:
        for lineValue in queryResult:
            localtime = pd.to_datetime(lineValue[0],errors='coerce')
            data.loc[localtime, ['windbin','pwrat',lineValue[4],lineValue[4]+'_count']] = [lineValue[5], lineValue[6],lineValue[7],lineValue[8]]
    return data

def selectPwTurbineAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=1), end_time=datetime.now()-timedelta(days=91)):
    if len(typeName) > 0:
        wtids = []
    else:
        wtids = {}
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"提取pw_time_all数据")
    if len(typeName) > 0:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wpsd, \
            pwrt \
            from pw_turbine_all \
            where farm_name=%s AND type_name=%s AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, typeName, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    else:
        obtain_query = "SELECT \
                data_time, \
                farm_name, \
                farm_id, \
                type_name, \
                wtid, \
                wpsd, \
                pwrt \
                from pw_turbine_all \
                where farm_name=%s AND data_time BETWEEN %s AND %s' \
            "
        data_to_obtain = (farmName, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain) 
    queryResult = cursor.fetchall()
    if queryResult == None:
        pass #return pd.DataFrame()
    else:
        for lineValue in queryResult:
            localtime = pd.to_datetime(lineValue[0],errors='coerce')
            data.loc[localtime, ['type',lineValue[4]+'_wpsd',lineValue[4]]] = [lineValue[3], lineValue[5],lineValue[6]]
            if len(typeName) > 0:
                wtids.append(lineValue[4])
            else:
                if lineValue[3] in wtids:
                    wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
    return data, wtids

def selectTechnologyLossAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=1), end_time=datetime.now()-timedelta(days=91)):
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"提取technology_loss_all数据")
    wtids = {}
    typeNameStr = ""
    for name in typeName:
        typeNameStr += str(name) + ','
    typeNameStr = typeNameStr.rstrip(',')
    if len(typeName) > 0:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            fault, \
            count, \
            wpsd, \
            time_rate, \
            loss, \
            fault_describe \
            from technology_loss_all \
            where farm_name=%s AND type_name=%s  AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    else:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            fault, \
            count, \
            wpsd, \
            time_rate, \
            loss, \
            fault_describe \
            from technology_loss_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None:
        pass #return pd.DataFrame()
    else:
        for lineValue in queryResult:
            localtime = pd.to_datetime(lineValue[0],errors='coerce')
            data.loc[localtime, ['type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe']] = [lineValue[3], lineValue[4],lineValue[5], lineValue[6], lineValue[8], lineValue[9], lineValue[7], lineValue[10]]
            if lineValue[3] in wtids:
                wtids[lineValue[3]].append(lineValue[4])
            else:
                wtids[lineValue[3]] = [lineValue[4]]
    return data, wtids
def selectLimturbineLossAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=1), end_time=datetime.now()-timedelta(days=91)):
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"提取limturbine_loss_all数据")
    wtids = {}
    typeNameStr = ""
    for name in typeName:
        typeNameStr += str(name) + ','
    typeNameStr = typeNameStr.rstrip(',')
    if len(typeName) > 0:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wpsd, \
            time_rate, \
            loss \
            from limturbine_loss_all \
            where farm_name=%s AND type_name=%s  AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    else:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wpsd, \
            time_rate, \
            loss \
            from limturbine_loss_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None:
        pass #return pd.DataFrame()
    else:
        for lineValue in queryResult:
            localtime = pd.to_datetime(lineValue[0],errors='coerce')
            data.loc[localtime, ['type', 'wtid', 'time', 'loss', 'wspd']] = [lineValue[3], lineValue[4],lineValue[6], lineValue[7], lineValue[5]]
            if lineValue[3] in wtids:
                wtids[lineValue[3]].append(lineValue[4])
            else:
                wtids[lineValue[3]] = [lineValue[4]]
    return data,wtids
def selectFaultgridLossAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=1), end_time=datetime.now()-timedelta(days=91)):
    conn = get_connection()
    cursor = conn.cursor()
    wtids = {}
    log.info(f"提取faultgrid_loss_all数据")
    typeNameStr = ""
    for name in typeName:
        typeNameStr += str(name) + ','
    typeNameStr = typeNameStr.rstrip(',')
    if len(typeName) > 0:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            fault, \
            count, \
            wpsd, \
            time_rate, \
            loss, \
            fault_describe \
            from faultgrid_loss_all \
            where farm_name=%s AND type_name=%s  AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    else:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            fault, \
            count, \
            wpsd, \
            time_rate, \
            loss, \
            fault_describe \
            from faultgrid_loss_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None:
        pass #return pd.DataFrame()
    else:
        for lineValue in queryResult:
            localtime = pd.to_datetime(lineValue[0],errors='coerce')
            data.loc[localtime, ['type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe']] = [lineValue[3], lineValue[4],lineValue[5], lineValue[6], lineValue[8], lineValue[9], lineValue[7], lineValue[10]]
            if lineValue[3] in wtids:
                wtids[lineValue[3]].append(lineValue[4])
            else:
                wtids[lineValue[3]] = [lineValue[4]]
    return data, wtids
def selectStopLossAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=1), end_time=datetime.now()-timedelta(days=91)):
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"提取stop_loss_all数据")
    wtids = {}
    typeNameStr = ""
    for name in typeName:
        typeNameStr += str(name) + ','
    typeNameStr = typeNameStr.rstrip(',')
    if len(typeName) > 0:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wpsd, \
            time_rate, \
            loss, \
            exltmp \
            from stop_loss_all \
            where farm_name=%s AND type_name=%s  AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    else:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wpsd, \
            time_rate, \
            loss, \
            exltmp \
            from stop_loss_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None:
        pass #return pd.DataFrame()
    else:
        for lineValue in queryResult:
            localtime = pd.to_datetime(lineValue[0],errors='coerce')
            data.loc[localtime, ['type', 'wtid', 'time', 'loss', 'wspd', 'exltmp']] = [lineValue[3], lineValue[4],lineValue[6], lineValue[7], lineValue[5], lineValue[8]]
            if lineValue[3] in wtids:
                wtids[lineValue[3]].append(lineValue[4])
            else:
                wtids[lineValue[3]] = [lineValue[4]]
    return data, wtids
def selectFaultLossAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=1), end_time=datetime.now()-timedelta(days=91)):
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"提取fault_loss_all数据")
    wtids = {}
    typeNameStr = ""
    for name in typeName:
        typeNameStr += str(name) + ','
    typeNameStr = typeNameStr.rstrip(',')
    if len(typeName) > 0:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            fault, \
            count, \
            wpsd, \
            time_rate, \
            loss, \
            fault_describe, \
            fsyst \
            from fault_loss_all \
            where farm_name=%s AND type_name in (%s)  AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    else:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            fault, \
            count, \
            wpsd, \
            time_rate, \
            loss, \
            fault_describe, \
            fsyst \
            from fault_loss_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None:
        pass #return pd.DataFrame()
    else:
        for lineValue in queryResult:
            localtime = pd.to_datetime(lineValue[0],errors='coerce')
            data.loc[localtime, ['type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe', 'fsyst']] = [lineValue[3], lineValue[4],lineValue[5], lineValue[6], lineValue[8], lineValue[9], lineValue[7], lineValue[10], lineValue[11]]
            if lineValue[3] in wtids:
                wtids[lineValue[3]].append(lineValue[4])
            else:
                wtids[lineValue[3]] = [lineValue[4]]
    return data, wtids
def selectLimgridLossAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=1), end_time=datetime.now()-timedelta(days=91)):
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"提取limgrid_loss_all数据")
    wtids = {}
    typeNameStr = ""
    for name in typeName:
        typeNameStr += str(name) + ','
    typeNameStr = typeNameStr.rstrip(',')
    if len(typeName) > 0:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wpsd, \
            time_rate, \
            loss \
            from limgrid_loss_all \
            where farm_name=%s AND type_name=%s  AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    else:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wpsd, \
            time_rate, \
            loss \
            from limgrid_loss_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None:
        pass #return pd.DataFrame()
    else:
        for lineValue in queryResult:
            localtime = pd.to_datetime(lineValue[0],errors='coerce')
            data.loc[localtime, ['type', 'wtid', 'time', 'loss', 'wspd']] = [lineValue[3], lineValue[4],lineValue[6], lineValue[7], lineValue[5]]
            if lineValue[3] in wtids:
                wtids[lineValue[3]].append(lineValue[4])
            else:
                wtids[lineValue[3]] = [lineValue[4]]
    return data, wtids

def selectEnyWspdAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=1), end_time=datetime.now()-timedelta(days=91)):
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"提取eny_wspd_all数据")
    wtids = {}
    typeNameStr = ""
    for name in typeName:
        typeNameStr += str(name) + ','
    typeNameStr = typeNameStr.rstrip(',')
    if len(typeName) > 0:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            eny, \
            wspd, \
            count, \
            rate_power \
            from eny_wspd_all \
            where farm_name=%s AND type_name=%s  AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    else:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            eny, \
            wspd, \
            count, \
            rate_power \
            from eny_wspd_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s' \
        "
        data_to_obtain = (farmName, start_time, end_time)
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None:
        pass #return pd.DataFrame()
    else:
        for lineValue in queryResult:
            localtime = pd.to_datetime(lineValue[0],errors='coerce')
            data.loc[localtime, ['type', 'wtid', 'eny', 'wspd', 'count', 'rate_power']] = [lineValue[3], lineValue[4],lineValue[5], lineValue[6], lineValue[7], lineValue[8]]
            if lineValue[3] in wtids:
                wtids[lineValue[3]].append(lineValue[4])
            else:
                wtids[lineValue[3]] = [lineValue[4]]
    return data, wtids
###################################################
#录入数据
###################################################

########################存功率##################################


def insertPwTimeAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like pw_time_all;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_pw_time_all_table_query)
        #插入数据
        log.info(f"pw_time_all表插入数据")
        #查询每个机子
        for i in range(len(algorithms_configs['wtids'])):
            turbine_name = algorithms_configs['wtids'][i]
        #插入每个机子大于已有数据的时间最大值的数据
        #截取dataframe的列，只摄取当前几号的列
            tmp = data[['windbin', 'pwrat', turbine_name, turbine_name+'_count']]
        #遍历时间
            for j in range(tmp.shape[0]):
                insert_query = "INSERT INTO pw_time_all ( \
                        data_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        wind_bin, \
                        pwrt_mean, \
                        pwrt, \
                        count \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, tmp.iloc[j]['windbin'],tmp.iloc[j]['pwrat'], tmp.iloc[j][turbine_name], tmp.iloc[j][turbine_name+'_count'])
                cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"pw_time_all表插入数据")
        #查询表中每个机子已有数据的时间最大值
        for i in range(len(algorithms_configs['wtids'])):
            turbine_name = algorithms_configs['wtids'][i]
            queryItem = "select max(data_time) from pw_time_all where wtid=%s"
            dataQuery = (turbine_name,)
            cursor.execute(queryItem, dataQuery)
            result = cursor.fetchone()
            max_sql_time = result[0]
        #插入每个机子大于已有数据的时间最大值的数据
        #截取dataframe的列，只摄取当前几号的列
            tmp = data[['windbin', 'pwrat', turbine_name, turbine_name+'_count']]
            tmp = tmp[tmp.index > max_sql_time] 
        #遍历时间
            for j in range(tmp.shape[0]):
                insert_query = "INSERT INTO pw_time_all ( \
                        data_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        wind_bin, \
                        pwrt_mean, \
                        pwrt, \
                        count \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, tmp.iloc[j]['windbin'],tmp.iloc[j]['pwrat'], tmp.iloc[j][turbine_name], tmp.iloc[j][turbine_name+'_count'])
                cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close()

def insertPwTurbineAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like pw_turbine_all;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_pw_turbine_all_table_query)
        #插入数据
        log.info(f"pw_turbine_all表插入数据")
        #查询每个机子
        for i in range(len(algorithms_configs['wtids'])):
            turbine_name = algorithms_configs['wtids'][i]
        #插入每个机子大于已有数据的时间最大值的数据
        #截取dataframe的列，只摄取当前几号的列
            tmp = data[['type', turbine_name+'_wspd', turbine_name]]
        #遍历时间
            for j in range(tmp.shape[0]):
                insert_query = "INSERT INTO pw_turbine_all ( \
                        data_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        wspd, \
                        pwrt \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], turbine_name, tmp.iloc[j][turbine_name+'_wspd'], tmp.iloc[j][turbine_name])
                cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"pw_turbine_all表插入数据")
        #查询表中每个机子已有数据的时间最大值
        for i in range(len(algorithms_configs['wtids'])):
            turbine_name = algorithms_configs['wtids'][i]
            queryItem = "select max(data_time) from pw_turbine_all where wtid=%s"
            dataQuery = (turbine_name,)
            cursor.execute(queryItem, dataQuery)
            result = cursor.fetchone()
            max_sql_time = result[0]
        #插入每个机子大于已有数据的时间最大值的数据
        #截取dataframe的列，只摄取当前几号的列
            tmp = data[['type', turbine_name+'_wspd', turbine_name]]
            tmp = tmp[tmp.index > max_sql_time] 
        #遍历时间
            for j in range(tmp.shape[0]):
                insert_query = "INSERT INTO pw_turbine_all ( \
                        data_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        wspd, \
                        pwrt \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], turbine_name, tmp.iloc[j][turbine_name+'_wspd'], tmp.iloc[j][turbine_name])
                cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close()

########################存损失电量##################################

#def insertTurbineWarningAll():
def insertTechnologyLossAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like technology_loss_all;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_technology_loss_all_table_query)
        #插入数据
        log.info(f"technology_loss_all表插入数据")
        
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe']]
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO technology_loss_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    fault, \
                    count, \
                    wspd, \
                    time_rate, \
                    loss, \
                    fault_describe \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['fault'], tmp.iloc[j]['count'], tmp.iloc[j]['wspd'], tmp.iloc[j]['time'], tmp.iloc[j]['loss'], tmp.iloc[j]['fault_describe'])
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"technology_loss_all表插入数据")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from technology_loss_all"
        cursor.execute(queryItem)
        result = cursor.fetchone()
        max_sql_time = result[0]
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe']]
        tmp = tmp[tmp.index > max_sql_time] 
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO technology_loss_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    fault, \
                    count, \
                    wspd, \
                    time_rate, \
                    loss, \
                    fault_describe \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['fault'], tmp.iloc[j]['count'], tmp.iloc[j]['wspd'], tmp.iloc[j]['time'], tmp.iloc[j]['loss'], tmp.iloc[j]['fault_describe'])
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close() 
def insertLimturbineLossAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like limturbine_loss_all;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_limturbine_loss_all_table_query)
        #插入数据
        log.info(f"limturbine_loss_all表插入数据")
        
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'time', 'loss', 'wspd']]
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO limturbine_loss_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    wspd, \
                    time_rate, \
                    loss \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['wspd'], tmp.iloc[j]['time'], tmp.iloc[j]['loss'])
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"limturbine_loss_all表插入数据")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from limturbine_loss_all"
        cursor.execute(queryItem)
        result = cursor.fetchone()
        max_sql_time = result[0]
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'time', 'loss', 'wspd']]
        tmp = tmp[tmp.index > max_sql_time] 
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO limturbine_loss_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    wspd, \
                    time_rate, \
                    loss \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['wspd'], tmp.iloc[j]['time'], tmp.iloc[j]['loss'])
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close() 
def insertFaultgridLossAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like faultgrid_loss_all;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_faultgrid_loss_all_table_query)
        #插入数据
        log.info(f"faultgrid_loss_all表插入数据")
        
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe']]
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO faultgrid_loss_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    fault, \
                    count, \
                    wspd, \
                    time_rate, \
                    loss, \
                    fault_describe \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['fault'], tmp.iloc[j]['count'], tmp.iloc[j]['wspd'], tmp.iloc[j]['time'], tmp.iloc[j]['loss'], tmp.iloc[j]['fault_describe'])
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"faultgrid_loss_all表插入数据")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from faultgrid_loss_all"
        cursor.execute(queryItem)
        result = cursor.fetchone()
        max_sql_time = result[0]
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe']]
        tmp = tmp[tmp.index > max_sql_time] 
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO faultgrid_loss_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    fault, \
                    count, \
                    wspd, \
                    time_rate, \
                    loss, \
                    fault_describe \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['fault'], tmp.iloc[j]['count'], tmp.iloc[j]['wspd'], tmp.iloc[j]['time'], tmp.iloc[j]['loss'], tmp.iloc[j]['fault_describe'])
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close() 
def insertStopLossAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like stop_loss_all;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_stop_loss_all_table_query)
        #插入数据
        log.info(f"stop_loss_all表插入数据")
        
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'time', 'loss', 'wspd', 'exltmp']]
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO stop_loss_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    wspd, \
                    time_rate, \
                    loss, \
                    exltmp \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['wspd'], tmp.iloc[j]['time'], tmp.iloc[j]['loss'], tmp.iloc[j]['exltmp'])
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"stop_loss_all表插入数据")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from stop_loss_all"
        cursor.execute(queryItem)
        result = cursor.fetchone()
        max_sql_time = result[0]
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'time', 'loss', 'wspd', 'exltmp']]
        tmp = tmp[tmp.index > max_sql_time] 
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO stop_loss_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    wspd, \
                    time_rate, \
                    loss, \
                    exltmp \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['wspd'], tmp.iloc[j]['time'], tmp.iloc[j]['loss'], tmp.iloc[j]['exltmp'])
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close() 
def insertFaultLossAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like fault_loss_all;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_fault_loss_all_table_query)
        #插入数据
        log.info(f"fault_loss_all表插入数据")
        
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe', 'fsyst']]
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO fault_loss_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    fault, \
                    count, \
                    wspd, \
                    time_rate, \
                    loss, \
                    fault_describe, \
                    fsyst \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['fault'], tmp.iloc[j]['count'], tmp.iloc[j]['wspd'], tmp.iloc[j]['time'], tmp.iloc[j]['loss'], tmp.iloc[j]['fault_describe'], tmp.iloc[j]['fsyst'])
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"fault_loss_all表插入数据")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from fault_loss_all"
        cursor.execute(queryItem)
        result = cursor.fetchone()
        max_sql_time = result[0]
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe', 'fsyst']]
        tmp = tmp[tmp.index > max_sql_time] 
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO fault_loss_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    fault, \
                    count, \
                    wspd, \
                    time_rate, \
                    loss, \
                    fault_describe, \
                    fsyst \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['fault'], tmp.iloc[j]['count'], tmp.iloc[j]['wspd'], tmp.iloc[j]['time'], tmp.iloc[j]['loss'], tmp.iloc[j]['fault_describe'], tmp.iloc[j]['fsyst'])
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close()

def insertLimgridLossAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like limgrid_loss_all;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_limgrid_loss_all_table_query)
        #插入数据
        log.info(f"limgrid_loss_all表插入数据")
        
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'time', 'loss', 'wspd']]
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO limgrid_loss_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    wspd, \
                    time_rate, \
                    loss \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['wspd'], tmp.iloc[j]['time'], tmp.iloc[j]['loss'])
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"limgrid_loss_all表插入数据")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from limgrid_loss_all"
        cursor.execute(queryItem)
        result = cursor.fetchone()
        max_sql_time = result[0]
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'time', 'loss', 'wspd']]
        tmp = tmp[tmp.index > max_sql_time] 
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO limgrid_loss_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    wspd, \
                    time_rate, \
                    loss \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['wspd'], tmp.iloc[j]['time'], tmp.iloc[j]['loss'])
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close() 

def insertEnyWspdAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like eny_wspd_all;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_eny_wspd_all_table_query)
        #插入数据
        log.info(f"eny_wspd_all表插入数据")
        
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'eny', 'wspd', 'count', 'rate_power']]
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO eny_wspd_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    eny, \
                    wspd, \
                    count, \
                    rate_power \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['eny'], tmp.iloc[j]['wspd'], tmp.iloc[j]['count'], tmp.iloc[j]['rate_power'])
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"eny_wspd_all表插入数据")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from eny_wspd_all"
        cursor.execute(queryItem)
        result = cursor.fetchone()
        max_sql_time = result[0]
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'eny', 'wspd', 'count', 'rate_power']]
        tmp = tmp[tmp.index > max_sql_time] 
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO eny_wspd_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    eny, \
                    wspd, \
                    count, \
                    rate_power \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['eny'], tmp.iloc[j]['wspd'], tmp.iloc[j]['count'], tmp.iloc[j]['rate_power'])
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close() 


#############################存图片地址######################################
def insertAllWindFrequencyPicture(algorithms_configs, url_path):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like wind_frequency_picture;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_wind_frequency_picture_table_query)
    #插入数据
    log.info(f"wind_frequency_picture表插入数据")
    insert_query = "INSERT INTO wind_frequency_picture (excute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        minio_url, \
                        ) VALUES (%s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], 'all', url_path)
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertWindFrequencyPicture(algorithms_configs, url_path):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like wind_frequency_picture;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_wind_frequency_picture_table_query)
    #插入数据
    log.info(f"wind_frequency_picture表插入数据")
    insert_query = "INSERT INTO wind_frequency_picture (excute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        minio_url, \
                        ) VALUES (%s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], url_path)
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertWindDirectionPicture(algorithms_configs, url_path, turbine_name):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like wind_direction_picture;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_wind_direction_picture_table_query)
    #插入数据
    log.info(f"wind_direction_picture表插入数据")
    insert_query = "INSERT INTO wind_direction_picture (excute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        minio_url, \
                        ) VALUES (%s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, url_path)
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()

def insertAllAirDensityPicture(algorithms_configs, url_path):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like air_density_picture;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_air_density_picture_table_query)
    #插入数据
    log.info(f"air_density_picture表插入数据")
    insert_query = "INSERT INTO air_density_picture (excute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        minio_url, \
                        ) VALUES (%s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], 'all', url_path)
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()

def insertAirDensityPicture(algorithms_configs, url_path):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like air_density_picture;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_air_density_picture_table_query)
    #插入数据
    log.info(f"air_density_picture表插入数据")
    insert_query = "INSERT INTO air_density_picture (excute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        minio_url, \
                        ) VALUES (%s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], url_path)
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()

def insertAllTurbulencePicture(algorithms_configs, url_path):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like turbulence_picture;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_turbulence_picture_table_query)
    #插入数据
    log.info(f"turbulence_picture表插入数据")
    insert_query = "INSERT INTO turbulence_picture (excute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        minio_url, \
                        ) VALUES (%s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], 'all', url_path)
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()

def insertTurbulencePicture(algorithms_configs, url_path):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like turbulence_picture;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_turbulence_picture_table_query)
    #插入数据
    log.info(f"turbulence_picture表插入数据")
    insert_query = "INSERT INTO turbulence_picture (excute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        minio_url, \
                        ) VALUES (%s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], url_path)
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertNavigationBiasDirectionPicture(algorithms_configs, url_path, turbine_name):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like navigation_bias_direction_picture;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_navigation_bias_direction_picture_table_query)
    #插入数据
    log.info(f"navigation_bias_direction_picture表插入数据")
    insert_query = "INSERT INTO navigation_bias_direction_picture (excute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        minio_url, \
                        ) VALUES (%s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, url_path)
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertNavigationBiasControlPicture(algorithms_configs, url_path, turbine_name):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like navigation_bias_control_picture;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_navigation_bias_control_picture_table_query)
    #插入数据
    log.info(f"navigation_bias_control_picture表插入数据")
    insert_query = "INSERT INTO navigation_bias_control_picture (excute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        minio_url, \
                        ) VALUES (%s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, url_path)
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertPitchAnglePicture(algorithms_configs, url_path, turbine_name):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like pitch_angle_picture;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_pitch_angle_picture_table_query)
    #插入数据
    log.info(f"pitch_angle_picture表插入数据")
    insert_query = "INSERT INTO pitch_angle_picture (excute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        minio_url, \
                        ) VALUES (%s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, url_path)
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertPitchActionPicture(algorithms_configs, url_path, turbine_name):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like pitch_action_picture;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_pitch_action_picture_table_query)
    #插入数据
    log.info(f"pitch_action_picture表插入数据")
    insert_query = "INSERT INTO pitch_action_picture (excute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        minio_url, \
                        ) VALUES (%s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, url_path)
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertTorqueControlPicture(algorithms_configs, url_path, turbine_name):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like torque_control_picture;"
    #执行
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        cursor.execute(create_torque_control_picture_table_query)
    #插入数据
    log.info(f"torque_control_picture表插入数据")
    insert_query = "INSERT INTO torque_control_picture (excute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        minio_url, \
                        ) VALUES (%s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, url_path)
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()

#########################################################
def removeElementFromList(originList, exceptList):
    return [x for x in originList if x not in exceptList]

def get_connection():
    conn = mysql.connector.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USERNAME,
        password=config.DB_PASSWORD,
        database=config.DB_DATABASE,
        buffered=True
    )
    return conn
def get_connection_efficiency(config_path):
    sql_config = os.path.join(config_path)
    with open(sql_config, "r") as f:
        ylv = yl.load(f.read(), Loader=yl.FullLoader)
    conn = mysql.connector.connect(
        host=ylv["DB_HOST"],
        port=ylv["DB_PORT"],
        user=ylv["DB_USERNAME"],
        password=ylv["DB_PASSWORD"],
        database=ylv["DB_DATABASE"]
    )
    return conn
def InsertIndex(conn, average_wind_speed, actual_power_generation, loss_of_electricity, equivalent_hours, wind_rate, time_rate, start_time, end_time):
    cursor = conn.cursor()
    insert_query = "INSERT INTO operation_index (average_wind_speed, \
                        actual_power_generation, \
                        loss_of_electricity, \
                        equivalent_hours, \
                        wind_rate, \
                        time_rate, \
                        create_time, \
                        start_time, \
                        end_time \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (average_wind_speed, actual_power_generation, loss_of_electricity, equivalent_hours, wind_rate*100, time_rate*100, datetime.now(), start_time, end_time)
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()

def insert_alarm(assetId, alarmName, alarmTime, error_start_time, error_end_time):#conn, 
    conn = get_connection()
    cursor = conn.cursor()
    insert_query = "INSERT INTO data_alarm (wind_turbine_code, \
                        content, \
                        level, \
                        state, \
                        create_time, \
                        update_time, \
                        gather_time, \
                        source, \
                        alarm_type, \
                        category,\
                        group_id) VALUES ((select code from base_asset where enos_id=%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (assetId, alarmName, 1, 0, datetime.now(), datetime.now(), alarmTime, 1, 3, 1, alarmName)
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()

def InsertAlgorithmDetail(headId,wind_num,start_time,status, turbineId, result, warning=0, alert=0): #warning预警， alert告警 conn,
    if headId == None or headId == 'None':
        return None
    conn = get_connection()
    cursor = conn.cursor()
    addItem = "insert into algorithm_execute_detail (\
        head_id, \
        wind_num, \
        execute_time, \
        status, \
        enos_id, \
        analysis_result, \
        warning_level, \
        alarm_level) values (%s, %s, %s, %s, %s, %s, %s, %s)"
    dataItem = (headId, wind_num, start_time, status, turbineId, result, warning, alert)
    cursor.execute(addItem, dataItem)
    itemId = cursor.lastrowid

    conn.commit()
    cursor.close()
    conn.close()

    return itemId


# def UpdateAlgorithmDetail(conn):
#     cursor = conn.cursor()

def InsertAlgorithmHead(code, start_time, data_start_time, data_end_time):#conn,
    if code in exceptAlgorithmList:
        return None
    conn = get_connection()
    cursor = conn.cursor()
    queryItem = "select id, name from algorithm_model where code=%s"
    dataQuery = (code,)
    cursor.execute(queryItem, dataQuery)
    queryResult = cursor.fetchall()

    addItem = "insert into algorithm_execute_head (\
        model_id, \
        model_name, \
        start_time, \
        data_start_time, \
        data_end_time, \
        model_code) values (%s, %s, %s, %s, %s, %s)"
    dataItem = (queryResult[-1][0], queryResult[-1][1], start_time, data_start_time, data_end_time, code)
    cursor.execute(addItem, dataItem)
    itemId = cursor.lastrowid

    conn.commit()
    cursor.close()
    conn.close()

    return itemId


def UpdateAlgorithmHead(ids):#ids:{algorithmCode:headId}     删#{headId:(algorithmCode,turbineIndex)} . conn, 
    for key, value in ids.items():
        if value == None or value == 'None':
            continue
        conn = get_connection()
        cursor = conn.cursor()
        queryItem = "select last_end_time, last_result_normal, last_result_abnormal, last_result_fail from algorithm_model where code=%s"
        dataQuery = (key,)
        cursor.execute(queryItem, dataQuery)
        queryResult = cursor.fetchall()

        updateItem = "update algorithm_execute_head \
        set end_time=%s, \
        result_normal=%s, \
        result_abnormal=%s,\
        result_fail=%s \
        where id=%s"
        dataItem = (queryResult[-1][0], queryResult[-1][1], queryResult[-1][2], queryResult[-1][3], value)
        cursor.execute(updateItem, dataItem)
        conn.commit()
        cursor.close()
        conn.close()

def ResetTubineNum(modeCode, totalTurbineNum, currentTurbineNum):#conn, 
    modeCode_ = removeElementFromList([modeCode] if isinstance(modeCode,str) else modeCode, exceptAlgorithmList)
    if len(modeCode_) > 0:
        conn = get_connection()
        cursor = conn.cursor()
        values = "(%s)" % ','.join(['%s'] * len(modeCode_))
        update_info = "update algorithm_model \
                        set sum_num=%s, \
                        current_num=%s \
                        where code in " + values
        data_to_insert = (totalTurbineNum, currentTurbineNum, *modeCode_)  
        cursor.execute(update_info, data_to_insert)
        
        conn.commit()
        cursor.close()
        conn.close()

def ReviewTubineNum(modeCode, currentTurbineNum):#conn, 
    modeCode_ = removeElementFromList([modeCode] if isinstance(modeCode,str) else modeCode, exceptAlgorithmList)
    if len(modeCode_) == 0:
        return []
    conn = get_connection()
    cursor = conn.cursor()
    values = "(%s)" % ','.join(['%s'] * len(modeCode_))
    #检查,算法列表中sum_num>0只更新current_num; sum_num=0传出待重置算法名列表
    # greaterZeroCode = "select code from algorithm_model \
    #                    where code in " + values + " \
    #                    and sum_num > 0"
    equalZeroCode = "select code from algorithm_model \
                       where code in " + values + " \
                       and sum_num = 0"
    #更新
    update_info = "update algorithm_model \
                    set current_num=%s \
                    where code in " + values + " \
                    and sum_num > 0" 
    data_to_insert = (currentTurbineNum, *modeCode_)  
    cursor.execute(update_info, data_to_insert)

    #筛选待重置算法
    dataQuery = (*modeCode_,)
    cursor.execute(equalZeroCode, dataQuery)
    queryResult = cursor.fetchall()                

    conn.commit()
    cursor.close()
    conn.close()

    resetSumNames = [i[0] for i in queryResult]
    

    return resetSumNames

def UpdateTubineNum(modeCode,last_start_time, last_end_time, totalTurbineNum, currentTurbineNum):#conn, 
    modeCode_ = removeElementFromList([modeCode] if isinstance(modeCode,str) else modeCode, exceptAlgorithmList)
    if len(modeCode_) == 0:
        return
    conn = get_connection()
    cursor = conn.cursor()
    if currentTurbineNum == 1 and currentTurbineNum!=totalTurbineNum:
        update_info = "update algorithm_model \
                        set status=%s, \
                        last_start_time=%s, \
                        sum_num=%s, \
                        last_result_normal=%s, \
                        last_result_abnormal=%s, \
                        last_result_fail=%s, \
                        current_num=%s \
                        where code=%s"
        # update_query = "UPDATE algorithm_model \
        #                SET status = %s, \
        #                last_start_time = %s \
        #                WHERE code = %s"
        data_to_insert = (1, last_start_time, totalTurbineNum, 0, 0, 0, currentTurbineNum, *modeCode_)  
        cursor.execute(update_info, data_to_insert)
    elif currentTurbineNum == totalTurbineNum and currentTurbineNum!=1:
        update_info = "update algorithm_model \
                        set last_end_time=%s, \
                        current_num=%s \
                        where code=%s"
        data_to_insert = (last_end_time, currentTurbineNum, *modeCode_)  
        cursor.execute(update_info, data_to_insert)
    elif currentTurbineNum== totalTurbineNum and currentTurbineNum==1:
        update_info = "update algorithm_model \
                        set status=%s, \
                        last_start_time=%s, \
                        last_end_time=%s, \
                        sum_num=%s, \
                        last_result_normal=%s, \
                        last_result_abnormal=%s, \
                        last_result_fail=%s, \
                        current_num=%s \
                        where code=%s"
        data_to_insert = (1, last_start_time, last_end_time, totalTurbineNum, 0, 0, 0, currentTurbineNum, *modeCode_)  
        cursor.execute(update_info, data_to_insert)
    else:
        update_info = "update algorithm_model \
                        set current_num=%s \
                        where code=%s"
        data_to_insert = (currentTurbineNum, *modeCode_)  
        cursor.execute(update_info, data_to_insert)
    conn.commit()
    cursor.close()
    conn.close()

def UpdateAlgorithmInfo(multi_algorithms, no_alarm_models, alarm_models, exception_models):#conn, 
    counterNoAlarm = Counter(no_alarm_models)
    counterAlarm = Counter(alarm_models)
    counterException = Counter(exception_models)

    for algorithm in multi_algorithms:
        conn = get_connection()
        cursor = conn.cursor()
        name = algorithm.__name__.split('.')[-1]
        if name in exceptAlgorithmList:
            continue
        countAlarm = 0
        countNoAlarm = 0
        countExcept = 0
        if name in counterNoAlarm.keys():
            countNoAlarm = counterNoAlarm[name]
        if name in counterAlarm.keys():
            countAlarm = counterAlarm[name]
        if name in counterException.keys():
            countExcept = counterException[name]
        update_info = "update algorithm_model \
                        set sum_abnormal=%s, \
                        last_result_normal=%s, \
                        last_result_abnormal=%s, \
                        last_result_fail=%s \
                        where code=%s"
        data_to_insert = (countAlarm, countNoAlarm, countAlarm, countExcept, name) 
        cursor.execute(update_info, data_to_insert)
        conn.commit()
        cursor.close()
        conn.close()

def UpdateResult(multi_algorithms, no_alarm_models, alarm_models, exception_models, data_empty_models):#conn, 
    counterNoAlarm = Counter(no_alarm_models)
    counterAlarm = Counter(alarm_models)
    counterException = Counter(exception_models)
    counterDataEmpty = Counter(data_empty_models)

    for algorithm in multi_algorithms:
        conn = get_connection()
        cursor = conn.cursor()
        name = algorithm.__name__.split('.')[-1]
        if name in exceptAlgorithmList:
            continue
        countAlarm = 0
        countNoAlarm = 0
        countExcept = 0
        countDataEmpty = 0
        if name in counterNoAlarm.keys():
            countNoAlarm = counterNoAlarm[name]
        if name in counterAlarm.keys():
            countAlarm = counterAlarm[name]
        if name in counterException.keys():
            countExcept = counterException[name]
        if name in counterDataEmpty.keys():
            countDataEmpty = counterDataEmpty[name]
        update_info = "update algorithm_model \
                        set status=%s, \
                        last_result_content=%s \
                        where code=%s"
        strContent = f"算法生成报警次数:{countAlarm},算法未产生报警次数{countNoAlarm},算法执行异常次数{countExcept},未获得原始数据或清洗后无数据的次数{countDataEmpty}。"
        data_to_insert = (0, strContent, name) 
        cursor.execute(update_info, data_to_insert)
        conn.commit()
        cursor.close()
        conn.close()

def CheckThreshold(multi_algorithms, algorithm_configs):#conn, 
    for algorithm in multi_algorithms:
        conn = get_connection()
        cursor = conn.cursor()
        name = algorithm.__name__.split('.')[-1]
        if name in exceptAlgorithmList(algorithm):
            algorithm_configs[name]['threshold'] = {}
            continue
        queryItem = "select threshold from algorithm_model where code=%s"
        dataQuery = (name,)
        cursor.execute(queryItem, dataQuery)
        conn.commit()
        queryResult = cursor.fetchall()
        threshold = eval(queryResult[-1][0])
        if type(threshold) == type(None) or len(threshold) == 0:
            algorithm_configs[name]['threshold'] = {}
        else:
            algorithm_configs[name]['threshold'] = threshold
        cursor.close()
        conn.close()

    return algorithm_configs

def CheckModuleCode(modeCode):
    conn = get_connection()
    cursor = conn.cursor()
    # values = "(%s)" % ','.join(['%s'] * len(modeCode))
    #筛选model_code，输出module_code
    module_query = "select module_code,name from algorithm_model \
                       where code=%s"
    
    #筛选待重置算法
    dataQuery = (modeCode,)
    cursor.execute(module_query, dataQuery)
    queryResult = cursor.fetchall()                

    conn.commit()
    cursor.close()
    conn.close()
    module_code = queryResult[-1][0]
    module_name = queryResult[-1][1]

    return module_code, module_name

def save_alarm(assetId, alarmName, alarmTime, error_start_time, error_end_time):
    conn = get_connection()
    insert_alarm(conn, assetId, alarmName, alarmTime, error_start_time, error_end_time)

if __name__ == '__main__':
    save_alarm('xxx', '偏航一场', datetime.now(), datetime.now(), datetime.now())