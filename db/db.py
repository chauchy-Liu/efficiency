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
from urllib.parse import urlparse, parse_qs
import requests
import io
import re
from urllib.parse import quote

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

nan = -99999

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
    dirname =os.path.dirname(filename)
    dirList = dirname.split('/')

    dir = algorithms_configs['minio_dir']
    response = minio_client.fput_object(ylv['bucketName'], os.path.join(dir,dirList[-1]+'-'+basename), filename)
    log.info(
        filename+" is successfully uploaded as "
        "object " +os.path.join(dir,basename)+" to bucket "+ylv['bucketName']+"."
    )
    file_url = minio_client.presigned_get_object(ylv['bucketName'], os.path.join(dir,dirList[-1]+'-'+basename))
    index = file_url.find("?")
    file_url = file_url[:index]
    if ylv["domainName"] != None and len(ylv["domainName"]) > 0:#替换url为domain name
        index = file_url.find(ylv['bucketName']) - 1
        file_url = file_url[:len("http://")] + ylv['domainName'] + file_url[index:]
        log.info("url替换: "+ylv["url"]+'==>'+ylv['domainName'])
        log.info("替换后的URL: "+file_url)
    return file_url

def download(file_url)-> bytes:
    response = requests.get(file_url)
    imageContent = response.content
    #加载道内存
    imageStream = io.BytesIO(imageContent)
    return imageStream.getvalue()

#################################mysql#################################
create_theory_wind_power_table_query = f'''
    create table theory_wind_power (
        id int auto_increment primary key comment '主键',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wspd float comment '风速m/s',
        pwrt float comment '功率kw*h'
    ) comment='各场站和机型对应的理论功率曲线';
'''
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
        execute_time datetime not null comment '数据日期',
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
    create table turbine_warning_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        fault text comment '告警标识',
        time_rate float comment '告警时长',
        count float comment '告警次数',
        fault_describe text comment '告警描述'
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
        time_rate float comment '待机时长',
        loss float comment '损失电量',
        fault_describe text comment '故障描述'
    ) comment='技术故障损失';
'''
create_limturbine_loss_all_table_query = f'''
    create table limturbine_loss_all (
        id int auto_increment primary key comment '主键',
        data_time datetime not null comment '数据日期',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '机型名',
        wtid varchar(100) not null comment '风机号',
        wspd float comment '风速m/s',
        time_rate float comment '限电时间',
        loss float comment '损失电量'
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
        time_rate float comment '故障时长',
        loss float comment '损失电量',
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
        time_rate float comment '计划停机时长',
        loss float comment '损失电量',
        limgrid_continue_flag int comment '限电持续标志, 1:限电，0:非限电'
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
        time_rate float comment '限电',
        loss float comment '损失电量'
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
        time_rate float comment '故障时长',
        loss float comment '损失电量',
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
#wtid varchar(100) not null comment '风机号名',
create_wind_frequency_picture_table_query = f'''
    create table wind_frequency_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='风频图';
'''
create_wind_direction_picture_table_query = f'''
    create table wind_direction_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='风向图';
'''
        # wtid varchar(100) not null comment '风机号名',
create_air_density_picture_table_query = f'''
    create table air_density_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='空气密度图';
'''
        # wtid varchar(100) not null comment '风机号名',
create_turbulence_picture_table_query = f'''
    create table turbulence_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='湍流图';
'''
create_navigation_bias_direction_picture_table_query = f'''
    create table navigation_bias_direction_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        yaw_duifeng_err float comment '偏航对风误差', 
        yaw_duifeng_loss float comment '偏航对风损失',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='偏航对风图';
'''
create_navigation_bias_control_picture_table_query = f'''
    create table navigation_bias_control_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        yaw_leiji_err int comment '偏航累积误差',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='偏航控制图';
'''
create_pitch_angle_picture_table_query = f'''
    create table pitch_angle_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        file_name_compare text comment '对比图片名',
        minio_url_compare text comment '对比图片存储minio地址',
        pitch_min_loss float comment '最小桨距角功率损失',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='最小桨距角图';
'''
create_pitch_action_picture_table_query = f'''
    create table pitch_action_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='变桨动作图';
'''
create_pitch_unbalance_picture_table_query = f'''
    create table pitch_unbalance_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='变桨不平衡图';
'''
create_torque_control_picture_table_query = f'''
    create table torque_control_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='转矩控制图';
'''
create_device_picture_table_query = f'''
    create table device_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        wtid varchar(100) not null comment '风机号名',
        device varchar(100) not null comment '大部件名',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='大部件异常图';
'''

########################################################
#创建word相关表
########################################################
create_wind_resource_table_query = f'''
    create table wind_resource (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成风资源的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        windbin float comment '风仓',
        freq float comment '风频',
        count int comment '频数',
        wind_max float comment '最大风速',
        wind_mean float comment '平均风速',
        mean_rho float comment '平均空气密度',
        max_speed_month text comment '最大风速月份',
        turbulence float comment '湍流强度',
        turbulence_flag15 int comment '是否使用了15m/s的湍流强度',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='风资源表';
'''
create_power_curve_picture_table_query = f'''
    create table power_curve_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='功率曲线图';
'''
create_cp_picture_table_query = f'''
    create table cp_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='CP能量利用率曲线图';
'''
create_zuobiao_picture_table_query = f'''
    create table zuobiao_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='全场机组坐标分布图';
'''
create_fault_pie_picture_table_query = f'''
    create table fault_pie_picture (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='故障分布图';
'''
create_farmInfo_table_query = f'''
    create table farmInfo (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成记录的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        company varchar(100) not null comment '二级公司',
        address text comment '风场地址',
        capacity text comment '容量',
        turbine_num text comment '风机数量',
        turbine_type text comment '风机类型',
        wind_resource text comment '风资源',
        operate_time text comment '并网时间',
        rccID text comment '生产运营中心',
        path_farm text comment '本地存储路径',
        minio_dir text comment 'minio存储目录',
        wtid text comment '风机号',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='风场信息';
'''
create_word_table_query = f'''
    create table word (
        id bigint auto_increment primary key comment '主键', 
        execute_time datetime not null comment '生成图片的执行时间',
        farm_name varchar(100) not null comment '风场名',
        farm_id varchar(100) not null comment '风场ID',
        type_name varchar(100) not null comment '风机机型',
        minio_url text comment '图片存储minio地址',
        bucket_name text comment '桶名',
        file_name text comment '图片名',
        del_flag tinyint default 0 comment '删除数据标志位'
    ) comment='word文档';
'''

####################################################33
#提取数据
####################################################33
def selectFarmInfo(farmName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"####################################提取farmInfo数据############################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        company, \
        address, \
        capacity, \
        turbine_num, \
        turbine_type, \
        wind_resource, \
        operate_time, \
        rccID, \
        path_farm, \
        minio_dir, \
        wtid \
        from farmInfo where farm_name=%s and execute_time = (select max(execute_time) from farmInfo where execute_time <= %s and farm_name=%s)"
    data_query = (farmName, end_time, farmName)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query, data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        farm_id = queryResult[2]
        company = queryResult[3]
        address = queryResult[4]
        capacity = queryResult[5]
        turbine_num = queryResult[6]
        turbine_type = queryResult[7]
        wind_resource = float(queryResult[8])
        operate_time = queryResult[9]
        rccID = queryResult[10]
        path_farm = queryResult[11]
        minio_dir = queryResult[12]
        # if queryResult[13]:
        wtid = eval(queryResult[13])
        # else:
            # wtid = {turbine_type: []}
    return {
            'execute_time': execute_time,
            'farm_name': farm_name,
            'farm_id': farm_id,
            'company': company,
            'address': address,
            'capacity': capacity,
            'turbine_num': turbine_num,
            'turbine_type': turbine_type,
            'wind_resource': wind_resource,
            'operate_time': operate_time,
            'rccID': rccID,
            'path_farm': path_farm,
            'minio_dir': minio_dir,
            'wtid': wtid
           }

def selectWindResourceWord(farmName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"####################################提取wind_resource数据############################")
    data = pd.DataFrame()
    obtain_query = "SELECT \
            execute_time, \
            farm_name, \
            farm_id, \
            windbin, \
            freq, \
            count, \
            wind_max, \
            wind_mean, \
            mean_rho, \
            max_speed_month, \
            turbulence, \
            turbulence_flag15 \
            from wind_resource \
            where farm_name=%s AND execute_time = (select max(execute_time) from wind_resource where execute_time <= %s and farm_name=%s) \
        "
    data_to_obtain = (farmName, end_time, farmName)
    log.info(f'sql语句：{obtain_query}')
    log.info(f'sql数据：{data_to_obtain}')
    cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None or len(queryResult) <= 0:
        pass #return pd.DataFrame()
    else:
        for i, lineValue in enumerate(queryResult):
            # localtime = pd.to_datetime(lineValue[0], errors='coerce')
            #nan验证
            lineValue = list(lineValue)
            if lineValue[3] == nan:
                lineValue[3] = np.nan
            if lineValue[4] == nan:
                lineValue[4] = np.nan
            if lineValue[5] == nan:
                lineValue[5] = np.nan
            if lineValue[6] == nan:
                lineValue[6] = np.nan
            if lineValue[7] == nan:
                lineValue[7] = np.nan
            if lineValue[8] == nan:
                lineValue[8] = np.nan
            if lineValue[9] == nan:
                lineValue[9] = np.nan
            if lineValue[10] == nan:
                lineValue[10] = np.nan
            if lineValue[11] == nan:
                lineValue[11] = np.nan
            data.loc[i, ['windbin', 'freq', 'count', 'wind_max', 'wind_mean', 'mean_rho', 'max_speed_month', 'turbulence', 'turbulence_flag15']] = [lineValue[3], lineValue[4], lineValue[5], lineValue[6], lineValue[7], lineValue[8], lineValue[9], lineValue[10], lineValue[11]]
    return data

def selectAllWindFrequencyPicture(farmName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取wind_frequency_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        del_flag \
        from wind_frequency_picture \
        where farm_name=%s and type_name='all' and execute_time = (select max(execute_time) from wind_frequency_picture where execute_time  <= %s and farm_name=%s and type_name='all')\
    "
    data_query = (farmName, end_time, farmName)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 图片名: {file_name} , 图片URL：{minio_url}")
        
    return io.BytesIO(download(minio_url))

def selectWindFrequencyPicture(farmName, typeName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取wind_frequency_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        del_flag \
        from wind_frequency_picture \
        where farm_name=%s and type_name=%s and execute_time = (select max(execute_time) from wind_frequency_picture where execute_time %s and farm_name=%s and type_name=%s)\
    "
    data_query = (farmName, typeName, end_time, farmName, typeName)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 图片名: {file_name} , 图片URL：{minio_url}")
        
    return io.BytesIO(download(minio_url))

def selectWindDirectionPicture(farmName, typeName, wtid, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取wind_direction_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        wtid, \
        del_flag \
        from wind_direction_picture \
        where farm_name=%s and type_name=%s and wtid=%s and execute_time = (select max(execute_time) from wind_direction_picture where execute_time <= %s and farm_name=%s and type_name=%s and wtid=%s)\
    "
    data_query = (farmName, typeName, wtid, end_time, farmName, typeName, wtid)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        wtid = queryResult[7]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 风机名：{wtid}, 图片名: {file_name} , 图片URL：{minio_url}")
        
    return io.BytesIO(download(minio_url))

def selectAllAirDensityPicture(farmName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取air_density_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        del_flag \
        from air_density_picture \
        where farm_name=%s and type_name='all' and execute_time = (select max(execute_time) from air_density_picture where execute_time  <= %s and farm_name=%s and type_name='all')\
    "
    data_query = (farmName, end_time, farmName)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 图片名: {file_name} , 图片URL：{minio_url}")
        
    return io.BytesIO(download(minio_url))

def selectAirDensityPicture(farmName, typeName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取air_density_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        del_flag \
        from air_density_picture \
        where farm_name=%s and type_name=%s and execute_time = (select max(execute_time) from air_density_picture where execute_time <= %s and farm_name=%s and type_name=%s)\
    "
    data_query = (farmName, typeName, end_time, farmName, typeName)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 图片名: {file_name} , 图片URL：{minio_url}")
        
    return io.BytesIO(download(minio_url))

def selectAllTurbulencePicture(farmName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取turbulence_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        del_flag \
        from turbulence_picture \
        where farm_name=%s and type_name='all' and execute_time = (select max(execute_time) from turbulence_picture where execute_time <= %s and farm_name=%s and type_name='all')\
    "
    data_query = (farmName, end_time, farmName)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 图片名: {file_name} , 图片URL：{minio_url}")
        
    return io.BytesIO(download(minio_url))

def selectTurbulencePicture(farmName, typeName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取turbulence_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        del_flag \
        from turbulence_picture \
        where farm_name=%s and type_name=%s and execute_time = (select max(execute_time) from turbulence_picture where execute_time <= %s and farm_name=%s and type_name=%s)\
    "
    data_query = (farmName, typeName, end_time, farmName, typeName)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 图片名: {file_name} , 图片URL：{minio_url}")
        
    return io.BytesIO(download(minio_url))

def selectNavigationBiasDirectionPicture(farmName, typeName, wtid, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取navigation_bias_direction_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        wtid, \
        yaw_duifeng_err, \
        yaw_duifeng_loss \
        from navigation_bias_direction_picture \
        where farm_name=%s and type_name=%s and wtid=%s and execute_time = (select max(execute_time) from navigation_bias_direction_picture where farm_name=%s and type_name=%s and wtid=%s and execute_time between %s and %s)\
    "
    data_query = (farmName, typeName, wtid, farmName, typeName, wtid, start_time, end_time)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        wtid = queryResult[7]
        yaw_duifeng_err = queryResult[8]
        yaw_duifeng_loss = queryResult[9]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 风机名：{wtid}, 图片名: {file_name} , 图片URL：{minio_url}, 偏航对风误差：{yaw_duifeng_err}, 偏航对风损失：{yaw_duifeng_loss}")
        
    return wtid, io.BytesIO(download(minio_url)), yaw_duifeng_err, yaw_duifeng_loss


def selectNavigationBiasControlPicture(farmName, typeName, wtid, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取navigation_bias_control_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        wtid, \
        yaw_leiji_err, \
        del_flag \
        from navigation_bias_control_picture \
        where farm_name=%s and type_name=%s and wtid=%s and execute_time = (select max(execute_time) from navigation_bias_control_picture where farm_name=%s and type_name=%s and wtid=%s and execute_time  between %s and %s)\
    "
    data_query = (farmName, typeName, wtid, farmName, typeName, wtid, start_time, end_time)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        wtid = queryResult[7]
        yaw_leiji_err = queryResult[8]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 风机名：{wtid}, 图片名: {file_name} , 图片URL：{minio_url}, 偏航累积误差：{yaw_leiji_err}")
        
    return wtid, io.BytesIO(download(minio_url)), yaw_leiji_err

def selectPitchAnglePicture(farmName, typeName, wtid, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取pitch_angle_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        wtid, \
        file_name_compare, \
        minio_url_compare, \
        pitch_min_loss, \
        del_flag \
        from pitch_angle_picture \
        where farm_name=%s and type_name=%s and wtid=%s and execute_time = (select max(execute_time) from pitch_angle_picture where farm_name=%s and type_name=%s and wtid=%s and execute_time between %s and %s)\
    "
    data_query = (farmName, typeName, wtid, farmName, typeName, wtid, start_time, end_time)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        wtid = queryResult[7]
        file_name_compare = queryResult[8]
        minio_url_compare = queryResult[9]
        pitch_min_loss = queryResult[10]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 风机名：{wtid}, 图片名: {file_name} , 图片URL：{minio_url}, 对比图片名: {file_name_compare}, 对比图片URL：{minio_url_compare}, 最小偏航损失：{pitch_min_loss}")
        
    return wtid, io.BytesIO(download(minio_url)), download(minio_url_compare), pitch_min_loss

def selectPitchActionPicture(farmName, typeName, wtid, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取pitch_action_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        wtid, \
        del_flag \
        from pitch_action_picture \
        where farm_name=%s and type_name=%s and wtid=%s and execute_time = (select max(execute_time) from pitch_action_picture where farm_name=%s and type_name=%s and wtid=%s and execute_time between %s and %s)\
    "
    data_query = (farmName, typeName, wtid, farmName, typeName, wtid, start_time, end_time)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        wtid = queryResult[7]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 风机名：{wtid}, 图片名: {file_name} , 图片URL：{minio_url}")
        
    return wtid, io.BytesIO(download(minio_url))
def selectPitchUnbalancePicture(farmName, typeName, wtid, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取pitch_unbalance_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        wtid, \
        del_flag \
        from pitch_unbalance_picture \
        where farm_name=%s and type_name=%s and wtid=%s and execute_time = (select max(execute_time) from pitch_unbalance_picture where farm_name=%s and type_name=%s and wtid=%s and execute_time between %s and %s)\
    "
    data_query = (farmName, typeName, wtid, farmName, typeName, wtid, start_time, end_time)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        wtid = queryResult[7]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 风机名：{wtid}, 图片名: {file_name} , 图片URL：{minio_url}")
        
    return wtid, io.BytesIO(download(minio_url))

def selectTorqueControlPicture(farmName, typeName, wtid, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取torque_control_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        wtid, \
        del_flag \
        from torque_control_picture \
        where farm_name=%s and type_name=%s and wtid=%s and execute_time = (select max(execute_time) from torque_control_picture where farm_name=%s and type_name=%s and wtid=%s and execute_time between %s and %s)\
    "
    data_query = (farmName, typeName, wtid, farmName, typeName, wtid, start_time, end_time)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        wtid = queryResult[7]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 风机名：{wtid}, 图片名: {file_name} , 图片URL：{minio_url}")
        
    return wtid, io.BytesIO(download(minio_url))

def selectAllZuobiaoPicture(farmName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取zuobiao_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        del_flag \
        from zuobiao_picture \
        where farm_name=%s and type_name='all' and execute_time = (select max(execute_time) from zuobiao_picture where farm_name=%s and type_name='all' and execute_time <= %s)"
    data_query = (farmName, farmName, end_time)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query, data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 图片名: {file_name} , 图片URL：{minio_url}")
        
    return io.BytesIO(download(minio_url))

def selectPowerCurvePicture(farmName, typeName, fileName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取power_curve_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        del_flag \
        from power_curve_picture \
        where farm_name=%s and type_name=%s and file_name like %s and execute_time = (select max(execute_time) from power_curve_picture where execute_time <= %s and farm_name=%s and type_name=%s)\
    "
    data_query = (farmName, typeName, '%'+quote(fileName)+'%', end_time, farmName, typeName)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 图片名: {file_name} , 图片URL：{minio_url}")
        
    return io.BytesIO(download(minio_url))

def selectCPPicture(farmName, typeName, fileName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取cp_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        del_flag \
        from cp_picture \
        where farm_name=%s and type_name=%s and file_name like %s and execute_time = (select max(execute_time) from cp_picture where execute_time <= %s and farm_name=%s and type_name=%s)\
    "
    data_query = (farmName, typeName, '%'+quote(fileName)+'%', end_time, farmName, typeName)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 图片名: {file_name} , 图片URL：{minio_url}")
        
    return io.BytesIO(download(minio_url))

def selectDevicePicture(farmName, typeName, wtid, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取device_picture数据####################")
    query = "SELECT \
        execute_time, \
        farm_name, \
        farm_id, \
        type_name, \
        minio_url, \
        bucket_name, \
        file_name, \
        wtid, \
        device \
        from device_picture \
        where farm_name=%s and type_name=%s and wtid=%s and execute_time = (select max(execute_time) from device_picture where execute_time <= %s and farm_name=%s and type_name=%s)\
    "
    data_query = (farmName, typeName, wtid, end_time, farmName, typeName)
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query,data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        execute_time = queryResult[0]
        farm_name = queryResult[1]
        type_name = queryResult[3]
        file_name = queryResult[6]
        minio_url = queryResult[4]
        wtid = queryResult[7]
        device = queryResult[8]
        log.info(f"图片生成时间：{execute_time}, 风场名：{farm_name}, 风机类型：{type_name}, 风机名：{wtid}, 设备：{device}, 图片名: {file_name} , 图片URL：{minio_url}")
        
    return wtid, device, io.BytesIO(download(minio_url))

####################################################################3

def selectFaultCode(turbingType: str):
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取turbineTypeToFaultCodeMap数据####################")
    fileName = None
    query = "SELECT \
        typeName, \
        primeIndex, \
        secondIndex, \
        thirdIndex, \
        fileName \
        from turbineTypeToFaultCodeMap \
    "
    log.info(f'sql语句：{query}')
    cursor.execute(query)
    queryResult = cursor.fetchall()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        for i, lineValue in enumerate(queryResult):
            if lineValue[0] != None and re.search(lineValue[0], turbingType, re.IGNORECASE):
                if lineValue[1] != None and lineValue[1] in turbingType:
                    if lineValue[2] != None and lineValue[2] in turbingType:
                        if lineValue[3] != None and lineValue[3] in turbingType:
                            fileName = lineValue[-1]
                            break
                        else:
                            if lineValue[3] != None:
                                continue
                            else: 
                                fileName = lineValue[-1]
                                break
                    else:
                        if lineValue[2] != None:
                            continue
                        else: 
                            fileName = lineValue[-1]
                            break
                else:
                    if lineValue[1] != None:
                        continue
                    else: 
                        fileName = lineValue[-1]
                        break
            else:
                continue
    
    return fileName
#功率限限停
def selectZeroWrop(farmName, start_time, end_time):
    data = pd.DataFrame()
    conn = get_connection_zero()
    cursor = conn.cursor()
    log.info(f"##################################提取zero_wrop_warn数据####################")
    obtain_query = "SELECT \
        station_name, \
        warn_start_time, \
        warn_end_time \
        from zero_wrop_warn \
        where (station_name LIKE %s) AND ((warn_start_time >= %s AND warn_start_time < %s) OR (warn_start_time < %s AND warn_end_time >= %s)) \
    "
    data_to_obtain = (farmName[:-1]+'%', start_time, end_time, start_time, start_time)
    log.info(f'sql语句：{obtain_query}')
    log.info(f'sql数据：{data_to_obtain}')
    cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None or len(queryResult) <= 0:
        pass #return pd.DataFrame()
    else:
        for i, lineValue in enumerate(queryResult):
            startTime = pd.to_datetime(lineValue[1],errors='coerce')
            # endTime = pd.to_datetime(lineValue[2],errors='coerce')
            #nan验证
            lineValue = list(lineValue)
            if startTime < start_time:
                startTime = start_time
            if lineValue[2] == None:
                endTime = pd.to_datetime(datetime.now(),errors='coerce')
                if endTime > end_time:
                    endTime = end_time
            elif pd.to_datetime(lineValue[1],errors='coerce') > end_time:
                endTime = end_time
            else:
                endTime = pd.to_datetime(lineValue[2],errors='coerce')
            
            data.loc[i, ['start_time','end_time']] = [startTime, endTime]
    data.replace(b'', 0, inplace=True)
    return data

def selectTheoryWindPower(farmName, typeName): #typeName是一个机型，不多个机型
    conn = get_connection()
    cursor = conn.cursor()
    wspd = []
    pwrt = []
    log.info(f"##################################提取theory_wind_power数据####################")
    obtain_query = "SELECT \
        farm_name, \
        farm_id, \
        type_name, \
        wspd, \
        pwrt \
        from theory_wind_power \
        where farm_name=%s AND type_name=%s \
    "
    data_to_obtain = (farmName, typeName)
    log.info(f'sql语句：{obtain_query}')
    log.info(f'sql数据：{data_to_obtain}')
    cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None or len(queryResult) <= 0:
        pass #return pd.DataFrame()
    else:
        for lineValue in queryResult:
            localtime = pd.to_datetime(lineValue[0],errors='coerce')
            #nan验证
            lineValue = list(lineValue)
            if lineValue[3] == nan:
                lineValue[3] = np.nan
            if lineValue[4] == nan:
                lineValue[4] = np.nan
            wspd.append(lineValue[3])
            pwrt.append(lineValue[4])
    return wspd, pwrt

def selectPwTimeAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    # if len(typeName) > 0: #str or []
    #     wtids = []
    # else:
    #     wtids = {}
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"####################################提取pw_time_all数据############################")
    # queryItem = "select max(data_time) from pw_time_all"
    # # dataQuery = (turbine_name,)
    # log.info(f'sql语句：{queryItem}')
    # # log.info(f'sql数据：{dataQuery}')
    # cursor.execute(queryItem)
    # result = cursor.fetchall()
    # if result != None and result[0] != None:
    #     max_sql_time = result[0][-1]
    # else:
    #     max_sql_time = "2020-10-24 00:00:00" 
    #     max_sql_time = datetime.strptime(max_sql_time, "%Y-%m-%d %H:%M:%S")
#插入每个机子大于已有数据的时间最大值的数据
#截取dataframe的列，只摄取当前几号的列
    if len(typeName) > 0:
        obtain_query = "SELECT \
            execute_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wind_bin, \
            pwrt_mean, \
            pwrt, \
            count \
            from pw_time_all \
            where farm_name=%s AND type_name=%s AND execute_time=(select max(execute_time) from pw_time_all where farm_name=%s AND type_name=%s) \
        "
        data_to_obtain = (farmName, typeName, farmName, typeName)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
    else:
        obtain_query = "SELECT \
            execute_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wind_bin, \
            pwrt_mean, \
            pwrt, \
            count \
            from pw_time_all \
            where farm_name=%s AND execute_time=(select max(execute_time) from pw_time_all where farm_name=%s) \
        "
        data_to_obtain = (farmName, farmName)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None or len(queryResult) <= 0:
        pass #return pd.DataFrame()
    else:
        #重复机名的次数
        wtidCount = 0
        firstWtidName = None
        for i, lineValue in enumerate(queryResult):
            localtime = pd.to_datetime(lineValue[0],errors='coerce')
            if i == 0:
                firstWtidName = lineValue[4]
                wtidCount += 1
            else:
                if lineValue[4] == firstWtidName:
                    wtidCount += 1
            #nan验证
            lineValue = list(lineValue)
            if lineValue[5] == nan:
                lineValue[5] = np.nan
            if lineValue[6] == nan:
                lineValue[6] = np.nan
            if lineValue[7] == nan:
                lineValue[7] = np.nan
            if lineValue[8] == nan:
                lineValue[8] = np.nan
            if lineValue[4] == firstWtidName:
                data.loc[i, ['windbin','pwrat',lineValue[4],lineValue[4]+'_count']] = [lineValue[5], lineValue[6],lineValue[7],lineValue[8]]
                # if len(typeName) > 0:
                #     wtids.append(lineValue[4])
                # else:
                #     wtids[lineValue[3]] = [lineValue[4]]
            else:
                data.loc[i%wtidCount, ['windbin','pwrat',lineValue[4],lineValue[4]+'_count']] = [lineValue[5], lineValue[6],lineValue[7],lineValue[8]]
                # if len(typeName) > 0:
                #     wtids.append(lineValue[4])
                # else:
                #     wtids[lineValue[3]] += [lineValue[4]]
    data.replace(b'', 0, inplace=True)
    return data#, wtids

def selectPwTurbineAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    if len(typeName) > 0:
        wtids = []
    else:
        wtids = {}
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##################################提取pw_turbine_all数据####################")
    if len(typeName) > 0:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wspd, \
            pwrt \
            from pw_turbine_all \
            where farm_name=%s AND type_name=%s AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, typeName, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
    else:
        obtain_query = "SELECT \
                data_time, \
                farm_name, \
                farm_id, \
                type_name, \
                wtid, \
                wspd, \
                pwrt \
                from pw_turbine_all \
                where farm_name=%s AND data_time BETWEEN %s AND %s \
            "
        data_to_obtain = (farmName, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain) 
    queryResult = cursor.fetchall()
    if queryResult == None or len(queryResult) <= 0:
        pass #return pd.DataFrame()
    else:
        # for lineValue in queryResult:
        #     localtime = pd.to_datetime(lineValue[0],errors='coerce')
        #     #nan验证
        #     lineValue = list(lineValue)
        #     if lineValue[5] == nan:
        #         lineValue[5] = np.nan
        #     if lineValue[6] == nan:
        #         lineValue[6] = np.nan
        #     data.loc[localtime, ['type',lineValue[4]+'_wspd',lineValue[4]]] = [lineValue[3], lineValue[5],lineValue[6]]
        #     if len(typeName) > 0:
        #         if lineValue[4] not in wtids:
        #             wtids.append(lineValue[4])
        #     else:
        #         if lineValue[3] in wtids:
        #             wtids[lineValue[3]].append(lineValue[4])
        #         else:
        #             wtids[lineValue[3]] = [lineValue[4]]
        originTable = pd.DataFrame(queryResult)
        turbineType = list(set(originTable[3]))[-1]
        originTable = originTable[originTable[3]==turbineType]
        turbineNames = list(set(originTable[4]))
        turbineNameWspd = [x+'_wspd' for x in turbineNames]
        localtime = sorted(list(set(originTable[0].apply(lambda x: pd.to_datetime(x,errors='coerce')))))
        data = pd.DataFrame(columns=['type']+turbineNames+turbineNameWspd, index=localtime)
        for i, ele in enumerate(turbineNames):
            tmp = originTable[originTable[4]==ele]
            tmp.set_index(0, inplace=True)
            common_indices = data.index.intersection(tmp.index)
            data.loc[common_indices, ele] = tmp.loc[common_indices, 6]
            data.loc[common_indices, turbineNameWspd[i]] = tmp.loc[common_indices, 5]
        data['type'] = turbineType
        #nan验证
        data.replace(nan, np.nan, inplace=True)
        if len(typeName) > 0:
            wtids = turbineNames
        else:
            wtids[turbineType] = turbineNames
        
    data.replace(b'', 0, inplace=True)
    return data, wtids

def selectTurbineWarningAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"###########################提取turbine_warning_all数据###############")
    wtids = {}
    #查询表名
    check_table_query = f"show tables like 'turbine_warning_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        columns = ['type', 'wtid', 'fault', 'count', 'time', 'wspd', 'fault_describe']
        data = pd.DataFrame(columns=columns)
        return data, wtids
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
            wspd, \
            time_rate, \
            fault_describe \
            from technology_loss_all \
            where farm_name=%s AND type_name=%s  AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
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
            wspd, \
            time_rate, \
            fault_describe \
            from technology_loss_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None or len(queryResult) <= 0:
        pass #return pd.DataFrame()
    else:
        if len(typeName) > 0:
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                #nan验证
                lineValue = list(lineValue)
                # if lineValue[9] == nan:
                #     lineValue[9] = np.nan
                if lineValue[6] == nan:
                    lineValue[6] = np.nan
                if lineValue[7] == nan:
                    lineValue[7] = np.nan
                if lineValue[8] == nan:
                    lineValue[8] = np.nan
                data.loc[i, ['localtime', 'type', 'wtid', 'fault', 'count', 'time', 'wspd', 'fault_describe']] = [localtime, lineValue[3], lineValue[4],lineValue[5], lineValue[6], lineValue[8], lineValue[7], lineValue[9]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
        else:
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                #nan验证
                lineValue = list(lineValue)
                # if lineValue[9] == nan:
                #     lineValue[9] = np.nan
                if lineValue[6] == nan:
                    lineValue[6] = np.nan
                if lineValue[7] == nan:
                    lineValue[7] = np.nan
                if lineValue[8] == nan:
                    lineValue[8] = np.nan
                data.loc[i, ['localtime','type', 'wtid', 'fault', 'count', 'time', 'wspd', 'fault_describe']] = [localtime, lineValue[3], lineValue[4],lineValue[5], lineValue[6], lineValue[8],  lineValue[7], lineValue[9]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
                if lineValue[3] in typeName:
                    pass
                else:
                    typeName.append(lineValue[3])
    data.replace(b'', 0, inplace=True)
    if 'localtime' in data.columns.to_list():
        data.set_index('localtime', inplace=True)
    return data, wtids

def selectTechnologyLossAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"###########################提取technology_loss_all数据###############")
    wtids = {}
    #查询表名
    check_table_query = f"show tables like 'technology_loss_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        columns = ['type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe']
        data = pd.DataFrame(columns=columns)
        return data, wtids
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
            wspd, \
            time_rate, \
            loss, \
            fault_describe \
            from technology_loss_all \
            where farm_name=%s AND type_name=%s  AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
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
            wspd, \
            time_rate, \
            loss, \
            fault_describe \
            from technology_loss_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None or len(queryResult) <= 0:
        pass #return pd.DataFrame()
    else:
        if len(typeName) > 0:
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                #nan验证
                lineValue = list(lineValue)
                if lineValue[9] == nan:
                    lineValue[9] = np.nan
                if lineValue[6] == nan:
                    lineValue[6] = np.nan
                if lineValue[7] == nan:
                    lineValue[7] = np.nan
                if lineValue[8] == nan:
                    lineValue[8] = np.nan
                data.loc[i, ['localtime', 'type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe']] = [localtime, lineValue[3], lineValue[4],lineValue[5], lineValue[6], lineValue[8], lineValue[9], lineValue[7], lineValue[10]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
        else:
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                #nan验证
                lineValue = list(lineValue)
                if lineValue[9] == nan:
                    lineValue[9] = np.nan
                if lineValue[6] == nan:
                    lineValue[6] = np.nan
                if lineValue[7] == nan:
                    lineValue[7] = np.nan
                if lineValue[8] == nan:
                    lineValue[8] = np.nan
                data.loc[i, ['localtime', 'type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe']] = [localtime, lineValue[3], lineValue[4],lineValue[5], lineValue[6], lineValue[8], lineValue[9], lineValue[7], lineValue[10]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
                if lineValue[3] in typeName:
                    pass
                else:
                    typeName.append(lineValue[3])
    data.replace(b'', 0, inplace=True)
    if 'localtime' in data.columns.to_list():
        data.set_index('localtime', inplace=True)
    return data, wtids
def selectLimturbineLossAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"############################提取limturbine_loss_all数据###########################")
    wtids = {}
    #查询表名
    check_table_query = f"show tables like 'limturbine_loss_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        columns = ['type', 'wtid', 'time', 'loss', 'wspd']
        data = pd.DataFrame(columns=columns)
        return data, wtids
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
            wspd, \
            time_rate, \
            loss \
            from limturbine_loss_all \
            where farm_name=%s AND type_name=%s  AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
    else:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wspd, \
            time_rate, \
            loss \
            from limturbine_loss_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None or len(queryResult) <= 0:
        pass #return pd.DataFrame()
    else:
        if len(typeName) > 0:
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                #nan验证
                lineValue = list(lineValue)
                if lineValue[5] == nan:
                    lineValue[5] = np.nan
                if lineValue[6] == nan:
                    lineValue[6] = np.nan
                if lineValue[7] == nan:
                    lineValue[7] = np.nan
                data.loc[i, ['localtime', 'type', 'wtid', 'time', 'loss', 'wspd']] = [localtime, lineValue[3], lineValue[4],lineValue[6], lineValue[7], lineValue[5]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
        else:
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                #nan验证
                lineValue = list(lineValue)
                if lineValue[5] == nan:
                    lineValue[5] = np.nan
                if lineValue[6] == nan:
                    lineValue[6] = np.nan
                if lineValue[7] == nan:
                    lineValue[7] = np.nan
                data.loc[i, ['localtime', 'type', 'wtid', 'time', 'loss', 'wspd']] = [localtime, lineValue[3], lineValue[4],lineValue[6], lineValue[7], lineValue[5]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
                if lineValue[3] in typeName:
                    pass
                else:
                    typeName.append(lineValue[3])
    data.replace(b'', 0, inplace=True)
    if 'localtime' in data.columns.to_list():
        data.set_index('localtime', inplace=True)
    return data,wtids
def selectFaultgridLossAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    wtids = {}
    log.info(f"############################提取faultgrid_loss_all数据#####################")
    #查询表名
    check_table_query = f"show tables like 'faultgrid_loss_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        columns = ['type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe']
        data = pd.DataFrame(columns=columns)
        return data, wtids
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
            wspd, \
            time_rate, \
            loss, \
            fault_describe \
            from faultgrid_loss_all \
            where farm_name=%s AND type_name=%s  AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
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
            wspd, \
            time_rate, \
            loss, \
            fault_describe \
            from faultgrid_loss_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None or len(queryResult) <= 0:
        pass #return pd.DataFrame()
    else:
        if len(typeName) > 0:
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                #nan验证
                lineValue = list(lineValue)
                if lineValue[9] == nan:
                    lineValue[9] = np.nan
                if lineValue[6] == nan:
                    lineValue[6] = np.nan
                if lineValue[7] == nan:
                    lineValue[7] = np.nan
                if lineValue[8] == nan:
                    lineValue[8] = np.nan
                data.loc[i, ['localtime', 'type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe']] = [localtime, lineValue[3], lineValue[4],lineValue[5], lineValue[6], lineValue[8], lineValue[9], lineValue[7], lineValue[10]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
        else:
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                #nan验证
                lineValue = list(lineValue)
                if lineValue[9] == nan:
                    lineValue[9] = np.nan
                if lineValue[6] == nan:
                    lineValue[6] = np.nan
                if lineValue[7] == nan:
                    lineValue[7] = np.nan
                if lineValue[8] == nan:
                    lineValue[8] = np.nan
                data.loc[i, ['localtime', 'type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe']] = [localtime, lineValue[3], lineValue[4],lineValue[5], lineValue[6], lineValue[8], lineValue[9], lineValue[7], lineValue[10]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
                if lineValue[3] in typeName:
                    pass
                else:
                    typeName.append(lineValue[3])
    data.replace(b'', 0, inplace=True)
    if 'localtime' in data.columns.to_list():
        data.set_index('localtime', inplace=True)
    return data, wtids
def selectStopLossAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##############################提取stop_loss_all数据######################")
    wtids = {}
    #查询表名
    check_table_query = f"show tables like 'stop_loss_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        columns = ['type', 'wtid', 'time', 'loss', 'wspd', 'exltmp', 'limgrid_continue_flag']
        data = pd.DataFrame(columns=columns)
        return data, wtids
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
            wspd, \
            time_rate, \
            loss, \
            exltmp, \
            limgrid_continue_flag \
            from stop_loss_all \
            where farm_name=%s AND type_name=%s  AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
    else:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wspd, \
            time_rate, \
            loss, \
            exltmp, \
            limgrid_continue_flag \
            from stop_loss_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None or len(queryResult) <= 0:
        pass #return pd.DataFrame()
    else:
        if len(typeName) > 0:
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                #nan验证
                lineValue = list(lineValue)
                if lineValue[5] == nan:
                    lineValue[5] = np.nan
                if lineValue[6] == nan:
                    lineValue[6] = np.nan
                if lineValue[7] == nan:
                    lineValue[7] = np.nan
                if lineValue[8] == nan:
                    lineValue[8] = np.nan
                data.loc[i, ['localtime', 'type', 'wtid', 'time', 'loss', 'wspd', 'exltmp', 'limgrid_continue_flag']] = [localtime, lineValue[3], lineValue[4],lineValue[6], lineValue[7], lineValue[5], lineValue[8], lineValue[9]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
        else:
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                #nan验证
                lineValue = list(lineValue)
                if lineValue[5] == nan:
                    lineValue[5] = np.nan
                if lineValue[6] == nan:
                    lineValue[6] = np.nan
                if lineValue[7] == nan:
                    lineValue[7] = np.nan
                if lineValue[8] == nan:
                    lineValue[8] = np.nan
                data.loc[i, ['localtime', 'type', 'wtid', 'time', 'loss', 'wspd', 'exltmp', 'limgrid_continue_flag']] = [localtime, lineValue[3], lineValue[4],lineValue[6], lineValue[7], lineValue[5], lineValue[8], lineValue[9]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
                if lineValue[3] in typeName:
                    pass
                else:
                    typeName.append(lineValue[3])
    data.replace(b'', 0, inplace=True)
    if 'localtime' in data.columns.to_list():
        data.set_index('localtime', inplace=True)
    return data, wtids
def selectFaultLossAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"########################提取fault_loss_all数据########################")
    wtids = {}
    #查询表名
    check_table_query = f"show tables like 'fault_loss_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        columns = ['type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe', 'fsyst']
        data = pd.DataFrame(columns=columns)
        return data, wtids
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
            wspd, \
            time_rate, \
            loss, \
            fault_describe, \
            fsyst \
            from fault_loss_all \
            where farm_name=%s AND type_name in (%s)  AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
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
            wspd, \
            time_rate, \
            loss, \
            fault_describe, \
            fsyst \
            from fault_loss_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None or len(queryResult) <= 0:
        pass #return pd.DataFrame()
    else:
        if len(typeName) > 0:
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                #nan验证
                lineValue = list(lineValue)
                if lineValue[9] == nan:
                    lineValue[9] = np.nan
                if lineValue[6] == nan:
                    lineValue[6] = np.nan
                if lineValue[7] == nan:
                    lineValue[7] = np.nan
                if lineValue[8] == nan:
                    lineValue[8] = np.nan
                data.loc[i, ['localtime', 'type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe', 'fsyst']] = [localtime, lineValue[3], lineValue[4],lineValue[5], lineValue[6], lineValue[8], lineValue[9], lineValue[7], lineValue[10], lineValue[11]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
        else:
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                #nan验证
                lineValue = list(lineValue)
                if lineValue[9] == nan:
                    lineValue[9] = np.nan
                if lineValue[6] == nan:
                    lineValue[6] = np.nan
                if lineValue[7] == nan:
                    lineValue[7] = np.nan
                if lineValue[8] == nan:
                    lineValue[8] = np.nan
                data.loc[i, ['localtime', 'type', 'wtid', 'fault', 'count', 'time', 'loss', 'wspd', 'fault_describe', 'fsyst']] = [localtime, lineValue[3], lineValue[4],lineValue[5], lineValue[6], lineValue[8], lineValue[9], lineValue[7], lineValue[10], lineValue[11]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
                if lineValue[3] in typeName:
                    pass
                else:
                    typeName.append(lineValue[3])
    data.replace(b'', 0, inplace=True)
    if 'localtime' in data.columns.to_list():
        data.set_index('localtime', inplace=True)
    return data, wtids
def selectLimgridLossAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    if isinstance(start_time, str):
        startTimeStr = start_time # datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        startTimeStr = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(startTimeStr, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_time, str):
        endTimeStr = end_time #datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    else:
        endTimeStr = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"##########################提取limgrid_loss_all数据#################")
    wtids = {}
    #查询表名
    check_table_query = f"show tables like 'limgrid_loss_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        columns = ['type', 'wtid', 'time', 'loss', 'wspd']
        data = pd.DataFrame(columns=columns)
        return data, wtids
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
            wspd, \
            time_rate, \
            loss \
            from limgrid_loss_all \
            where farm_name=%s AND type_name=%s  AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
    else:
        obtain_query = "SELECT \
            data_time, \
            farm_name, \
            farm_id, \
            type_name, \
            wtid, \
            wspd, \
            time_rate, \
            loss \
            from limgrid_loss_all \
            where farm_name=%s AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None or len(queryResult) <= 0:
        pass #return pd.DataFrame()
    else:
        if len(typeName) > 0:
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                #nan验证
                lineValue = list(lineValue)
                if lineValue[5] == nan:
                    lineValue[5] = np.nan
                if lineValue[6] == nan:
                    lineValue[6] = np.nan
                if lineValue[7] == nan:
                    lineValue[7] = np.nan
                data.loc[i, ['localtime', 'type', 'wtid', 'time', 'loss', 'wspd']] = [localtime, lineValue[3], lineValue[4],lineValue[6], lineValue[7], lineValue[5]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
        else:
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                #nan验证
                lineValue = list(lineValue)
                if lineValue[5] == nan:
                    lineValue[5] = np.nan
                if lineValue[6] == nan:
                    lineValue[6] = np.nan
                if lineValue[7] == nan:
                    lineValue[7] = np.nan
                data.loc[i, ['localtime', 'type', 'wtid', 'time', 'loss', 'wspd']] = [localtime, lineValue[3], lineValue[4],lineValue[6], lineValue[7], lineValue[5]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
                if lineValue[3] in typeName:
                    pass
                else:
                    typeName.append(lineValue[3])
    data.replace(b'', 0, inplace=True)
    if 'localtime' in data.columns.to_list():
        data.set_index('localtime', inplace=True)
    return data, wtids

def selectEnyWspdAll(data, farmName, typeName, start_time=datetime.now()-timedelta(days=91), end_time=datetime.now()-timedelta(days=1)):
    startTimeStr = datetime.strftime(start_time, "%Y-%m-%d")#stop_loss
    start_time = datetime.strptime(startTimeStr, "%Y-%m-%d")
    endTimeStr = datetime.strftime(end_time, "%Y-%m-%d")
    end_time = datetime.strptime(endTimeStr, "%Y-%m-%d")
    conn = get_connection()
    cursor = conn.cursor()
    log.info(f"###################提取eny_wspd_all数据#######################")
    wtids = {}
    #查询表名
    check_table_query = f"show tables like 'eny_wspd_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        columns = ['type', 'wtid', 'eny', 'wspd', 'count', 'Rate_power']
        data = pd.DataFrame(columns=columns)
        return data, wtids
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
            where farm_name=%s AND type_name=%s  AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, typeNameStr, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
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
            where farm_name=%s AND data_time BETWEEN %s AND %s \
        "
        data_to_obtain = (farmName, start_time, end_time)
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
    queryResult = cursor.fetchall()
    if queryResult == None or len(queryResult) <= 0:
        pass #return pd.DataFrame()
    else:
        if len(typeName) > 0:#字符串
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                data.loc[i, ['localtime', 'type', 'wtid', 'eny', 'wspd', 'count', 'Rate_power']] = [localtime, lineValue[3], lineValue[4],lineValue[5], lineValue[6], lineValue[7], lineValue[8]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
        else: #空列表代表全机组
            for i, lineValue in enumerate(queryResult):
                localtime = pd.to_datetime(lineValue[0],errors='coerce')
                data.loc[i, ['localtime', 'type', 'wtid', 'eny', 'wspd', 'count', 'Rate_power']] = [localtime, lineValue[3], lineValue[4],lineValue[5], lineValue[6], lineValue[7], lineValue[8]]
                if lineValue[3] in wtids:
                    if lineValue[4] not in wtids[lineValue[3]]:
                        wtids[lineValue[3]].append(lineValue[4])
                else:
                    wtids[lineValue[3]] = [lineValue[4]]
                if lineValue[3] in typeName:
                    pass
                else:
                    typeName.append(lineValue[3])
    data.replace(b'', 0, inplace=True)
    if 'localtime' in data.columns.to_list():
        data.set_index('localtime', inplace=True)
    return data, wtids
###################################################
#录入数据
###################################################

########################存功率##################################
def insertTheoryWindPower(algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'theory_wind_power';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_theory_wind_power_table_query}')
        cursor.execute(create_theory_wind_power_table_query)
        #插入数据
        log.info(f"#########################theory_wind_power表插入数据#########################")
        #遍历风仓
        for j in len(algorithms_configs["wspdTheory"]):
            insert_query = "INSERT INTO theory_wind_power ( \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wspd, \
                    pwrt \
                    ) VALUES (%s, %s, %s, %s, %s)"
            #nan验证
            if algorithms_configs["wspdTheory"][j] == np.nan or str(algorithms_configs["wspdTheory"][j]) == 'nan':
                wspd = nan
            else:
                wspd = algorithms_configs["wspdTheory"][j]
            if algorithms_configs["pwratTheory"][j] == np.nan or str(algorithms_configs["pwratTheory"][j]) == 'nan':
                pwrat = nan
            else:
                pwrat = algorithms_configs["pwratTheory"][j]
            data_to_insert = (algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['turbineTypeID'], wspd, pwrat)
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"#########################theory_wind_power表插入数据#########################")
        obtain_query = "SELECT \
            farm_name, \
            farm_id, \
            type_name, \
            wspd, \
            pwrt \
            from theory_wind_power \
            where farm_name=%s AND type_name=%s \
        "
        data_to_obtain = (algorithms_configs['farmName'], algorithms_configs['turbineTypeID'])
        log.info(f'sql语句：{obtain_query}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(obtain_query, data_to_obtain)
        queryResult = cursor.fetchall()    
        if not queryResult and len(queryResult) == 0:
            #遍历风仓
            for j in range(len(algorithms_configs["wspdTheory"])):
                insert_query = "INSERT INTO theory_wind_power ( \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wspd, \
                        pwrt \
                        ) VALUES (%s, %s, %s, %s, %s)"
                #nan验证
                if algorithms_configs["wspdTheory"][j] == np.nan or str(algorithms_configs["wspdTheory"][j]) == 'nan':
                    wspd = nan
                else:
                    wspd = algorithms_configs["wspdTheory"][j]
                if algorithms_configs["pwratTheory"][j] == np.nan or str(algorithms_configs["pwratTheory"][j]) == 'nan':
                    pwrat = nan
                else:
                    pwrat = algorithms_configs["pwratTheory"][j]
                data_to_insert = (algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['turbineTypeID'], wspd, pwrat)
                log.info(f'sql语句：{insert_query}')
                log.info(f'sql数据：{data_to_insert}')
                cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close()

def insertPwTimeAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'pw_time_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_pw_time_all_table_query}')
        cursor.execute(create_pw_time_all_table_query)
        #插入数据
        log.info(f"#################pw_time_all表插入数据#########################")
        #查询每个机子
        for i in range(len(algorithms_configs['wtids'])):
            turbine_name = algorithms_configs['wtids'][i]
        #插入每个机子大于已有数据的时间最大值的数据
        #截取dataframe的列，只摄取当前几号的列
            tmp = data[['windbin', 'pwrat', turbine_name, turbine_name+'_count']]
        #遍历时间
            for j in range(tmp.shape[0]):
                insert_query = "INSERT INTO pw_time_all ( \
                        execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        wind_bin, \
                        pwrt_mean, \
                        pwrt, \
                        count \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                #nan验证
                if tmp.iloc[j]['windbin'] == np.nan or str(tmp.iloc[j]['windbin']) == 'nan':
                    windbin = nan
                else:
                    windbin = tmp.iloc[j]['windbin']
                if tmp.iloc[j]['pwrat'] == np.nan or str(tmp.iloc[j]['pwrat']) == 'nan':
                    pwrat = nan
                else:
                    pwrat = tmp.iloc[j]['pwrat']
                if tmp.iloc[j][turbine_name] == np.nan or str(tmp.iloc[j][turbine_name]) == 'nan':
                    turbine_pwrat = nan
                else:
                    turbine_pwrat = tmp.iloc[j][turbine_name]
                if tmp.iloc[j][turbine_name+'_count'] == np.nan or str(tmp.iloc[j][turbine_name+'_count']) == 'nan':
                    count = nan
                else:
                    count = tmp.iloc[j][turbine_name+'_count']
                data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, windbin,pwrat, turbine_pwrat, count)
                log.info(f'sql语句：{insert_query}')
                log.info(f'sql数据：{data_to_insert}')
                cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"#########################pw_time_all表插入数据#########################")
        #查询表中每个机子已有数据的时间最大值
        for i in range(len(algorithms_configs['wtids'])):
            turbine_name = algorithms_configs['wtids'][i]
            # queryItem = "select max(execute_time) from pw_time_all where wtid=%s"
            # dataQuery = (turbine_name,)
            # log.info(f'sql语句：{queryItem}')
            # log.info(f'sql数据：{dataQuery}')
            # cursor.execute(queryItem, dataQuery)
            # result = cursor.fetchone()
            # if result != None and result[0] != None:
            #     max_sql_time = result[0]
            # else:
            #     max_sql_time = "2020-10-24 00:00:00" 
            #     max_sql_time = datetime.strptime(max_sql_time, "%Y-%m-%d %H:%M:%S")
        #插入每个机子大于已有数据的时间最大值的数据
        #截取dataframe的列，只摄取当前几号的列
            tmp = data[['windbin', 'pwrat', turbine_name, turbine_name+'_count']]
            # tmp = tmp[tmp.index > max_sql_time] 
        #遍历时间
            for j in range(tmp.shape[0]):
                insert_query = "INSERT INTO pw_time_all ( \
                        execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        wind_bin, \
                        pwrt_mean, \
                        pwrt, \
                        count \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                #nan验证
                if tmp.iloc[j]['windbin'] == np.nan or str(tmp.iloc[j]['windbin']) == 'nan':
                    windbin = nan
                else:
                    windbin = tmp.iloc[j]['windbin']
                if tmp.iloc[j]['pwrat'] == np.nan or str(tmp.iloc[j]['pwrat']) == 'nan':
                    pwrat = nan
                else:
                    pwrat = tmp.iloc[j]['pwrat']
                if tmp.iloc[j][turbine_name] == np.nan or str(tmp.iloc[j][turbine_name]) == 'nan':
                    turbine_pwrat = nan
                else:
                    turbine_pwrat = tmp.iloc[j][turbine_name]
                if tmp.iloc[j][turbine_name+'_count'] == np.nan or str(tmp.iloc[j][turbine_name+'_count']) == 'nan':
                    count = nan
                else:
                    count = tmp.iloc[j][turbine_name+'_count']
                data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, windbin,pwrat, turbine_pwrat, count)
                log.info(f'sql语句：{insert_query}')
                log.info(f'sql数据：{data_to_insert}')
                cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close()

def insertPwTurbineAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'pw_turbine_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_pw_turbine_all_table_query}')
        cursor.execute(create_pw_turbine_all_table_query)
        #插入数据
        log.info(f"#########################pw_turbine_all表插入数据#########################")
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
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                #nan验证
                if tmp.iloc[j][turbine_name+'_wspd'] == np.nan or str(tmp.iloc[j][turbine_name+'_wspd']) == 'nan':
                    wspd = nan
                else:
                    wspd = tmp.iloc[j][turbine_name+'_wspd']
                if tmp.iloc[j][turbine_name] == np.nan or str(tmp.iloc[j][turbine_name]) == 'nan':
                    pwrat = nan
                else:
                    pwrat = tmp.iloc[j][turbine_name]
                data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], turbine_name, wspd, pwrat)
                log.info(f'sql语句：{insert_query}')
                log.info(f'sql数据：{data_to_insert}')
                cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"#########################pw_turbine_all表插入数据#########################")
        #查询表中每个机子已有数据的时间最大值
        for i in range(len(algorithms_configs['wtids'])):
            turbine_name = algorithms_configs['wtids'][i]
            queryItem = "select max(data_time) from pw_turbine_all where wtid=%s and farm_name=%s AND type_name=%s"
            dataQuery = (turbine_name, algorithms_configs['farmName'], algorithms_configs['typeName'])
            log.info(f'sql语句：{queryItem}')
            log.info(f'sql数据：{dataQuery}')
            cursor.execute(queryItem, dataQuery)
            result = cursor.fetchone()
            if result != None and result[0] != None:
                max_sql_time = result[0]
            else:
                max_sql_time = "2020-10-24 00:00:00"
                max_sql_time = datetime.strptime(max_sql_time, "%Y-%m-%d %H:%M:%S")
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
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                #nan验证
                if tmp.iloc[j][turbine_name+'_wspd'] == np.nan or str(tmp.iloc[j][turbine_name+'_wspd']) == 'nan':
                    wspd = nan
                else:
                    wspd = tmp.iloc[j][turbine_name+'_wspd']
                if tmp.iloc[j][turbine_name] == np.nan or str(tmp.iloc[j][turbine_name]) == 'nan':
                    pwrat = nan
                else:
                    pwrat = tmp.iloc[j][turbine_name]
                data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], turbine_name, wspd, pwrat)
                log.info(f'sql语句：{insert_query}')
                log.info(f'sql数据：{data_to_insert}')
                cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close()

########################存损失电量##################################

def insertTurbineWarningAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'turbine_warning_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_turbine_warning_all_table_query}')
        cursor.execute(create_turbine_warning_all_table_query)
        #插入数据
        log.info(f"#########################turbine_warning_all表插入数据#########################")
        
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'fault', 'count', 'time', 'wspd', 'fault_describe']]
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO turbine_warning_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    fault, \
                    count, \
                    wspd, \
                    time_rate, \
                    fault_describe \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            #nan验证
            if tmp.iloc[j]['count'] == np.nan or str(tmp.iloc[j]['count']) == 'nan':
                count = nan
            else:
                count = tmp.iloc[j]['count']
            if tmp.iloc[j]['wspd'] == np.nan or str(tmp.iloc[j]['wspd']) == 'nan':
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan or str(tmp.iloc[j]['time']) == 'nan':
                time = nan
            else:
                time = tmp.iloc[j]['time']
            # if tmp.iloc[j]['loss'] == np.nan or str(tmp.iloc[j]['loss']) == 'nan':
            #     loss = nan
            # else:
            #     loss = tmp.iloc[j]['loss']
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['fault'], count, wspd, time, tmp.iloc[j]['fault_describe'])
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"#########################turbine_warning_all表插入数据#########################")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from turbine_warning_all where farm_name=%s AND type_name=%s"
        data_to_obtain = (algorithms_configs['farmName'], algorithms_configs['typeName'])
        log.info(f'sql语句：{queryItem}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(queryItem, data_to_obtain)
        result = cursor.fetchone()
        if result != None and result[0] != None:
            max_sql_time = result[0]
        else:
            max_sql_time = "2020-10-24 00:00:00"
            max_sql_time = datetime.strptime(max_sql_time, "%Y-%m-%d %H:%M:%S")
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'fault', 'count', 'time', 'wspd', 'fault_describe']]
        tmp = tmp[tmp.index > max_sql_time] 
        #遍历时间
        for j in range(tmp.shape[0]):
            insert_query = "INSERT INTO turbine_warning_all ( \
                    data_time, \
                    farm_name, \
                    farm_id, \
                    type_name, \
                    wtid, \
                    fault, \
                    count, \
                    wspd, \
                    time_rate, \
                    fault_describe \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            #nan验证
            if tmp.iloc[j]['count'] == np.nan or str(tmp.iloc[j]['count']) == 'nan':
                count = nan
            else:
                count = tmp.iloc[j]['count']
            if tmp.iloc[j]['wspd'] == np.nan or str(tmp.iloc[j]['wspd']) == 'nan':
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan or str(tmp.iloc[j]['time']) == 'nan':
                time = nan
            else:
                time = tmp.iloc[j]['time']
            # if tmp.iloc[j]['loss'] == np.nan or str(tmp.iloc[j]['loss']) == 'nan':
            #     loss = nan
            # else:
            #     loss = tmp.iloc[j]['loss']
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['fault'], count, wspd, time, tmp.iloc[j]['fault_describe'])
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close() 
def insertTechnologyLossAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'technology_loss_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_technology_loss_all_table_query}')
        cursor.execute(create_technology_loss_all_table_query)
        #插入数据
        log.info(f"#########################technology_loss_all表插入数据#########################")
        
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
            #nan验证
            if tmp.iloc[j]['count'] == np.nan or str(tmp.iloc[j]['count']) == 'nan':
                count = nan
            else:
                count = tmp.iloc[j]['count']
            if tmp.iloc[j]['wspd'] == np.nan or str(tmp.iloc[j]['wspd']) == 'nan':
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan or str(tmp.iloc[j]['time']) == 'nan':
                time = nan
            else:
                time = tmp.iloc[j]['time']
            if tmp.iloc[j]['loss'] == np.nan or str(tmp.iloc[j]['loss']) == 'nan':
                loss = nan
            else:
                loss = tmp.iloc[j]['loss']
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['fault'], count, wspd, time, loss, tmp.iloc[j]['fault_describe'])
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"#########################technology_loss_all表插入数据#########################")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from technology_loss_all where farm_name=%s AND type_name=%s"
        data_to_obtain = (algorithms_configs['farmName'], algorithms_configs['typeName'])
        log.info(f'sql语句：{queryItem}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(queryItem, data_to_obtain)
        result = cursor.fetchone()
        if result != None and result[0] != None:
            max_sql_time = result[0]
        else:
            max_sql_time = "2020-10-24 00:00:00"
            max_sql_time = datetime.strptime(max_sql_time, "%Y-%m-%d %H:%M:%S")
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
            #nan验证
            if tmp.iloc[j]['count'] == np.nan or str(tmp.iloc[j]['count']) == 'nan':
                count = nan
            else:
                count = tmp.iloc[j]['count']
            if tmp.iloc[j]['wspd'] == np.nan or str(tmp.iloc[j]['wspd']) == 'nan':
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan or str(tmp.iloc[j]['time']) == 'nan':
                time = nan
            else:
                time = tmp.iloc[j]['time']
            if tmp.iloc[j]['loss'] == np.nan or str(tmp.iloc[j]['loss']) == 'nan':
                loss = nan
            else:
                loss = tmp.iloc[j]['loss']
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['fault'], count, wspd, time, loss, tmp.iloc[j]['fault_describe'])
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close() 
def insertLimturbineLossAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'limturbine_loss_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_limturbine_loss_all_table_query}')
        cursor.execute(create_limturbine_loss_all_table_query)
        #插入数据
        log.info(f"#########################limturbine_loss_all表插入数据#########################")
        
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
            #nan验证
            if tmp.iloc[j]['wspd'] == np.nan or str(tmp.iloc[j]['wspd']) == 'nan':
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan or str(tmp.iloc[j]['time']) == 'nan':
                time = nan
            else:
                time = tmp.iloc[j]['time']
            if tmp.iloc[j]['loss'] == np.nan or str(tmp.iloc[j]['loss']) == 'nan':
                loss = nan
            else:
                loss = tmp.iloc[j]['loss']
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], wspd, time, loss)
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"#########################limturbine_loss_all表插入数据#########################")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from limturbine_loss_all where farm_name=%s AND type_name=%s"
        data_to_obtain = (algorithms_configs['farmName'], algorithms_configs['typeName'])
        log.info(f'sql语句：{queryItem}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(queryItem, data_to_obtain)
        result = cursor.fetchone()
        if result != None and result[0] != None:
            max_sql_time = result[0]
        else:
            max_sql_time = "2020-10-24 00:00:00"
            max_sql_time = datetime.strptime(max_sql_time, "%Y-%m-%d %H:%M:%S")
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
            #nan验证
            if tmp.iloc[j]['wspd'] == np.nan or str(tmp.iloc[j]['wspd']) == 'nan':
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan or str(tmp.iloc[j]['time']) == 'nan':
                time = nan
            else:
                time = tmp.iloc[j]['time']
            if tmp.iloc[j]['loss'] == np.nan or str(tmp.iloc[j]['loss']) == 'nan':
                loss = nan
            else:
                loss = tmp.iloc[j]['loss']
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], wspd, time, loss)
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close() 
def insertFaultgridLossAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'faultgrid_loss_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_faultgrid_loss_all_table_query}')
        cursor.execute(create_faultgrid_loss_all_table_query)
        #插入数据
        log.info(f"###########################faultgrid_loss_all表插入数据#########################")
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
            #nan验证
            if tmp.iloc[j]['count'] == np.nan or str(tmp.iloc[j]['count']) == 'nan':
                count = nan
            else:
                count = tmp.iloc[j]['count']
            if tmp.iloc[j]['wspd'] == np.nan or str(tmp.iloc[j]['wspd']) == 'nan':
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan or str(tmp.iloc[j]['time']) == 'nan':
                time = nan
            else:
                time = tmp.iloc[j]['time']
            if tmp.iloc[j]['loss'] == np.nan or str(tmp.iloc[j]['loss']) == 'nan':
                loss = nan
            else:
                loss = tmp.iloc[j]['loss']
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['fault'], count, wspd, time, loss, tmp.iloc[j]['fault_describe'])
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"#########################faultgrid_loss_all表插入数据#########################")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from faultgrid_loss_all where farm_name=%s AND type_name=%s"
        data_to_obtain = (algorithms_configs['farmName'], algorithms_configs['typeName'])
        log.info(f'sql语句：{queryItem}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(queryItem, data_to_obtain)
        result = cursor.fetchone()
        if result != None and result[0] != None:
            max_sql_time = result[0]
        else:
            max_sql_time = "2020-10-24 00:00:00" 
            max_sql_time = datetime.strptime(max_sql_time, "%Y-%m-%d %H:%M:%S")
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
            #nan验证
            if tmp.iloc[j]['count'] == np.nan or str(tmp.iloc[j]['count']) == 'nan':
                count = nan
            else:
                count = tmp.iloc[j]['count']
            if tmp.iloc[j]['wspd'] == np.nan or str(tmp.iloc[j]['wspd']) == 'nan':
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan or str(tmp.iloc[j]['time']) == 'nan':
                time = nan
            else:
                time = tmp.iloc[j]['time']
            if tmp.iloc[j]['loss'] == np.nan or str(tmp.iloc[j]['loss']) == 'nan':
                loss = nan
            else:
                loss = tmp.iloc[j]['loss']
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['fault'], count, wspd, time, loss, tmp.iloc[j]['fault_describe'])
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close() 
def insertStopLossAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'stop_loss_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_stop_loss_all_table_query}')
        cursor.execute(create_stop_loss_all_table_query)
        #插入数据
        log.info(f"#########################stop_loss_all表插入数据#########################")
        
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'time', 'loss', 'wspd', 'exltmp', 'limgrid_continue_flag']]
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
                    exltmp, \
                    limgrid_continue_flag \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            #nan验证
            if tmp.iloc[j]['exltmp'] == np.nan or str(tmp.iloc[j]['exltmp']) == 'nan':
                exltmp = nan
            else:
                exltmp = tmp.iloc[j]['exltmp']
            if tmp.iloc[j]['wspd'] == np.nan or str(tmp.iloc[j]['wspd']) == 'nan':
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan or str(tmp.iloc[j]['time']) == 'nan':
                time = nan
            else:
                time = tmp.iloc[j]['time']
            if tmp.iloc[j]['loss'] == np.nan or str(tmp.iloc[j]['loss']) == 'nan':
                loss = nan
            else:
                loss = tmp.iloc[j]['loss']
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], wspd, time, loss, exltmp, int(tmp.iloc[j]['limgrid_continue_flag']))
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"#########################stop_loss_all表插入数据#########################")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from stop_loss_all where farm_name=%s AND type_name=%s"
        data_to_obtain = (algorithms_configs['farmName'], algorithms_configs['typeName'])
        log.info(f'sql语句：{queryItem}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(queryItem, data_to_obtain)
        result = cursor.fetchone()
        if result != None and result[0] != None:
            max_sql_time = result[0]
        else:
            max_sql_time = "2020-10-24 00:00:00" 
            max_sql_time = datetime.strptime(max_sql_time, "%Y-%m-%d %H:%M:%S")
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'time', 'loss', 'wspd', 'exltmp', 'limgrid_continue_flag']]
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
                    exltmp, \
                    limgrid_continue_flag \
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            #nan验证
            if tmp.iloc[j]['exltmp'] == np.nan or str(tmp.iloc[j]['exltmp']) == 'nan':
                exltmp = nan
            else:
                exltmp = tmp.iloc[j]['exltmp']
            if tmp.iloc[j]['wspd'] == np.nan or str(tmp.iloc[j]['wspd']) == 'nan':
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan or str(tmp.iloc[j]['time']) == 'nan':
                time = nan
            else:
                time = tmp.iloc[j]['time']
            if tmp.iloc[j]['loss'] == np.nan or str(tmp.iloc[j]['loss']) == 'nan':
                loss = nan
            else:
                loss = tmp.iloc[j]['loss']
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], wspd, time, loss, exltmp, int(tmp.iloc[j]['limgrid_continue_flag']))
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close() 
def insertFaultLossAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'fault_loss_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_fault_loss_all_table_query}')
        cursor.execute(create_fault_loss_all_table_query)
        #插入数据
        log.info(f"#########################fault_loss_all表插入数据#########################")
        
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
            #nan验证
            if tmp.iloc[j]['count'] == np.nan or str(tmp.iloc[j]['count']) == 'nan':
                count = nan
            else:
                count = tmp.iloc[j]['count']
            if tmp.iloc[j]['wspd'] == np.nan or str(tmp.iloc[j]['wspd']) == 'nan':
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan or str(tmp.iloc[j]['time']) == 'nan':
                time = nan
            else:
                time = tmp.iloc[j]['time']
            if tmp.iloc[j]['loss'] == np.nan or str(tmp.iloc[j]['loss']) == 'nan':
                loss = nan
            else:
                loss = tmp.iloc[j]['loss']
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['fault'], count, wspd, time, loss, tmp.iloc[j]['fault_describe'], tmp.iloc[j]['fsyst'])
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"#########################fault_loss_all表插入数据#########################")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from fault_loss_all where farm_name=%s AND type_name=%s"
        data_to_obtain = (algorithms_configs['farmName'], algorithms_configs['typeName'])
        log.info(f'sql语句：{queryItem}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(queryItem, data_to_obtain)
        result = cursor.fetchone()
        if result != None and result[0] != None:
            max_sql_time = result[0]
        else:
            max_sql_time = "2020-10-24 00:00:00" 
            max_sql_time = datetime.strptime(max_sql_time, "%Y-%m-%d %H:%M:%S")
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
            #nan验证
            if tmp.iloc[j]['count'] == np.nan or str(tmp.iloc[j]['count']) == 'nan':
                count = nan
            else:
                count = tmp.iloc[j]['count']
            if tmp.iloc[j]['wspd'] == np.nan or str(tmp.iloc[j]['wspd']) == 'nan':
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan or str(tmp.iloc[j]['time']) == 'nan':
                time = nan
            else:
                time = tmp.iloc[j]['time']
            if tmp.iloc[j]['loss'] == np.nan or str(tmp.iloc[j]['loss']) == 'nan':
                loss = nan
            else:
                loss = tmp.iloc[j]['loss']
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['fault'], count, wspd, time, loss, tmp.iloc[j]['fault_describe'], tmp.iloc[j]['fsyst'])
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close()

def insertLimgridLossAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'limgrid_loss_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_limgrid_loss_all_table_query}')
        cursor.execute(create_limgrid_loss_all_table_query)
        #插入数据
        log.info(f"#########################limgrid_loss_all表插入数据#########################")
        
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
            #nan验证
            if tmp.iloc[j]['wspd'] == np.nan or str(tmp.iloc[j]['wspd']) == 'nan':
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan or str(tmp.iloc[j]['time']) == 'nan':
                time = nan
            else:
                time = tmp.iloc[j]['time']
            if tmp.iloc[j]['loss'] == np.nan or str(tmp.iloc[j]['loss']) == 'nan':
                loss = nan
            else:
                loss = tmp.iloc[j]['loss']
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], wspd, time, loss)
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"#########################limgrid_loss_all表插入数据#########################")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from limgrid_loss_all where farm_name=%s AND type_name=%s"
        data_to_obtain = (algorithms_configs['farmName'], algorithms_configs['typeName'])
        log.info(f'sql语句：{queryItem}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(queryItem, data_to_obtain)
        result = cursor.fetchone()
        if result != None and result[0] != None:
            max_sql_time = result[0]
        else:
            max_sql_time = "2020-10-24 00:00:00" 
            max_sql_time = datetime.strptime(max_sql_time, "%Y-%m-%d %H:%M:%S")
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
            #nan验证
            if tmp.iloc[j]['wspd'] == np.nan or str(tmp.iloc[j]['wspd']) == 'nan':
                wspd = nan
            else:
                wspd = tmp.iloc[j]['wspd']
            if tmp.iloc[j]['time'] == np.nan or str(tmp.iloc[j]['time']) == 'nan':
                time = nan
            else:
                time = tmp.iloc[j]['time']
            if tmp.iloc[j]['loss'] == np.nan or str(tmp.iloc[j]['loss']) == 'nan':
                loss = nan
            else:
                loss = tmp.iloc[j]['loss']
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], wspd, time, loss)
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close() 

def insertEnyWspdAll(data, algorithms_configs):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'eny_wspd_all';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_eny_wspd_all_table_query}')
        cursor.execute(create_eny_wspd_all_table_query)
        #插入数据
        log.info(f"#########################eny_wspd_all表插入数据#########################")
        
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'eny', 'wspd', 'count', 'Rate_power']]
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
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['eny'], tmp.iloc[j]['wspd'], tmp.iloc[j]['count'], tmp.iloc[j]['Rate_power'])
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)
    else:
        #插入数据
        log.info(f"#########################eny_wspd_all表插入数据#########################")
        #查询表中已有数据的时间最大值
        queryItem = "select max(data_time) from eny_wspd_all where farm_name=%s AND type_name=%s"
        data_to_obtain = (algorithms_configs['farmName'], algorithms_configs['typeName'])
        log.info(f'sql语句：{queryItem}')
        log.info(f'sql数据：{data_to_obtain}')
        cursor.execute(queryItem, data_to_obtain)
        result = cursor.fetchone()
        if result != None and result[0] != None:
            max_sql_time = result[0]
        else:
            max_sql_time = "2020-10-24 00:00:00" 
            max_sql_time = datetime.strptime(max_sql_time, "%Y-%m-%d %H:%M:%S")
        #dataframe摄取全部的列
        tmp = data[['type', 'wtid', 'eny', 'wspd', 'count', 'Rate_power']]
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
            data_to_insert = (tmp.index[j], algorithms_configs['farmName'], algorithms_configs['farmId'], tmp.iloc[j]['type'], tmp.iloc[j]['wtid'], tmp.iloc[j]['eny'], tmp.iloc[j]['wspd'], tmp.iloc[j]['count'], tmp.iloc[j]['Rate_power'])
            log.info(f'sql语句：{insert_query}')
            log.info(f'sql数据：{data_to_insert}')
            cursor.execute(insert_query, data_to_insert)

    conn.commit()
    cursor.close() 


#############################存图片地址######################################
def insertAllWindFrequencyPicture(algorithms_configs, url_path):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'wind_frequency_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_wind_frequency_picture_table_query}')
        cursor.execute(create_wind_frequency_picture_table_query)
    #插入数据
    log.info(f"#########################wind_frequency_picture表插入数据#########################")
    insert_query = "INSERT INTO wind_frequency_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], 'all', file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertWindFrequencyPicture(algorithms_configs, url_path):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'wind_frequency_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_wind_frequency_picture_table_query}')
        cursor.execute(create_wind_frequency_picture_table_query)
    #插入数据
    log.info(f"#########################wind_frequency_picture表插入数据#########################")
    insert_query = "INSERT INTO wind_frequency_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertWindDirectionPicture(algorithms_configs, url_path, turbine_name):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'wind_direction_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_wind_direction_picture_table_query}')
        cursor.execute(create_wind_direction_picture_table_query)
    #插入数据
    log.info(f"#########################wind_direction_picture表插入数据#########################")
    insert_query = "INSERT INTO wind_direction_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()

def insertAllAirDensityPicture(algorithms_configs, url_path):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'air_density_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_air_density_picture_table_query}')
        cursor.execute(create_air_density_picture_table_query)
    #插入数据
    log.info(f"#########################air_density_picture表插入数据#########################")
    insert_query = "INSERT INTO air_density_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], 'all', file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()

def insertAirDensityPicture(algorithms_configs, url_path):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'air_density_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{check_table_query}')
        cursor.execute(create_air_density_picture_table_query)
    #插入数据
    log.info(f"#########################air_density_picture表插入数据#########################")
    insert_query = "INSERT INTO air_density_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()

def insertAllTurbulencePicture(algorithms_configs, url_path):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'turbulence_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_turbulence_picture_table_query}')
        cursor.execute(create_turbulence_picture_table_query)
    #插入数据
    log.info(f"#########################turbulence_picture表插入数据#########################")
    insert_query = "INSERT INTO turbulence_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], 'all', file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()

def insertTurbulencePicture(algorithms_configs, url_path):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'turbulence_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_turbulence_picture_table_query}')
        cursor.execute(create_turbulence_picture_table_query)
    #插入数据
    log.info(f"#########################turbulence_picture表插入数据#########################")
    insert_query = "INSERT INTO turbulence_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertNavigationBiasDirectionPicture(algorithms_configs, url_path, turbine_name, yaw_duifeng_err, yaw_duifeng_loss):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'navigation_bias_direction_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_navigation_bias_direction_picture_table_query}')
        cursor.execute(create_navigation_bias_direction_picture_table_query)
    #插入数据
    log.info(f"#########################navigation_bias_direction_picture表插入数据#########################")
    insert_query = "INSERT INTO navigation_bias_direction_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        file_name, \
                        bucket_name, \
                        minio_url, \
                        yaw_duifeng_err, \
                        yaw_duifeng_loss \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, file_name, bucket_name, url_path, yaw_duifeng_err, yaw_duifeng_loss)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertNavigationBiasControlPicture(algorithms_configs, url_path, turbine_name, yaw_leiji_err):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'navigation_bias_control_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_navigation_bias_control_picture_table_query}')
        cursor.execute(create_navigation_bias_control_picture_table_query)
    #插入数据
    log.info(f"#########################navigation_bias_control_picture表插入数据#########################")
    insert_query = "INSERT INTO navigation_bias_control_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        file_name, \
                        bucket_name, \
                        minio_url, \
                        yaw_leiji_err \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, file_name, bucket_name, url_path, float(yaw_leiji_err.values[0]))
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertPitchAnglePicture(algorithms_configs, url_path, turbine_name):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'pitch_angle_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_pitch_angle_picture_table_query}')
        cursor.execute(create_pitch_angle_picture_table_query)
    #插入数据
    log.info(f"#########################pitch_angle_picture表插入数据#########################")
    insert_query = "INSERT INTO pitch_angle_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def updatePitchAnglePicture(algorithms_configs, url_path, turbine_name, pitch_min_loss):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'pitch_angle_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_pitch_angle_picture_table_query}')
        cursor.execute(create_pitch_angle_picture_table_query)
    #更新数据
    log.info(f"#########################pitch_angle_picture表更新数据#########################")
    update_query = "UPDATE pitch_angle_picture SET \
                        file_name_compare = %s, \
                        minio_url_compare = %s, \
                        pitch_min_loss = %s  \
                    WHERE execute_time=%s and farm_name=%s and type_name=%s and wtid=%s"
    data_to_insert = (file_name, url_path, float(pitch_min_loss.values[0]), algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['typeName'], turbine_name)
    log.info(f'sql语句：{update_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(update_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertPitchActionPicture(algorithms_configs, url_path, turbine_name):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'pitch_action_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_pitch_action_picture_table_query}')
        cursor.execute(create_pitch_action_picture_table_query)
    #插入数据
    log.info(f"#########################pitch_action_picture表插入数据#########################")
    insert_query = "INSERT INTO pitch_action_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertPitchUnbalancePicture(algorithms_configs, url_path, turbine_name):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'pitch_unbalance_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_pitch_unbalance_picture_table_query}')
        cursor.execute(create_pitch_unbalance_picture_table_query)
    #插入数据
    log.info(f"#########################pitch_unbalance_picture表插入数据#########################")
    insert_query = "INSERT INTO pitch_unbalance_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertTorqueControlPicture(algorithms_configs, url_path, turbine_name):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'torque_control_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_torque_control_picture_table_query}')
        cursor.execute(create_torque_control_picture_table_query)
    #插入数据
    log.info(f"#########################torque_control_picture表插入数据#########################")
    insert_query = "INSERT INTO torque_control_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()


def insertDevicePicture(algorithms_configs, url_path, turbine_name, device_name):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'device_picture_table_query';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_device_picture_table_query}')
        cursor.execute(create_device_picture_table_query)
    #插入数据
    log.info(f"#########################device_picture_table_query表插入数据#########################")
    insert_query = "INSERT INTO device_picture_table_query (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        wtid, \
                        device, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], turbine_name, device_name, file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()


##########################################################
#插入word相关表
##########################################################
def insertFarmInfo(algorithms_configs, path_farm):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'farmInfo';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_farmInfo_table_query}')
        cursor.execute(create_farmInfo_table_query)
    #插入数据
    log.info(f"#########################farmInfo表插入数据#########################")
    
    insert_query = "INSERT INTO farmInfo (execute_time, \
                        farm_name, \
                        farm_id, \
                        company, \
                        address, \
                        capacity, \
                        turbine_num, \
                        turbine_type, \
                        wind_resource, \
                        operate_time, \
                        rccID, \
                        path_farm, \
                        minio_dir \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = [algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['company'], algorithms_configs['address'], algorithms_configs['capacity'], algorithms_configs['turbineNum'], algorithms_configs['turbineType'], algorithms_configs['windResource'], algorithms_configs['operateTime'], algorithms_configs['rccID'], path_farm, algorithms_configs['minio_dir']]
    for i in range(len(data_to_insert)):
        if str(data_to_insert[i]) == 'nan':
            data_to_insert[i] = None
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()

def addWtidToFarmInfo(algorithms_configs, wtidNew):
    conn = get_connection()
    cursor = conn.cursor()
    #修改数据
    log.info(f"#########################farmInfo表修改wtid数据#########################")
    query = "SELECT \
        wtid \
        from farmInfo where farm_name=%s and execute_time = %s"
    data_query = (algorithms_configs["farmName"], algorithms_configs["jobTime"])
    log.info(f'sql语句：{query}')
    log.info(f'sql数据：{data_query}')
    cursor.execute(query, data_query)
    queryResult = cursor.fetchone()
    if queryResult == None or len(queryResult) <= 0:
        return None
    else:
        if queryResult[0] != None:
            wtidOld = eval(queryResult[0])
        else:
            wtidOld = {}
    #合并wtid
    wtid = {**wtidOld, **wtidNew}
    update_query = "UPDATE farmInfo SET wtid = %s WHERE farm_name = %s and execute_time = %s"
    data_to_update = (str(wtid), algorithms_configs['farmName'], algorithms_configs['jobTime'])
    log.info(f'sql语句：{update_query}')
    log.info(f'sql数据：{data_to_update}')
    cursor.execute(update_query, data_to_update)
    conn.commit()
    cursor.close()

def insertWindResourceWord(algorithms_configs, windFreq, windMax, wind_mean):
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'wind_resource';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_wind_resource_table_query}')
        cursor.execute(create_wind_resource_table_query)
    #插入数据
    log.info(f"#########################wind_resource表插入数据#########################")
    for index, row in windFreq.iterrows():
        insert_query = "INSERT INTO wind_resource (execute_time, \
                            farm_name, \
                            farm_id, \
                            windbin, \
                            freq, \
                            count, \
                            wind_max, \
                            wind_mean \
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], row['windbin'], row['freq'], row['count'], windMax, wind_mean)
        log.info(f'sql语句：{insert_query}')
        log.info(f'sql数据：{data_to_insert}')
        cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()

def updateWindResourceWord(algorithms_configs, mean_rho, max_speed_month, turbulence, turbulence_flag15):
    conn = get_connection()
    cursor = conn.cursor()
    #修改数据
    log.info(f"#########################wind_resource表修改wtid数据#########################")
    # query = "SELECT \
    #     mean_rho, \
    #     max_speed_month \
    #     from wind_resource where farm_name=%s and execute_time = %s"
    # data_query = (algorithms_configs["farmName"], algorithms_configs["jobTime"])
    # log.info(f'sql语句：{query}')
    # log.info(f'sql数据：{data_query}')
    # cursor.execute(query, data_query)
    # queryResult = cursor.fetchone()
    # if queryResult == None or len(queryResult) <= 0:
    #     return None
    # else:
    #     if queryResult[0] != None:
    #         mean_rho_ = eval(queryResult[0])
    #     else:
    #         mean_rho = ""
    # wtid = {**wtidOld, **wtidNew}
    update_query = "UPDATE wind_resource SET mean_rho = %s, max_speed_month = %s, turbulence = %s, turbulence_flag15 = %s WHERE farm_name = %s and execute_time = %s"
    data_to_update = (mean_rho, max_speed_month, turbulence, turbulence_flag15,  algorithms_configs['farmName'], algorithms_configs['jobTime'])
    log.info(f'sql语句：{update_query}')
    log.info(f'sql数据：{data_to_update}')
    cursor.execute(update_query, data_to_update)
    conn.commit()
    cursor.close()

def insertPowerCurvePicture(algorithms_configs, url_path):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'power_curve_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_power_curve_picture_table_query}')
        cursor.execute(create_power_curve_picture_table_query)
    #插入数据
    log.info(f"#########################power_curve_picture表插入数据#########################")
    insert_query = "INSERT INTO power_curve_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertCPPicture(algorithms_configs, url_path):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'cp_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_cp_picture_table_query}')
        cursor.execute(create_cp_picture_table_query)
    #插入数据
    log.info(f"#########################cp_picture表插入数据#########################")
    insert_query = "INSERT INTO cp_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertAllZuobiaoPicture(algorithms_configs, url_path):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'zuobiao_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_zuobiao_picture_table_query}')
        cursor.execute(create_zuobiao_picture_table_query)
    #插入数据
    log.info(f"#########################zuobiao_picture表插入数据#########################")
    insert_query = "INSERT INTO zuobiao_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], 'all', file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertFaultPiePicture(algorithms_configs, url_path):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'fault_pie_picture';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_fault_pie_picture_table_query}')
        cursor.execute(create_fault_pie_picture_table_query)
    #插入数据
    log.info(f"#########################fault_pie_picture表插入数据#########################")
    insert_query = "INSERT INTO fault_pie_picture (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (algorithms_configs['jobTime'], algorithms_configs['farmName'], algorithms_configs['farmId'], algorithms_configs['typeName'], file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
def insertWord(farmInfo, url_path):
    urlList = urlparse(url_path).path.split('/')
    bucket_name = urlList[1]
    file_name = os.path.join(urlList[2],urlList[3])
    conn = get_connection()
    cursor = conn.cursor()
    #查询表名
    check_table_query = f"show tables like 'word';"
    #执行
    log.info(f'sql语句：{check_table_query}')
    cursor.execute(check_table_query)
    #获取结果
    result = cursor.fetchone()
    #判断表是否存在
    if not result:
        #新建表
        log.info(f'sql语句：{create_word_table_query}')
        cursor.execute(create_word_table_query)
    #插入数据
    log.info(f"#########################word表插入数据#########################")
    insert_query = "INSERT INTO word (execute_time, \
                        farm_name, \
                        farm_id, \
                        type_name, \
                        file_name, \
                        bucket_name, \
                        minio_url \
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = (farmInfo['execute_time'], farmInfo['farm_name'], farmInfo['farm_id'], 'all', file_name, bucket_name, url_path)
    log.info(f'sql语句：{insert_query}')
    log.info(f'sql数据：{data_to_insert}')
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
def get_connection_zero():
    conn = mysql.connector.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USERNAME,
        password=config.DB_PASSWORD,
        database=config.DB_DATABASE2,
        buffered=True
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