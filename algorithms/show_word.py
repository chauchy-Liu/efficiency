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
from db.db import selectFaultgridLossAll
from matplotlib import pyplot as plt
from pylab import mpl
import sys
import statistics as st
from scipy import signal,integrate
from datetime import datetime
import os
import data.generate_word as generate_word
from db.db import upload, insertWord, selectFarmInfo, updateWord
import traceback
import logging
from data.get_data_async import getToken, javaUploadWord
# from configs.config import wspd, pwrat

logger = logging.getLogger('http-word')
if not logger.handlers:
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(process)d - %(threadName)s - %(message)s')
    # console_handler = logging.StreamHandler()
    # console_handler.setFormatter(formatter)
    # alarm_file_handler = TimedRotatingFileHandler('logs/alarm.log', when='midnight', interval=1, backupCount=30)
    data_file_handler = logging.handlers.RotatingFileHandler(filename=os.path.join("logs","http-word"+".log"), mode='a', maxBytes=5*1024**2, backupCount=3)
    data_file_handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    # data_logger.addHandler(console_handler)
    logger.addHandler(data_file_handler)

def analyse(farmName, startTime, endTime, stateType):
    logger.info(f"\n\n##########show_word请求: ############################################3\n")
    try:
        # 生成word报告
        # word_path_name = generate_word.write_word(word_path, algorithms_configs['farmName'], Df_all_m_all_alltype.index.min(), Df_all_m_all_alltype.index.max(), algorithms_configs['Turbine_attr_type_filted'], wind_freq, wind_freq, wind_max, wind_mean, month_data, wind_ti_alltype)
        farmInfo = selectFarmInfo(farmName, startTime, endTime)
        #先生成一条空的word记录，状态为生成中
        execute_time = insertWord(farmInfo, "", startTime, endTime)
        word_path_name = generate_word.write_word(farmInfo, startTime, endTime, execute_time)
        #word上传minio 
        logger.info(f"##########word_path_name: {word_path_name}#######3")
        if stateType == 0:
            url_word = upload(word_path_name, farmInfo)
        else:
            url_word = word_path_name
        #mysql记录
        updateWord(execute_time, url_word, word_process=1)
        if stateType == 0:
            return {"word_url": url_word}
        else:
            #调取后端接口通知word生成完毕
            #请求Java上传word
            jobTimeStr = datetime.strftime(execute_time, '%Y-%m-%d %H:%M:%S')
            token = getToken()
            logger.info(f"##########token: {token}#######3")
            javaUploadWord(jobTimeStr, token)
        logger.info(f"\n##########word生成成功: #######3\n")
    except Exception as e:
        errorInfomation = traceback.format_exc()
        logger.info(f'\033[31m{errorInfomation}\033[0m')
        logger.info(f'\033[33m指标报错：{e}\033[0m')
        updateWord(execute_time, "", word_process=-1)
        logger.info(f"\n##########word生成失败: #######3\n")