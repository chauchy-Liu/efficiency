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
from db.db import upload, insertWord, selectFarmInfo
# from configs.config import wspd, pwrat

def analyse(farmName, startTime, endTime):
    # 生成word报告
        
    # word_path_name = generate_word.write_word(word_path, algorithms_configs['farmName'], Df_all_m_all_alltype.index.min(), Df_all_m_all_alltype.index.max(), algorithms_configs['Turbine_attr_type_filted'], wind_freq, wind_freq, wind_max, wind_mean, month_data, wind_ti_alltype)
    farmInfo = selectFarmInfo(farmName, startTime, endTime)
    word_path_name = generate_word.write_word(farmInfo, startTime, endTime)
    #上传word
    url_word = upload(word_path_name, farmInfo)
    #mysql记录
    insertWord(farmInfo, url_word)
    
    return {"word_url": url_word}