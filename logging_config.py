# -*- coding: utf-8 -*-
"""
Created on Mon Sep 11 15:37:42 2023

@author: sunyan
"""
import logging
import os
from logging.handlers import TimedRotatingFileHandler


def get_all_algorithms():
    file_names = []
    for file_path in os.listdir('algorithms'):
        if os.path.isfile(os.path.join('algorithms', file_path)) and file_path.endswith('.py'):
            file_names.append(os.path.splitext(os.path.basename(file_path))[0])
    return file_names


def init_loggers():
    # 配置日志格式
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(process)d - %(threadName)s - %(message)s')
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # 动态创建文件处理器
    all_algorithms = get_all_algorithms()
    for name in all_algorithms:
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        if os.path.exists(os.path.dirname(os.path.join('logs', name+".log"))):
            pass
        else:
            os.makedirs(os.path.dirname(os.path.join('logs', name+".log")))
        file_handler = TimedRotatingFileHandler('logs/' + name + '.log', when='midnight', interval=1, backupCount=30)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.propagate = False
        
    # 配置根日志器
    root_logger = logging.getLogger()
    root_file_handler = TimedRotatingFileHandler('logs/root.log', when='midnight', interval=1, backupCount=30)
    root_file_handler.setFormatter(formatter)
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(root_file_handler)



if __name__ == '__main__':
    print(init_loggers())