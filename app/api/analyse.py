import asyncio
import logging
#  from app.api.test import atest
from main_job import execute_multi_algorithms
import traceback

def atest(code):
    modelCode = code#'all_model'#code#response.data.decode('utf-8') #request.args.get('modelCode')
    print("modelCode: "+str(modelCode))
    if modelCode == 'blade_angle_not_balance':
        asyncio.run(execute_multi_algorithms(['blade_angle_not_balance']))
    elif modelCode == 'blade_freeze':
        asyncio.run(execute_multi_algorithms(['blade_freeze']))
    elif modelCode == 'capacity_reduction':
        asyncio.run(execute_multi_algorithms(['capacity_reduction']))
    elif modelCode == 'chilunxiang_disu_zhoucheng_temperature':
        asyncio.run(execute_multi_algorithms(['chilunxiang_disu_zhoucheng_temperature']))
    elif modelCode == 'chilunxiang_gaosu_zhoucheng_temperature':
        asyncio.run(execute_multi_algorithms(['chilunxiang_gaosu_zhoucheng_temperature']))
    elif modelCode == 'chilunxiang_sanre':
        asyncio.run(execute_multi_algorithms(['chilunxiang_sanre']))
    elif modelCode == 'engine_cabinet_temperature':
        asyncio.run(execute_multi_algorithms(['engine_cabinet_temperature']))
    elif modelCode == 'engine_env_temperature':
        asyncio.run(execute_multi_algorithms(['engine_env_temperature']))
    elif modelCode == 'generator_houzhoucheng_temperature':
        asyncio.run(execute_multi_algorithms(['generator_houzhoucheng_temperature']))
    elif modelCode == 'generator_qianzhoucheng_temperature':
        asyncio.run(execute_multi_algorithms(['generator_qianzhoucheng_temperature']))
    elif modelCode == 'generator_raozu_not_balance':
        asyncio.run(execute_multi_algorithms(['generator_raozu_not_balance']))
    elif modelCode == 'generator_temperature':
        asyncio.run(execute_multi_algorithms(['generator_temperature']))
    elif modelCode == 'generator_zhuanju_kongzhi':
        asyncio.run(execute_multi_algorithms(['generator_zhuanju_kongzhi']))
    elif modelCode == 'generator_zhuzhou_rpm_not_balance':
        asyncio.run(execute_multi_algorithms(['generator_zhuzhou_rpm_not_balance']))
    elif modelCode == 'oar_electric_capacity_temperature':
        asyncio.run(execute_multi_algorithms(['oar_electric_capacity_temperature']))
    elif modelCode == 'oar_engine_performance':
        asyncio.run(execute_multi_algorithms(['oar_engine_performance']))
    elif modelCode == 'oar_engine_temperature':
        asyncio.run(execute_multi_algorithms(['oar_engine_temperature']))
    elif modelCode == 'oar_machine_temperature':
        asyncio.run(execute_multi_algorithms(['oar_machine_temperature']))
    elif modelCode == 'pianhang_duifeng_buzheng':
        asyncio.run(execute_multi_algorithms(['pianhang_duifeng_buzheng']))
    elif modelCode == 'weathercock_freeze':
        asyncio.run(execute_multi_algorithms(['weathercock_freeze']))
    elif modelCode == 'wind_speed_fault':
        asyncio.run(execute_multi_algorithms(['wind_speed_fault']))
    elif modelCode == 'all_model':
        asyncio.run(execute_multi_algorithms(['blade_angle_not_balance', 'blade_freeze', 'capacity_reduction', 'chilunxiang_disu_zhoucheng_temperature', 'chilunxiang_gaosu_zhoucheng_temperature', 'chilunxiang_sanre', 'engine_cabinet_temperature', 'engine_env_temperature', 'generator_houzhoucheng_temperature', 'generator_qianzhoucheng_temperature', 'generator_raozu_not_balance', 'generator_temperature', 'generator_zhuanju_kongzhi', 'generator_zhuzhou_rpm_not_balance', 'oar_electric_capacity_temperature', 'oar_engine_performance', 'oar_engine_temperature', 'oar_machine_temperature', 'pianhang_duifeng_buzheng', 'weathercock_freeze', 'wind_speed_fault']))#asyncio.run()

def analyseData(code):
    
    try:
        from main_job import execute_multi_algorithms
        modelCode = code#'all_model'#code#response.data.decode('utf-8') #request.args.get('modelCode')
        print("modelCode: "+str(modelCode))

        if len(modelCode) > 1:
            asyncio.run(execute_multi_algorithms(modelCode))
        elif len(modelCode) == 1:
            if modelCode[-1] == 'all_model':
                asyncio.run(execute_multi_algorithms(['record_loss_indicator', 'record_pwrt_picture']))
            else:
                asyncio.run(execute_multi_algorithms(modelCode))
    except Exception as e:
        # logging.getLogger().error(f'接口执行报错，错误信息：{str(e)}')
        errorInfomation = traceback.format_exc()
        logging.getLogger().error(f'\033[31m{errorInfomation}\033[0m')
        logging.getLogger().error(f'\033[33m接口执行报错，错误信息：{e}\033[0m')