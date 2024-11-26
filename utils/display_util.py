import numpy as np
import pickle
import zlib
import utils.time_util as time_util
import configs.config as config
import os
import glob


def get_file_creation_date(filename):
    # os.path.getmtime(filename)#修改时间
    return os.path.getctime(filename) #创建时间
 
def sort_files_by_date(directory, reverse=True): #默认降序
    files = os.listdir(directory)
    files = [os.path.join(directory, file) for file in files]
    files.sort(key=get_file_creation_date, reverse=reverse)
    return files

def DisplayFigures(xUnit:str, yUnit:str, time, multiDimensionDataxy:list):
    # interval_value, interval_unit = time_util.split_time_delta(resample_interval)
    # result = {
    #     'startTime': startTime,
    #     'endTime': endTime,
    #     'timeInterval': interval_unit.lower(),
    #     'timeIntervalValue': interval_value,
    #     'measurement': measurement,
    #     'multiDimensionData': multiDimensionData,
    #     'type':0 #时间类型
    # }
    result = {
        "ordinateUnit": yUnit,
        "abscissaUnit": xUnit,
        "time": time, #x轴为时间设1，否则设0
        "multiDimensionDataxy": multiDimensionDataxy
    }

    return result

def DisplayResultXY(lineType:str, name:str, color:str, line:str, xyData:list):
    result = {
        # 'customStart': startX,
        # 'customEnd': endX,
        # 'customInterval': deltaX,
        # 'measurement': measurement,
        # 'multiDimensionDataxy': multiDimensionDataXY,
        # 'type':1 #自定义类型,
        "type": lineType, #0：线， 1:散点
        "color": color, #FF0000 红，#FFFF00 黄，#800080 紫，#0000FF 蓝，#00FFFF 青，#00FF00 绿，#FF00FF 粉，#888888 灰
        "linetype": line, #lineType为0时：'Solid', 'Dash'；lineType为1时：''
        "name": name,
        "xyData": xyData

    }

    return result

def StoreResult(result:dict, algorithmName:str, turbineName:str, detailTableId:str):

    # 使用pickle序列化字典
    serialized_data = pickle.dumps(result)
    
    # 使用zlib压缩序列化后的数据
    compressed_data = zlib.compress(serialized_data)
    
    # 将压缩的数据写入文件
    path = config.Path
    if path == '':
        # 使用glob模块找到所有匹配的文件
        for filename in glob.glob('./*.pklz'):
            os.remove(filename)
        with open(path+str(detailTableId)+".pklz", 'wb') as f:
            f.write(compressed_data)
    else:
        fileName = os.path.join(path,algorithmName,turbineName, str(detailTableId))
        currentDir = os.path.dirname(fileName+".pklz")
        if os.path.exists(currentDir):
            #对当前目录下的文件按创建日期降序排序
            files = sort_files_by_date(currentDir)
            #维持10个文件数量
            if len(files) >= 10:
                for file in files[9:]:
                    os.remove(file)
            with open(fileName+".pklz", 'wb') as f:
                f.write(compressed_data)
        else:
            os.makedirs(currentDir)
            with open(fileName+".pklz", 'wb') as f:
                f.write(compressed_data)

    return fileName+".pklz"

def ReadFile(filename:str): 
    # 从文件读取压缩的数据
    with open(filename, 'rb') as f:
        compressed_data = f.read()
    
    # 解压缩数据
    decompressed_data = zlib.decompress(compressed_data)
    
    # 反序列化数据
    loaded_data = pickle.loads(decompressed_data)

    return loaded_data