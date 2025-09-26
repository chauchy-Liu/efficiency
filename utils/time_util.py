import re
import pytz
from datetime import datetime

__timedelta_resample_unit_dict = {
    'D':'D',
    'd':'D',
    'h':'H',
    'm':'min',
    's':'S',
    'ms':'ms',
    'us':'U',
    'ns':'N',
    'T':'min',
    'Y':'Y',
    'Q':'Q',
    'M':'M'
}

__timedelta_numpy_unit_dict = {
    'D':'D',
    'h':'h',
    's':'s',
    'ms':'ms',
    'us':'us',
    'ns':'ns',
    'T':'m',
    'Y':'Y',
    'Q':'3M',
    'M':'M'
}

def split_time_delta(sample_interval):
    interval_value = re.match(r"(\d+)", sample_interval)
    if interval_value == None:
        interval_value = '1'
    else:
        interval_value = interval_value.group(1)
    interval_unit = sample_interval.strip(interval_value)
    interval_value = int(interval_value)
    return interval_value, interval_unit


def replace_to_resample(sample_interval):
    interval_value = re.match(r"(\d+)", sample_interval).group(1)
    interval_unit = sample_interval.strip(interval_value)
    return interval_value + __timedelta_resample_unit_dict[interval_unit]


def use_raw_api(sample_interval):
    interval_value, interval_unit = split_time_delta(sample_interval)
    if interval_unit == 's': # 秒级数据用原始数据api
        return True
    else:
        return False
    

def is_lower_than_day(sample_interval):
    interval_value = re.match(r"(\d+)", sample_interval).group(1)
    interval_unit = sample_interval.strip(interval_value)
    return True if (interval_unit == 'h' or interval_unit == 'm' or interval_unit == 's' or interval_unit == 'ms' or interval_unit == 'um' or interval_unit == 'ns') else False

#时间戳转本地时间
def timestamp_to_localtime(timestamp):
    #毫秒单位转秒单位
    timestamp /= 1000
    # 指定时区（例如，'Asia/Shanghai'为中国标准时间）
    timezone = pytz.timezone('Asia/Shanghai')

    # 转换为指定时区的datetime对象
    dt_object = datetime.fromtimestamp(timestamp, tz=timezone)

    # 格式化输出
    formatted_time = dt_object.strftime("%Y-%m-%d %H:%M:%S %Z")
    return formatted_time

def timestamp_to_datetime(timestamp):
    # 创建一个 UTC 时间
    utc_time = datetime.utcfromtimestamp(timestamp)
    
    # 指定目标时区（例如，'Asia/Shanghai' 对应中国标准时间）
    target_tz = pytz.timezone('Asia/Shanghai')
    
    # 将 UTC 时间转换为目标时区时间
    local_time = utc_time.replace(tzinfo=pytz.UTC).astimezone(target_tz)
    
    return local_time