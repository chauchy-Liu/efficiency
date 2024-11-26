import re

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