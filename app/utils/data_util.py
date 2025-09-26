import warnings

from pandas import json_normalize
from pandas import DataFrame


def convert_df_to_dict(result: DataFrame) -> dict:
    warnings.warn("此方法已废弃，不推荐使用", DeprecationWarning)
    data_dict = {}
    for index, row in result.iterrows():
        data_dict[row['code']] = row.to_dict()
    return data_dict


def convert_df_to_list(result: DataFrame) -> list:
    data_list = []
    for index, row in result.iterrows():
        data_list.append(row.to_dict())
    return data_list


def convert_dict_to_df(param: dict) -> DataFrame:
    """
    {
        "A_B_C":
        {
            "C89": 1,
            "C90": 2,
            "C91": 3
        },
        "C_D_E":
        {
            "C89": 1,
            "C90": 2,
            "C91": 3
        }
    }
    """
    warnings.warn("此方法已废弃，不推荐使用", DeprecationWarning)
    data_list = []
    for key in param.keys():
        pn_dict = param[key]
        pn_dict["code"] = key
        data_list.append(pn_dict)
    return DataFrame(data=data_list)


def convert_list_to_df(param: list) -> DataFrame:
    """
    [{
        "code": "A_B_C",
        "time": "2022-01-01 11:00:00",
        "C89": 1,
        "C90": 2,
        "C91": 3
    },
    {
        "code": "C_D_E",
        "time": "2022-01-01 11:00:00"
        "C89": 1,
        "C90": 2,
        "C91": 3
    }]
    """
    df = json_normalize(param)
    df['time'] = df['time'].astype('datetime64')
    # df['time']= pd.to_datetime(df['time'])
    # df['time'] = pd.to_datetime(df['time'], format='%y%m%d')
    return df


if __name__ == '__main__':
    param = [{
        "code": "A_B_C",
        "time": "2022-01-01 11:00:00",
        "C89": 1,
        "C90": 2,
        "C91": 3
    },
    {
        "code": "C_D_E",
        "time": "2022-01-01 11:00:00",
        "C89": 1,
        "C90": 2,
        "C91": 3
    }]
    convert_list_to_df(param)