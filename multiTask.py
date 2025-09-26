# import multiprocessing
import pandas as pd
import multiprocessing as mp
from functools import partial

def process_dataframe(df, result):
    # 创建一个结果的副本，以避免并发写入问题
    local_result = result.copy()
    
    # 使用 pandas 的向量化操作来更新非空值
    mask = df.notna()
    local_result.update(df.where(mask))
    
    return local_result

def parallel_fill_data(df_list, result, num_processes=None):
    if num_processes is None:
        num_processes = mp.cpu_count()

    # 创建一个进程池
    with mp.Pool(processes=num_processes) as pool:
        # 使用偏函数来固定 result 参数
        process_func = partial(process_dataframe, result=result)
        
        # 使用 map 来并行处理每个数据框
        results = pool.map_async(process_func, df_list)
        pool.close()
        pool.join()

    # 合并所有结果
    for res in results.get():
        result.update(res)

    return result

# 使用示例


# def process_task(number):
#     result = number * 2
#     print(f"处理数字 {number}，结果为 {result}")