# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd

from Common import df_write_to_csv


# def convert_timestamp_to_datetime(timestamp):
#     return [datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S.%f') for x in timestamp]
def convert_timestamp_to_datetime(timestamp):
    return [datetime.fromtimestamp(x).strftime('%M:%S.%f') for x in timestamp]


data_file = r'D:\MrData\3月4号_new\下午\demo_out.csv'

# 读取包含 "UE Time" 列的 CSV 文件
df = pd.read_csv(data_file)


# 定义一个函数来将时间字符串转换为秒数的整数类型
def time_to_seconds(time_str):
    time_obj = datetime.strptime(time_str, "%M:%S.%f")
    seconds = (time_obj.minute * 60 + time_obj.second) + (time_obj.microsecond / 1000000)
    return int(seconds)


# 将 "UE Time" 列中的时间字符串转换为整数类型的秒数
df['Seconds'] = df['UE Time'].apply(time_to_seconds)

# 打印转换后的秒数列
# print(df['Seconds'])

start_value = df['Seconds'][0]
end_time = df['Seconds'].iloc[-1]

# print(start_value)

index = 0

while True:
    if start_value > end_time:
        break

    start_value += 1

    row_data = df.loc[0]
    # print(value)
    if start_value in df['Seconds'].values:
        row_data = df.loc[index]
    else:
        print('添加数据 index: ', index)
        # df.loc[index] = row_data
        top_half = df.iloc[:index]
        bottom_half = df.iloc[index:]
        if top_half and bottom_half:
            df = pd.concat([top_half, pd.DataFrame(row_data, index=[0]), bottom_half]).reset_index(drop=True)
        elif top_half:
            # bottom_half = df.iloc[index:]
            df = pd.concat([top_half, pd.DataFrame(row_data, index=[0])]).reset_index(drop=True)
        else:
            df = pd.concat([pd.DataFrame(row_data, index=[0]), bottom_half]).reset_index(drop=True)
    index += 1

df_write_to_csv(df, r'D:\MrData\3月4号\20240304\demo_res.csv')

# save_row = 0

# for index, row in df.iterrows():


#     save_row = row
#     p_value = df.iloc[index, 'Seconds']
#     p_value += 1
#
#     if p_value not in df['Seconds'].values:
#     print(f"Index: {index}, UE Time: {cur_sec}")
