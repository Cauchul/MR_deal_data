# -*- coding: utf-8 -*-
import time
from datetime import datetime, timedelta

import pandas as pd

from Common import df_write_to_csv

data_file = r'D:\MrData\3月4号_new\下午\demo_out.csv'

# 读取包含 "UE Time" 列的 CSV 文件
df = pd.read_csv(data_file)


# 定义一个函数来将时间字符串转换为秒数的整数类型
def time_to_seconds(time_str):
    time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
    seconds = (time_obj.minute * 60 + time_obj.second) + (time_obj.microsecond / 1000000)
    return int(seconds)


def convert_datetime_to_seconds(dtime):
    return [int(time.mktime(time.strptime(x, '%y-%m-%d %H:%M:%S.%f'))) for x in dtime]


# 将 "UE Time" 列中的时间字符串转换为整数类型的秒数
df['Seconds'] = convert_datetime_to_seconds(df['UE Time'])

loop_value = df['Seconds'][0]
end_time = df['Seconds'].iloc[-1]

res_df = pd.DataFrame(columns=df.columns)

i_index = old_index = 0
row_data = df.iloc[0]

while True:
    if loop_value > end_time:
        break

    loop_value += 1
    i_index += 1
    print(f'loop_value: {loop_value}')
    if loop_value in df['Seconds'].values:
        print(f'{loop_value} 在数据中')
        old_index += 1

    print(type(row_data))
    res_df.loc[i_index] = df.loc[old_index]
    res_df.loc[i_index, 'UE Time'] = datetime.fromtimestamp(loop_value).strftime('%y-%m-%d %H:%M:%S.%f')
    print('---' * 50)

res_df = res_df.drop(columns='UE Time')

out_file = data_file.replace('.csv', '_res.csv')
df_write_to_csv(res_df, out_file)
