# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pandas as pd

from Common import df_write_to_csv, print_with_line_number

# def convert_timestamp_to_datetime(timestamp):
#     return [datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S.%f') for x in timestamp]

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

loop_value = df['Seconds'][0]
end_time = df['Seconds'].iloc[-1]

res_df = pd.DataFrame(columns=df.columns)

i_index = 0
row_data = df.iloc[0]

while True:
    if loop_value > end_time:
        break

    loop_value += 1
    # row_data = df.loc[0]
    i_index += 1
    # print(i_index)
    print(f'loop_value: {loop_value}')
    if loop_value in df['Seconds'].values:
        print(f'{loop_value} 在数据中')
        # row_data = df.loc[df.loc[df['Seconds'] == loop_value].index]
        row_data = df.iloc[df.loc[df['Seconds'] == loop_value].index[0]]

        # row_data = df.loc[i_index, 'UE Time']
    print(type(row_data))
    res_df.loc[i_index] = row_data
    res_df.loc[i_index, 'UE Time'] = datetime.fromtimestamp(loop_value).strftime('%M:%S.%f')
    print('---' * 50)
    # res_df = res_df.concat(row_data, ignore_index=True)

res_df = res_df.drop(columns='Seconds')
df_write_to_csv(res_df, r'D:\MrData\3月4号_new\下午\demo_res.csv')
