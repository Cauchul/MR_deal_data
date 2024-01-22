# -*- coding: utf-8 -*-
import math

import numpy as np
import pandas as pd
from geopy.distance import geodesic

from Common import df_write_to_csv, read_csv_get_df


def calculate_distance(in_xy_list1, in_xy_list2):
    # print('in_xy_list1: ', type(in_xy_list1))
    # print('in_xy_list2: ', type(in_xy_list2[1]))
    in_res_distance = np.linalg.norm(in_xy_list1 - in_xy_list2)
    # in_res_distance = math.sqrt(pow(in_xy_list2[0] - in_xy_list1[0], 2) + pow(in_xy_list2[1] - in_xy_list1[1], 2))
    # print(in_res_distance)
    return in_res_distance


data_file = r'E:\work\MR_Data\demo\4G_HaiDian_outdoor_WT_LOG_DT_UE_0118_finger_1_4G.csv'
res_df = read_csv_get_df(data_file)

# 将 'f_time' 列转换为 Pandas 的时间戳格式
# df['f_time'] = pd.to_datetime(df['f_time'], unit='s')

# 按 'f_time' 分组，并获取每组的最后一行数据
result = res_df.groupby('f_time').tail(1)

result = result[['f_latitude', 'f_longitude']]

# 设置有效位数
# result['f_longitude'] = result['f_longitude'].round(12)
# result['f_latitude'] = result['f_latitude'].round(12)

res_distance_list = []

for i in range(len(result.values) - 1):
    # result = result.values[i] + result.values[i + 1]
    print('1: ', result.values[i])
    print('2: ', result.values[i + 1])

    distance = geodesic(result.values[i], result.values[i + 1]).meters  # 距离的单位为米

    print('distance: ', distance)

    res_dis = calculate_distance(result.values[i], result.values[i + 1])
    # print(res_dis)
    res_distance_list.append(distance)

    # break

# print('res_distance_list: ', res_distance_list)

# 计算最大值
max_value = max(res_distance_list)

# 计算均值
mean_value = sum(res_distance_list) / len(res_distance_list)

print("最大值:", max_value)
print("均值:", mean_value)

# # 遍历 'f_longitude' 列数据
# for index, row in result.iterrows():
#     longitude_value = row['f_longitude']
#     print(f"Row {index}, Longitude: {longitude_value}")
#
# print(result)
# df_write_to_csv(result, r'E:\work\MR_Data\demo\demo.csv')
