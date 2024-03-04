# -*- coding: utf-8 -*-
import os

import pandas as pd

from Common import read_csv_get_df

data_file = r'D:\MrData\2月27号\20240227\4G\WalkTour\IQOO7\1\iQOO7-4G--OUT20240227-145526-Ping(1)_0228100953.csv'


def check_blank_line(in_csv_file):
    res_columns = pd.read_csv(in_csv_file, nrows=0).columns

    # 获取两列数据的空值情况
    if 'Longitude' in res_columns and 'Latitude' in res_columns:
        in_res_df = pd.read_csv(in_csv_file, usecols=['Longitude', 'Latitude'])
        threshold = len(in_res_df) / 3
        empty_rows_count = in_res_df[(in_res_df['Longitude'].isnull()) & (in_res_df['Latitude'].isnull())].shape[0]
    else:
        in_res_df = pd.read_csv(in_csv_file, usecols=['startlocation_longitude', 'startlocation_latitude'])
        threshold = len(in_res_df) / 3
        empty_rows_count = in_res_df[
            (in_res_df['startlocation_longitude'].isnull()) & (in_res_df['startlocation_latitude'].isnull())].shape[0]

    if empty_rows_count > threshold:
        raise ValueError(f"{in_csv_file} 文件空行数超过三分之一，数据异常！")


# 找到路径下的所有csv文件

def get_all_csv_files(directory):
    csv_files = []
    for root, dirs, files in os.walk(directory):
        if "output" in dirs:
            dirs.remove("output")  # 排除名为 "output" 的子目录
        for file in files:
            if file.endswith(".csv"):
                # csv_files.append(os.path.join(root, file))
                csv_files.append(root)
    return csv_files


# target_directory = r"D:\MrData\2月27号\20240227"
#
# # 获取目标目录下以及子目录下的所有 CSV 文件路径
# csv_files_list = get_all_csv_files(target_directory)
#
# # print(csv_files_list)
#
# for i in csv_files_list:
#     print(i)
# 定义一个函数，用于计算目录下以及子目录下的所有 CSV 文件的个数（排除名为 "output" 的子目录）
def count_csv_files_exclude_output(directory):
    csv_count = 0
    for root, dirs, files in os.walk(directory):
        if "output" in dirs:
            dirs.remove("output")  # 排除名为 "output" 的子目录
        for file in files:
            if file.endswith(".csv"):
                csv_count += 1
    return csv_count


if __name__ == '__main__':
    # 指定目标目录
    target_directory = r"D:\MrData"

    # 获取目标目录下以及子目录下的所有 CSV 文件个数（排除名为 "output" 的子目录）
    total_csv_count = count_csv_files_exclude_output(target_directory)

    print("目录及其子目录下的所有 CSV 文件个数（排除名为 'output' 的子目录）:", total_csv_count)
