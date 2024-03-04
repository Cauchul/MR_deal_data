# -*- coding: utf-8 -*-
import os

import pandas as pd

from Common import find_output_dir, read_csv_get_df, print_with_line_number


def check_empty(in_dir):
    tmp_dir_list = []
    res_output_dir_list = find_output_dir(in_dir)

    for i_output_dir in res_output_dir_list:
        dir_contents = os.listdir(i_output_dir)

        if not dir_contents:
            print(f"{i_output_dir} 目录为空")
            tmp_dir_list.append(os.path.dirname(i_output_dir))

    return tmp_dir_list


def check_netwalk_type(in_data_file):
    # print('检查网络类型')
    in_file_name = os.path.basename(in_data_file)
    if '5G' in in_file_name:
        in_f_net_type = 'NR'
    elif '4G' in in_file_name:
        in_f_net_type = 'LTE'
    else:
        in_f_net_type = ''

    # 读取文件中的network type；指读取前五行的数据
    in_res_df = pd.read_csv(in_data_file, nrows=5)
    if 'Network Type' not in in_res_df.columns:
        if 'startlocation_longitude' in in_res_df.columns:
            # print_with_line_number('WeTest 文件，不检测网络类型', __file__)
            return
        else:
            print_with_line_number(f'文件 {in_data_file} 中没有 Network Type 字段', __file__)
            return
    in_net_type = in_res_df['Network Type'][0]

    if in_f_net_type.lower() != in_net_type.lower():
        print_with_line_number(f'Error 网络类型异常： {in_data_file}  期望 Network Type：{in_f_net_type}, 实际 Network Type：{in_net_type}', __file__)
    else:
        print_with_line_number('检测网络类型正常', __file__)


def check_blank_line(in_csv_file):
    # print('检查空行')
    # in_res_df = read_csv_get_df(in_csv_file)
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
        print_with_line_number(f"文件 GPS 空行数异常：{in_csv_file} 文件空行数超过三分之一，数据异常！", __file__)
    else:
        print_with_line_number('检测文件 GPS 空行数正常', __file__)


# 获取除output目录外的所有csv文件
def get_all_csv_files(directory):
    csv_files = []
    for root, dirs, i_files in os.walk(directory):
        if "output" in dirs:
            dirs.remove("output")  # 排除名为 "output" 的子目录
        for file in i_files:
            if file.endswith(".csv"):
                # csv_files.append(os.path.join(root, file))
                csv_files.append(root)
    return list(set(csv_files))


# 获取当前目录下的所有文件
def get_cur_dir_all_csv(in_src_data):
    tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if file.endswith('.csv')]
    return tmp_csv_files


if __name__ == '__main__':
    src_data = r'D:\MrData'
    # 获取所有的空的output路径
    # res_list = check_empty(src_data)
    # 获取所有output路径
    res_list = find_output_dir(src_data)
    # 获取所有csv文件的目录
    # res_list = get_all_csv_files(src_data)

    # 获取当前文件夹下的所有文件
    # get_cur_dir_all_csv(src_data)

    for i_dir in res_list:
        # 获取目录下的csv文件
        files = os.listdir(i_dir)
        # print(f'当前检查目录：{i_dir}')
        for i_file in files:
            if i_file.endswith('.csv'):
                check_file = os.path.join(i_dir, i_file)
                # 检查网络的格式是否一致
                print_with_line_number(f'当前检查文件： {check_file}', __file__)
                # 检查网络类型
                check_netwalk_type(check_file)
                # 检查空行数量
                check_blank_line(check_file)
                print_with_line_number('--' * 50, __file__)
