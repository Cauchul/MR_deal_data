# -*- coding: utf-8 -*-

import os
import shutil

import pandas as pd

from Common import get_path_sub_dir, get_all_csv_file, Common


def clear_directory(directory_path):
    # 确保目录存在
    if os.path.exists(directory_path):
        # 遍历目录中的所有文件和子目录
        for item in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item)

            # 如果是文件，直接删除
            if os.path.isfile(item_path):
                os.remove(item_path)
            # 如果是目录，递归删除
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)


def merge_all_data(in_file_list):
    res_path_list = Common.split_path_get_list(in_file_list[0])
    print(res_path_list)
    out_file = f'merge_' + res_path_list[-1]
    print('out_file: ', out_file)
    # 合并数据
    data = pd.concat([pd.read_csv(file) for file in in_file_list])
    # 删除空行
    data = data.dropna(subset=['f_longitude', 'f_latitude'], how='any')
    data.to_csv(fr'D:\working\data_conv\out_path\{out_file}', index=False)


def merge_data(in_file_list, in_char):
    res_path_list = Common.split_path_get_list(in_file_list[0])
    print(res_path_list)
    out_file = f'merge_{in_char}_' + res_path_list[-1]
    print('out_file: ', out_file)
    # 合并数据
    data = pd.concat([pd.read_csv(file) for file in in_file_list])
    # 删除空行
    data = data.dropna(subset=['f_longitude', 'f_latitude'], how='any')
    data.to_csv(fr'D:\working\data_conv\out_path\{out_file}', index=False)


def get_merge_file_list(in_path, in_char):
    tmp_csv_files = [os.path.join(in_path, file) for file in os.listdir(in_path) if
                     file.endswith('.csv') and in_char in file]
    return tmp_csv_files


if __name__ == '__main__':
    folder_path = r'D:\working\1月3号_数据\2024-01-03中兴室内数据\定位\iqoo_7\4'
    res_dir_list = get_path_sub_dir(folder_path)
    file_list = []

    for i_p in res_dir_list:
        sub_path = os.path.join(folder_path, i_p)
        out_put_path = os.path.join(sub_path, 'output')
        res_list = get_merge_file_list(out_put_path, '4G')
        file_list.extend(res_list)
    # print(file_list)
    h_1_list = []
    h_2_list = []
    h_3_list = []
    for i_f in file_list:
        if '4横1' in i_f or '4纵1' in i_f:
            h_1_list.append(i_f)
        elif '4横2' in i_f or '4纵2' in i_f:
            h_2_list.append(i_f)
        elif '4横3' in i_f or '4纵3' in i_f:
            h_3_list.append(i_f)

    print(h_1_list)
    print(h_2_list)
    print(h_3_list)
    merge_data(h_1_list, '1')
    merge_data(h_2_list, '2')
    merge_data(h_3_list, '3')

    res_list = Common.list_files_in_directory(r'D:\working\data_conv\out_path')
    merge_all_data(res_list)

