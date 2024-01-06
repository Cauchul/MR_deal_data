# -*- coding: utf-8 -*-

import os
import shutil

import pandas as pd


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


def merge_data(in_file_list):
    # print('in_file_list: ', in_file_list)
    out_file = in_file_list[1].split('.')[0]
    if 'merge' not in out_file:
        out_file = out_file + '_merge.csv'
    else:
        out_file = out_file + '.csv'
    print('out_file: ', out_file)
    # 合并数据
    data = pd.concat([pd.read_csv(os.path.join(folder_path, file)) for file in in_file_list])
    # 删除空行
    data = data.dropna(subset=['f_longitude', 'f_latitude'], how='any')
    data.to_csv(fr'DD:\working\data_conv\out_path\{out_file}', index=False)

    # 清理merge_tmp目录
    # clear_directory(folder_path)


def get_merge_file_list(in_path, in_char):
    tmp_csv_files = [file for file in os.listdir(in_path) if file.endswith('.csv') and in_char in file]
    return tmp_csv_files


if __name__ == '__main__':
    # 获取需要合并的文件list
    folder_path = r'D:\working\data_conv\src_data'
    res_list = get_merge_file_list(folder_path, '5G')
    # 合并数据
    merge_data(res_list)
