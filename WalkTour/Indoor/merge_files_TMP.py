# -*- coding: utf-8 -*-
# 合并多次测试结果
import os
import shutil

import pandas as pd

from Common import get_path_sub_dir, get_all_csv_file, Common, get_data_path_by_char, print_with_line_number


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
    # data.to_csv(fr'D:\working\data_conv\out_path\{out_file}', index=False)
    data.to_csv(os.path.join(folder_path, out_file), index=False)


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


def get_merge_file_list(in_path, in_char, in_ot_char):
    tmp_csv_files = [os.path.join(in_path, file) for file in os.listdir(in_path) if
                     file.endswith('.csv') and in_char in file and in_ot_char in file]
    return tmp_csv_files


if __name__ == '__main__':
    # 处理当前路径下的文件
    folder_path = r'E:\work\MR_Data\1月15号\20240115数据\4G\反横'
    file_list = []
    res_list = get_data_path_by_char(folder_path, '5G')
    # print(res_list)
    for i_path in res_list:
        print_with_line_number(f'当前处理的路径：{i_path}', __file__)
        # res_list = Common.split_path_get_list(i_path)
        # print_with_line_number(f'路径list: {res_list}', __file__)
        res_file_list = Common.list_files_in_directory(i_path)
        file_list.extend(res_file_list)
        print_with_line_number(f'返回的文件列表：{res_file_list}', __file__)
        print('---' * 50)
    print_with_line_number(f'需要合并的文件列表为：{file_list}', __file__)
    # 把list中的文件全部合并
    merge_all_data(file_list)

