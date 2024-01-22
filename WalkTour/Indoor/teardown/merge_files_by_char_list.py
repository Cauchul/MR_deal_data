# -*- coding: utf-8 -*-
# 合并多次测试结果
import os

import pandas as pd

from Common import Common, FindFile, find_output_dir, print_with_line_number


def merge_all_data(in_file_list, in_char=''):
    res_path_list = Common.split_path_get_list(in_file_list[0])
    print(res_path_list)
    if in_char:
        out_file = f'merge_{in_char}_' + res_path_list[-1]
    else:
        out_file = f'merge_' + res_path_list[-1]
    print_with_line_number(f'out_file: {out_file}', __file__)
    # 合并数据
    data = pd.concat([pd.read_csv(file) for file in in_file_list])
    # 删除空行
    data = data.dropna(subset=['f_longitude', 'f_latitude'], how='any')
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


def get_output_dir_csv(in_src_data):
    tmp_res_list = []
    in_output_dir_list = find_output_dir(in_src_data)
    for i_dir in in_output_dir_list:
        in_res_list = Common.list_files_in_directory(i_dir)
        tmp_res_list.extend(in_res_list)

    # 只获取finger文件
    tmp_res_list = [x for x in tmp_res_list if 'finger' in x]
    return tmp_res_list


def filter_file_list(in_file_list, in_char):
    tmp_list = [i_f for i_f in in_file_list if in_char in i_f]
    return tmp_list


def filter_file_list_by_char_list(in_file_list, in_char_list):
    in_res_list = []
    for in_i_char in in_char_list:
        in_tmp_list = [file for file in in_file_list if in_i_char in os.path.basename(file)]
        in_res_list.extend(in_tmp_list)

    # 去重
    in_res_list = list(set(in_res_list))
    return in_res_list


if __name__ == '__main__':
    folder_path = r'E:\work\MR_Data\merge\123_134'
    # 获取当前路径下的所有csv文件
    res_file_list = FindFile.get_cur_dir_all_csv(folder_path)
    # 获取output目录
    # res_file_list = get_output_dir_csv(folder_path)
    # 获取sub目录
    # res_dir_list = get_path_sub_dir(folder_path)
    # res_file_list = []
    #
    # for i_p in res_dir_list:
    #     sub_path = os.path.join(folder_path, i_p)
    #     out_put_path = os.path.join(sub_path, 'output')
    #     # 获取需要合并的所有的文件list
    #     res_list = get_merge_file_list(out_put_path, '4G', 'finger')
    #     res_file_list.extend(res_list)

    # res_file_list = FindFile.find_files_with_string(folder_path, 'set_sid')
    # print(res_file_list)
    # 根据特征list获取指定文件
    char_list = ['_1_', '_4_', '_3_']
    res_list = filter_file_list_by_char_list(res_file_list, char_list)

    # 去除merge
    res_list = [i_f for i_f in res_list if 'merge' not in os.path.basename(i_f)]
    # print('res_list: ', res_list)
    for i_f in res_list:
        print('char_file: ', i_f)
        # if 'merge' in os.path.basename(i_f):
        #     res_list.remove(i_f)

    char_string = '_'.join([item.strip('_') for item in char_list])
    print('char_string:', char_string)

    # 获取 45G 文件
    res_4G_file_list = filter_file_list(res_list, '4G')
    res_5G_file_list = filter_file_list(res_list, '5G')

    if res_4G_file_list:
        # print_with_line_number(f'4G 文件 list：{res_4G_file_list}', __file__)
        for i_f in res_4G_file_list:
            if 'merge' in os.path.basename(i_f):
                # res_4G_file_list.remove(i_f)
                continue
            print_with_line_number(f'4G 文件：{i_f}', __file__)
        print_with_line_number(f'开始 4G 指纹文件合并', __file__)
        merge_all_data(res_4G_file_list, char_string)
    print('---' * 50)

    # 把list中的文件全部合并
    if res_5G_file_list:
        for i_f in res_5G_file_list:
            # if 'merge' in os.path.basename(i_f):
            #     res_5G_file_list.remove(i_f)
            #     continue
            print_with_line_number(f'5G 文件：{i_f}', __file__)
        # print_with_line_number(f'5G 文件 list：{res_5G_file_list}', __file__)
        print_with_line_number(f'开始 5G 指纹文件合并', __file__)
        merge_all_data(res_5G_file_list, char_string)
    print('---' * 50)
