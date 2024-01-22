# -*- coding: utf-8 -*-
# 拷贝文件到上一层目录
import fnmatch
import os

import pandas as pd

from Common import find_output_dir, get_file_list_by_char, print_with_line_number, copy_file, Common, FindFile, \
    read_csv_get_df, df_write_to_csv


def file_add_specified_suffix(in_file, *in_suffix):
    in_res_file_name, in_res_file_extension = os.path.splitext(in_file)
    # print('in_res_file_name: ', in_res_file_name)
    if f'_{in_suffix[0]}' not in in_res_file_name:
        suffix_str = "_".join(in_suffix)
        print_with_line_number(f'文件添加后缀为: {suffix_str}', __file__)
        in_tmp_file_name = in_res_file_name + f'_{suffix_str}' + in_res_file_extension
    else:
        in_tmp_file_name = in_res_file_name + in_res_file_extension
    return in_tmp_file_name


def file_rename(in_file):
    # copy_output_file_to_dir()
    res_list = Common.split_path_get_list(os.path.dirname(in_file))
    print_with_line_number(f'返回的文件路径list：{res_list}', __file__)
    res_new_name = file_add_specified_suffix(in_file, res_list[-3], res_list[-2])
    print_with_line_number(f'原文件名：{in_file}', __file__)
    print_with_line_number(f'拷贝文件名：{res_new_name}', __file__)
    os.rename(in_file, res_new_name)
    return res_new_name


def copy_output_file_to_dir(in_char='4G'):
    # 找到output目录
    res_output_path_list = find_output_dir(data_path)

    for i_output_path in res_output_path_list:
        print_with_line_number(f'当前查找文件的标志为：{in_char}', __file__)
        print_with_line_number(f'output路径：{i_output_path}', __file__)
        # 找到目录下的所有的包含4G 字段的文件
        in_res_list = get_file_list_by_char(i_output_path, in_char)
        if not in_res_list:
            print_with_line_number(f'output路径：{i_output_path} 没有包含：{in_char} 的文件', __file__)
            print_with_line_number(f'output路径：{i_output_path} 的文件列表为: {in_res_list}', __file__)
        for i_f in in_res_list:
            print('++' * 50)
            print_with_line_number(f'当前处理文件: {i_f}', __file__)
            # 文件重命名
            i_f = file_rename(i_f)
            # 根据新文件名拷贝文件
            copy_file(i_f, output_path)
            print_with_line_number(f'拷贝文件: {i_f} 输出目录：{output_path}', __file__)
        print('---' * 50)


def copy_merge_file_to_dir(in_src_path):
    in_res_list = FindFile.find_files_with_string(in_src_path, 'merge')
    for i_f in in_res_list:
        # print('++' * 50)
        print_with_line_number(f'当前处理文件: {i_f}', __file__)
        # 文件重命名
        i_f = file_rename(i_f)
        # 根据新文件名拷贝文件
        copy_file(i_f, output_path)
        print_with_line_number(f'拷贝文件: {i_f} 输出目录：{output_path}', __file__)


# 获取所有output下的所有文件
def get_output_dir_csv(in_src_data):
    tmp_res_list = []
    in_output_dir_list = find_output_dir(in_src_data)
    for i_dir in in_output_dir_list:
        in_res_list = Common.list_files_in_directory(i_dir)
        tmp_res_list.extend(in_res_list)

    # 只获取finger文件
    tmp_res_list = [x for x in tmp_res_list if 'finger' in x]
    return tmp_res_list


# 获取整个目录下zip文件存在的所有的子目录路径，也即数据路径
def get_char_file_path(in_dir, in_char='zip'):
    tmp_data_path_list = []
    tmp_file_list = []
    for root, dirs, files in os.walk(in_dir):
        # if root != in_dir:
        for file in files:
            if in_char in file:
                tmp_data_path_list.append(root)
                file_path = os.path.join(root, file)
                tmp_file_list.append(file_path)
                # print('file_path: ', file_path)

    return tmp_data_path_list, tmp_file_list


def get_cur_dir_char_file(in_src_data):
    tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if
                     file.endswith('.csv') and 'char' in file]
    return tmp_csv_files


# 拷贝output目录下的文件
if __name__ == '__main__':
    # data_path = r'E:\work\MR_Data\1月18号\20240118_源数据\室内'
    # output_path = r'E:\work\MR_Data\clean\data'
    #
    # # 获取char文件路径
    # res_list, res_file_list = get_char_file_path(data_path, 'char')
    # res_file_list = [x for x in res_file_list if 'unzip' not in x]
    # # print('res_list: ', res_list)
    #
    # for i_f in res_file_list:
    #     print(i_f)
    #     copy_file(i_f, output_path)

    # 获取当前目录下的char文件
    data_path = r'E:\work\MR_Data\1月18号\20240118_源数据\室内\6\5G'
    res_char_file = get_cur_dir_char_file(data_path)

    # print(res_char_file)
    for i_f in res_char_file:
        res_f = file_add_specified_suffix(i_f, 'clear')

        if 'clear' not in i_f:
            print(i_f)
            char_df = read_csv_get_df(i_f)
            filtered_df = char_df.loc[char_df['x'].between(489, 608)]
            # filtered_df = char_df.loc[(char_df['y'] < 300) & (char_df['x'] < 921) | char_df['x'].between(300, 749)]

            # filtered_df = char_df.loc[(char_df['y'] < 950) & (char_df['x'] > 837) | char_df['y'].between(300, 765)]
            # filtered_df = char_df.loc[(char_df['x'] > 827) & (char_df['x'] < 957)]

            # 多条件
            # condition1 = (char_df['x'] > 868) & (char_df['y'] > 766)
            # condition2 = (char_df['y'] > 300) & (char_df['y'] < 766)
            # condition3 = (char_df['y'] < 300) & (char_df['x'] < 913)
            #
            # # 合并切分后的数据
            # filtered_df = pd.concat([char_df.loc[condition1], char_df.loc[condition2], char_df.loc[condition3]])

            df_write_to_csv(filtered_df, res_f)
