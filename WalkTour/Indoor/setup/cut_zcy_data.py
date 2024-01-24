# -*- coding: utf-8 -*-

import os

import pandas as pd

from Common import print_with_line_number, Common, read_csv_get_df, df_write_to_csv


def file_add_specified_suffix(in_file, *in_suffix):
    in_res_file_name, in_res_file_extension = os.path.splitext(in_file)
    # print('in_res_file_name: ', in_res_file_name)
    if f'_{in_suffix[0]}' not in os.path.basename(in_res_file_name):
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


def get_cur_dir_char_file(in_src_data):
    tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if
                     file.endswith('.csv') and 'char' in file]
    return tmp_csv_files


# 拷贝output目录下的文件
if __name__ == '__main__':
    # 获取当前目录下的char文件
    data_path = r'E:\work\MR_Data\1月15号\20240115数据_new_changge_clear\5G\正纵\3'
    res_char_file = get_cur_dir_char_file(data_path)

    res_char_file = [x for x in res_char_file if 'chart_clear' not in x]

    for i_f in res_char_file:
        res_f = file_add_specified_suffix(i_f, 'clear')
        if 'clear' not in os.path.basename(i_f):
            print_with_line_number(f'当前处理文件为: {i_f} ', __file__)
            char_df = read_csv_get_df(i_f)
            # between
            filtered_df = char_df.loc[char_df['x'].between(524, 620)]

            # 多条件
            # condition1 = (char_df['x'] > 560) & (char_df['y'] < 1200) & (char_df['y'] > 1020)
            # condition2 = (char_df['y'] > 620) & (char_df['y'] < 1020)
            # condition3 = (char_df['x'] < 630) & (char_df['y'] < 620)
            # # filtered_df = char_df.loc[condition1]
            # filtered_df = pd.concat([char_df.loc[condition1], char_df.loc[condition2], char_df.loc[condition3]])

            print_with_line_number(f'输出文件为: {res_f} ', __file__)
            df_write_to_csv(filtered_df, res_f)
