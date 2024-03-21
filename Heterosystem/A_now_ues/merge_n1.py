# -*- coding: utf-8 -*-
import os.path

import pandas as pd

from Common import read_csv_get_df, df_write_to_csv, find_output_dir, Common

# # 读取df数据
# finger_file = r'E:\work\MrData\data_place\out\上午\1\5G_HaiDian_indoor_WT_LOG_DT_UE_0304_finger_20240304_1.csv'
# table_file = os.path.join(os.path.dirname(finger_file), 'demo_out_res.csv')
#
# print(f'异系统数据文件： {table_file}')
#
# finger_df = read_csv_get_df(finger_file)
# table_df = read_csv_get_df(table_file)
#
# tmp_merger_df = pd.merge(finger_df, table_df, left_on="f_time", right_on="f_time", how='left')
# # tmp_merger_df = pd.merge(finger_df, table_df, how='left')
#
# out_file = finger_file.replace('.csv', '_hetero_sys.csv')
# # tmp_merger_df = tmp_merger_df.drop(columns='Seconds')
# df_write_to_csv(tmp_merger_df, out_file)

import os

from WalkTour.new_deal.delete_empty_step_1_n1 import get_all_sub_dir, get_cur_dir_all_csv


def get_file(in_find_dir, in_find_char):
    file_names = os.listdir(in_find_dir)

    for file_name in file_names:
        if file_name.endswith('.csv') and in_find_char in file_name:
            return os.path.join(in_find_dir, file_name)

    return False


def find_all_file(in_path):
    # 遍历根目录及其子目录
    for in_res_folder_name, in_res_sub_folder, in_res_file_name in os.walk(in_path):
        if in_res_file_name:
            print('in_res_file_name：', in_res_file_name)
            print('in_res_folder_name:', in_res_folder_name)
            # 合并两个csv
            finger_file = ''
            hetero_system_file = ''
            for i_f in in_res_file_name:
                if 'finger' in i_f:
                    finger_file = os.path.join(in_res_folder_name, i_f)
                else:
                    hetero_system_file = os.path.join(in_res_folder_name, i_f)

            print('指纹文件为：', finger_file)
            print('异系统文件为：', hetero_system_file)

            tmp_merger_df = pd.merge(read_csv_get_df(finger_file), read_csv_get_df(hetero_system_file),
                                     left_on="f_time", right_on="f_time", how='left')

            out_file = finger_file.replace('.csv', '_hetero_sys_merge.csv')
            df_write_to_csv(tmp_merger_df, out_file)
            print('---' * 50)


def merge_two_file(in_file_a, in_file_b):
    tmp_merger_df = pd.merge(read_csv_get_df(in_file_a), read_csv_get_df(in_file_b), left_on="f_time",
                             right_on="f_time")

    out_file = in_file_a.replace('.csv', '_hetero_sys_final_result.csv')
    df_write_to_csv(tmp_merger_df, out_file)


def get_output_dir_csv(in_src_data):
    tmp_res_list = []
    in_output_dir_list = find_output_dir(in_src_data)
    for i_dir in in_output_dir_list:
        in_res_list = Common.list_files_in_directory(i_dir)
        tmp_res_list.extend(in_res_list)

    # 只获取finger文件
    # tmp_res_list = [x for x in tmp_res_list if 'finger' in x]
    return tmp_res_list


def merge_file_list(in_file_list):
    finger_file = ''
    hetero_system_file = ''
    for i_in_f in in_file_list:
        print(i_in_f)

        if 'finger' in os.path.basename(i_in_f):
            finger_file = i_in_f
        else:
            hetero_system_file = i_in_f

    print('指纹文件为：', finger_file)
    print('异系统文件为：', hetero_system_file)

    tmp_merger_df = pd.merge(read_csv_get_df(finger_file), read_csv_get_df(hetero_system_file),
                             left_on="f_time", right_on="f_time", how='left')

    out_file = finger_file.replace('.csv', '_hetero_sys_merge.csv')
    df_write_to_csv(tmp_merger_df, out_file)
    print('---' * 50)


def merge_file_list_in_mr_file(in_file_list, in_mr_file):
    finger_file = ''
    for i_in_f in in_file_list:
        print(i_in_f)
        if 'finger' in os.path.basename(i_in_f):
            finger_file = i_in_f

    print('指纹文件为：', finger_file)
    print('异系统文件为：', in_mr_file)

    tmp_merger_df = pd.merge(read_csv_get_df(finger_file), read_csv_get_df(in_mr_file),
                             left_on="f_time", right_on="f_time", how='left')

    out_file = finger_file.replace('.csv', '_hetero_sys_merge.csv')
    df_write_to_csv(tmp_merger_df, out_file)
    print('---' * 50)


if __name__ == '__main__':
    folder_path = r'D:\MrData\0321\20240321\iqoo7'
    mr_data_file = r'D:\MrData\0321\20240321\iqoo7\NR_MR_Detail_20240321152838_hetero_sys_final_result_add_5G.csv'

    res_dir_list = get_all_sub_dir(folder_path)
    for i_data_dir in res_dir_list:
        res_file_list = get_output_dir_csv(i_data_dir)
        # 传人文件list
        # merge_file_list(res_file_list)
        # 传入mr数据文件
        merge_file_list_in_mr_file(res_file_list, mr_data_file)
        print('--' * 50)

    # 获取output目录
    # res_file_list = get_output_dir_csv(folder_path)
    # print(res_file_list)
    # merge_file_list(res_file_list)
