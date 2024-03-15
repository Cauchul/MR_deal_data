# -*- coding: utf-8 -*-
import os.path

import pandas as pd

from Common import read_csv_get_df, df_write_to_csv, find_output_dir, Common, FindFile, print_with_line_number


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

def list_dir_files(in_dir):
    tmp_list = []
    for root, dirs, files in os.walk(in_dir):
        if in_dir == root:
            for file in files:
                # file_path = os.path.join(root, file)
                # 处理文件的逻辑
                tmp_list.append(root)
    return tmp_list


def get_output_dir_csv(in_src_data):
    tmp_res_list = []
    in_output_dir_list = find_output_dir(in_src_data)
    for in_i_dir in in_output_dir_list:
        in_res_list = list_dir_files(in_i_dir)
        tmp_res_list.extend(in_res_list)

    return list(set(tmp_res_list))


def merge_all_data(in_file_list):
    in_out_file = os.path.join(os.path.dirname(in_file_list[0]), 'finger_hetero_system_merge.csv')

    if os.path.exists(in_out_file):
        print_with_line_number(f'删除旧输出文件 {in_out_file}', __file__)
        in_file_list.remove(in_out_file)
        os.remove(in_out_file)

    left_file = right_file = ''

    for i_f in in_file_list:
        if '_finger' in i_f:
            left_file = i_f
        elif 'extension_final_result' in i_f:
            right_file = i_f

    if left_file and right_file:
        print_with_line_number(f'当前左边文件 {left_file}', __file__)
        print_with_line_number(f'当前右边文件 {right_file}', __file__)
        data = pd.merge(read_csv_get_df(left_file), read_csv_get_df(right_file), left_on="f_time",
                        right_on="f_time", how='left')

        data.to_csv(in_out_file, index=False)
        print_with_line_number(f'输出文件为: {in_out_file}', __file__)

    # one_file = in_file_list[0]
    # two_file = in_file_list[1]
    #
    # if 'finger' in one_file:
    #     print(f'当前左边文件 {one_file}')
    #     data = pd.merge(read_csv_get_df(one_file), read_csv_get_df(two_file), left_on="f_time",
    #                     right_on="f_time", how='left')
    # else:
    #     print(f'当前左边文件 {two_file}')
    #     data = pd.merge(read_csv_get_df(two_file), read_csv_get_df(one_file), left_on="f_time",
    #                     right_on="f_time", how='left')


if __name__ == '__main__':
    folder_path = r'E:\work\MrData\data_place\merge\0315\mate40'
    # 获取当前路径下的所有csv文件
    # res_file_list = FindFile.get_cur_dir_all_csv(folder_path)
    # merge_all_data(res_file_list)
    # 获取output目录
    res_file_list = get_output_dir_csv(folder_path)
    for i_dir in res_file_list:
        res_f = Common.list_files_in_directory(i_dir)
        # print_with_line_number(f'当前合并文件为：{res_f}', __file__)
        merge_all_data(res_f)
        print('--' * 50)
