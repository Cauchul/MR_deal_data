# -*- coding: utf-8 -*-
import os
import shutil
import time

import pandas as pd

from Common import check_path, list_files_in_directory, move_file, read_csv_get_df, clear_path, find_nth_occurrence, \
    copy_file, merge_mult_csv_file, df_write_to_csv
from GlobalConfig import tmp_res_out_path


def clear_merge_path(in_path):
    if os.path.exists(in_path):
        shutil.rmtree(in_path)
        time.sleep(1)


def get_out_file_name(in_f):
    # print('in_f: ', in_f)
    in_date_str = in_f.split('.')[0]
    last_str = in_date_str.split('_')[-1]
    index = find_nth_occurrence(in_date_str, '_', 7)
    tmp_out_file = in_date_str[:index] + '_' + last_str[-1:]
    # print(tmp_out_file)
    return tmp_out_file


# # 获取需要合并的文件list
# def merge_file_new_format(in_out_path, in_vertical, in_horizontal):
#     in_file_list = list_files_in_directory(tmp_res_out_path)
#     tmp_list = []
#     print('in_f_list: ', in_file_list)
#     check_list = []
#     # 获取所有的
#     for i_file in in_file_list:
#         print(i_file)
#         if in_horizontal in i_file:
#             tmp_h_f = i_file.replace('UE_H', 'UE_V')
#         else:
#             tmp_h_f = i_file.replace('UE_V', 'UE_H')
#
#         print('in_i_f: ', i_file)
#         print('tmp_h_f: ', tmp_h_f)
#         if os.path.exists(tmp_h_f):
#             if tmp_h_f not in check_list and i_file not in check_list:
#                 check_list.append(tmp_h_f)
#                 tmp_list.append([i_file, tmp_h_f])
#
#     print('tmp_list: ', tmp_list)

def merge_data(in_list, in_out_path):
    # 合并数据
    for i_file in in_list:
        # print('i_file: ', i_file)
        if 'UE_V' in i_file or 'v.csv' in i_file:
            tmp_file = i_file.replace('UE_V', 'UE_H').replace('v.csv', 'h.csv')
        else:
            tmp_file = i_file.replace('UE_H', 'UE_V').replace('h.csv', 'v.csv')

        out_f = get_out_file_name(os.path.basename(i_file))
        # print('out_f: ', out_f)
        out_file = os.path.join(in_out_path, out_f)
        # print('out_file: ', out_file)
        # 如果另一半存在，则合并，否则直接重命名拷贝
        if os.path.exists(tmp_file):
            print('i_file: ', i_file)
            print('tmp_file: ', tmp_file)
            in_list.remove(tmp_file)
            merge_df = merge_mult_csv_file(i_file, tmp_file)
            df_write_to_csv(merge_df, out_file + '_merge.csv')
        else:
            # 重命名，拷贝
            print('in copy')
            copy_file(i_file, out_file + '.csv')


# 获取需要合并的文件list
def merge_file_new_format(in_out_path):
    in_file_list = list_files_in_directory(tmp_res_out_path)
    lte_list = []
    nr_list = []
    # 获取LTE NR的的文件list
    for i_file in in_file_list:
        # print(i_file)
        if 'LTE' in i_file:
            lte_list.append(i_file)
        else:
            nr_list.append(i_file)

    print('lte_list: ', lte_list)
    print('nr_list: ', nr_list)
    merge_data(lte_list, in_out_path)
    merge_data(nr_list, in_out_path)
    # # 合并数据
    # for i_file in lte_list:
    #     print(i_file)
    #     if 'UE_V' in i_file:
    #         tmp_file = i_file.replace('UE_V', 'UE_H')
    #     else:
    #         tmp_file = i_file.replace('UE_H', 'UE_V')
    #
    #     out_f = get_out_file_name(os.path.basename(i_file))
    #     print('out_f: ', out_f)
    #     out_file = os.path.join(in_out_path, out_f)
    #     print('out_file: ', out_file)
    #     # 如果另一半存在，则合并，否则直接重命名拷贝
    #     if os.path.exists(tmp_file):
    #         lte_list.remove(tmp_file)
    #         merge_df = merge_mult_csv_file(i_file, tmp_file)
    #         df_write_to_csv(merge_df, out_file + '_merge.csv')
    #     else:
    #         # 重命名，拷贝
    #         copy_file(i_file, out_file + '.csv')


if __name__ == '__main__':
    out_d_p = r'E:\work\demo_merge\merged'
    merge_file_new_format(out_d_p)
