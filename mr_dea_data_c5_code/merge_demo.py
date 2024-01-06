# -*- coding: utf-8 -*-
import os
import shutil
import time

import pandas as pd

from Common import check_path, list_files_in_directory, move_file, read_csv_get_df, clear_path, find_nth_occurrence
from GlobalConfig import tmp_res_out_path


def clear_merge_path(in_path):
    if os.path.exists(in_path):
        shutil.rmtree(in_path)
        time.sleep(1)


def merge_file_new_format(in_out_path, in_c_v, in_c_h):
    in_f_list = list_files_in_directory(tmp_res_out_path)

    print('in_f_list: ', in_f_list)

    # 先把非横纵数据先清理出去
    for in_i_f in in_f_list:
        if in_c_v not in in_i_f and in_c_h not in in_i_f:
            print('move file11: ', in_i_f)
            move_file(in_i_f, in_out_path)
        else:
            if in_c_v in in_i_f:
                tmp_h_f = in_i_f.replace('UE_V', 'UE_H')
            else:
                continue
            # else:
            #     tmp_h_f = in_i_f.replace('UE_H', 'UE_V')

            print('in_i_f: ', in_i_f)
            print('tmp_h_f: ', tmp_h_f)
            if os.path.exists(tmp_h_f):  # 如果能找到对应的文件，则合并
                in_f_list.remove(tmp_h_f)
                i_df = read_csv_get_df(in_i_f)
                opp_df = read_csv_get_df(tmp_h_f)
                # 合并输出
                data = pd.concat([i_df, opp_df])

                out_f_n = os.path.basename(in_i_f).split('.')[0]

                def get_out_file_name(in_f):
                    in_date_str = in_f.split('.')[0]
                    last_str = in_date_str.split('_')[-1]
                    index = find_nth_occurrence(in_date_str, '_', 7)
                    tmp_out_file = in_date_str[:index] + '_' + last_str + '.csv'
                    print(tmp_out_file)
                    return tmp_out_file

                out_f_n = get_out_file_name(out_f_n)
                out_file = os.path.join(in_out_path, out_f_n)
                print('out_file: ', out_file)
                data.to_csv(out_file, index=False)
                os.remove(in_i_f)
                os.remove(tmp_h_f)
            else:
                print('move file22: ', in_i_f)
                move_file(in_i_f, in_out_path)


def merge_file_old_format(in_out_path, in_c_v, in_c_h):
    in_f_list = list_files_in_directory(tmp_res_out_path)

    print('in_f_list: ', in_f_list)

    # 先把非横纵数据先清理出去
    for in_i_f in in_f_list:
        if in_c_v not in in_i_f and in_c_h not in in_i_f:
            print('move file11: ', in_i_f)
            move_file(in_i_f, in_out_path)
        else:
            if in_c_v in in_i_f:
                tmp_h_f = in_i_f.replace('纵', '横').replace('v.csv', 'h.csv')
            else:
                tmp_h_f = in_i_f.replace('横', '纵').replace('h.csv', 'v.csv')

            print('in_i_f: ', in_i_f)
            print('tmp_h_f: ', tmp_h_f)
            if os.path.exists(tmp_h_f):  # 如果能找到对应的文件，则合并
                in_f_list.remove(tmp_h_f)
                i_df = read_csv_get_df(in_i_f)
                opp_df = read_csv_get_df(tmp_h_f)
                # 合并输出
                data = pd.concat([i_df, opp_df])

                merge_out_file_name = f'4G_HaiDian_Indoor_WT_LOG_LOG_UE_1206'
                out_file = os.path.join(in_out_path, out_f_n + '_横纵.csv')
                print('out_file: ', out_file)
                data.to_csv(out_file, index=False)
                os.remove(in_i_f)
                os.remove(tmp_h_f)
            else:
                print('move file22: ', in_i_f)
                move_file(in_i_f, in_out_path)


if __name__ == '__main__':
    out_d_p = r'E:\work\demo_merge\merged'
    # clear_merge_path(out_d_p)
    # check_path(out_d_p)
    # merge_res_file(out_d_p)
    # f_list = list_files_in_directory(tmp_res_out_path)
    # for i_f in f_list:
    #     print(i_f)

    # 先把非横纵数据先清理出去
    # merge_file_old_format(out_d_p, 'v.csv', 'h.csv')
    merge_file_new_format(out_d_p, r'UE_V', r'UE_H')
