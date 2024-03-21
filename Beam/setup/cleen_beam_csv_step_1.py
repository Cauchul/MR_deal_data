# -*- coding: utf-8 -*-

# 清楚数据最后一行时间不对的数据

import pandas as pd

from Common import read_csv_get_df, df_write_to_csv, get_data_path_by_char, FindFile


def clean_beam(in_beam_csv):
    in_res_df = read_csv_get_df(in_beam_csv)

    in_res_df = in_res_df[~in_res_df['PC Time'].str.contains('1970-01-01')]

    # for i in in_res_df['PC Time']:
    #     if '1970-01-01' in i:
    #         print('hello')
    #     print(i)

    df_write_to_csv(in_res_df, in_beam_csv)


def clean_up_mult_files(in_folder_path):
    res_list = FindFile.find_files_with_string(in_folder_path, 'Beam.csv')
    for i_f in res_list:
        print(i_f)
        clean_beam(i_f)


if __name__ == '__main__':
    # 找到目录下所有的Beam csv文件
    # folder_path = r'D:\MrData\3月4号\20240304'
    # clean_up_mult_files(folder_path)

    data_file = r'D:\MrData\3月15日\5G\20240315_Beam\xiaomi_13\小米13-5G--IN20240315-101344-Ping(1)_0318105044.csv'
    clean_beam(data_file)
