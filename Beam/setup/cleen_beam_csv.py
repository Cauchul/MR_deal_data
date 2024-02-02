# -*- coding: utf-8 -*-
import pandas as pd

from Common import read_csv_get_df, df_write_to_csv, get_data_path_by_char, FindFile

data_file = r'E:\work\MR_Data\new_format_data\小米13-1-5G--IN20240112-141903-FTPD(1)_0129091830.csv'


def clean_beam(in_beam_csv):
    in_res_df = read_csv_get_df(in_beam_csv)

    in_res_df = in_res_df[~in_res_df['PC Time'].str.contains('1970-01-01')]

    # for i in in_res_df['PC Time']:
    #     if '1970-01-01' in i:
    #         print('hello')
    #     print(i)

    df_write_to_csv(in_res_df, in_beam_csv)


if __name__ == '__main__':
    # 找到目录下所有的Beam csv文件
    folder_path = r'E:\work\MR_Data\1月26号\20240126室外上午_new_no_table\上午\岳云伟\S22\5G'
    # res_list = get_data_path_by_char(folder_path, '')
    res_list = FindFile.find_files_with_string(folder_path, 'Beam.csv')
    for i_f in res_list:
        print(i_f)
        clean_beam(i_f)

