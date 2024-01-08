# -*- coding: utf-8 -*-
# 修改文件头，从f_到u_或者是u_到f_
import os.path

from Common import read_csv_get_df, Common, df_write_to_xlsx

src_path = r'D:\working\data_conv\试卷\OPPOreno8'

file_res_list = Common.list_files_in_directory(src_path)

for i_f in file_res_list:
    print(i_f)
    src_df = read_csv_get_df(i_f)

    res_list = Common.split_path_get_list(i_f)
    print('res_list: ', res_list)
    if 'finger_id' in src_df.columns:
        src_df.rename(columns=lambda x: x.replace('f_', 'u_'), inplace=True)
        src_df = src_df.rename(
            columns={
                'finger_id': 'uemr_id',
            })
    else:
        src_df.rename(columns=lambda x: x.replace('u_', 'f_'), inplace=True)
        src_df = src_df.rename(
            columns={
                'uemr_id': 'finger_id',
            })

    df_write_to_xlsx(src_df, os.path.join(src_path, res_list[-1]))
