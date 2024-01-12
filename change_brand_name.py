# -*- coding: utf-8 -*-
from Common import read_csv_get_df, df_write_to_csv, Common

file_res_list = Common.list_files_in_directory(r'D:\working\data_conv\out_path')
for i_f in file_res_list:
    res_list = Common.split_path_get_list(i_f)
    bran_name = res_list[-1].split('_')[0]
    print(i_f)
    print(bran_name)
    src_df = read_csv_get_df(i_f)
    src_df['f_device_brand'] = bran_name
    df_write_to_csv(src_df, i_f)
