# -*- coding: utf-8 -*-
import os

from Common import find_output_dir, Common, get_file_list_by_char, print_with_line_number, read_csv_get_df, \
    df_write_to_csv

# 修改f_sid值
data_path = r'D:\working\data_conv\室外\iqoo7'
res_output_list = find_output_dir(data_path)

# print_with_line_number(res_output_list, __file__)
for i_path in res_output_list:
    print_with_line_number(f'当前output路径: {i_path}', __file__)
    res_list = Common.split_path_get_list(os.path.dirname(i_path))
    print_with_line_number(f'路径list: {res_list}', __file__)
    res_csv_list = get_file_list_by_char(i_path, '5G')
    for i_f in res_csv_list:
        print_with_line_number(f'当前处理结果文件: {i_f}', __file__)
        res_df = read_csv_get_df(i_f)
        if 'f_sid' in res_df.columns:
            res_df['f_sid'] = int(res_list[-1])
            print_with_line_number(f'当前 f_sid 设置的sid为: {res_list[-1]}', __file__)
        else:
            res_df['u_sid'] = int(res_list[-1])
            print_with_line_number(f'当前 u_sid 设置的sid为: {res_list[-1]}', __file__)
        df_write_to_csv(res_df, i_f)
    print('---' * 50)

