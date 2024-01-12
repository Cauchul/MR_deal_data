# -*- coding: utf-8 -*-
import os

from Common import find_output_dir, Common, get_file_list_by_char, print_with_line_number, read_csv_get_df, \
    df_write_to_csv


# 修改f_sid值
# data_path = r'E:\work\MR_Data\1月12号\下午测试'


def set_f_sid_value(in_char='4G'):
    res_output_list = find_output_dir(data_path)

    print_with_line_number(f'当前查找文件的标志为：{in_char}', __file__)
    # print_with_line_number(res_output_list, __file__)
    for i_path in res_output_list:
        print_with_line_number(f'当前output路径: {i_path}', __file__)
        res_list = Common.split_path_get_list(os.path.dirname(i_path))
        print_with_line_number(f'路径list: {res_list}', __file__)
        res_csv_list = get_file_list_by_char(i_path, in_char)
        for i_f in res_csv_list:
            print_with_line_number(f'当前处理结果文件: {i_f}', __file__)
            res_df = read_csv_get_df(i_f)
            if 'f_sid' in res_df.columns:
                f_sid = int(res_list[-2])
                res_df['f_sid'] = f_sid
                # res_df['f_sid'] = 1
                print_with_line_number(f'当前 f_sid 设置的sid为: {f_sid}', __file__)
            else:
                u_sid = int(res_list[-2])
                res_df['u_sid'] = u_sid
                # res_df['u_sid'] = 1
                print_with_line_number(f'当前 u_sid 设置的sid为: {u_sid}', __file__)
            df_write_to_csv(res_df, i_f)
        print('---' * 50)


# 处理当前路径下的文件
data_path = r'E:\work\MR_Data\1月12号\下午测试'
set_f_sid_value('4G')
set_f_sid_value('5G')
