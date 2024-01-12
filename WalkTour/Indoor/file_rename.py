# -*- coding: utf-8 -*-
# 结果文件重命名
import os
import shutil

from Common import get_path_sub_dir, get_file_list_by_char, copy_file, print_with_line_number


def copy_file_to_dir(in_file, in_out_path):
    shutil.copy(in_file, in_out_path)


def file_add_specified_suffix(in_file, in_suffix):
    in_res_file_name, in_res_file_extension = os.path.splitext(in_file)
    in_tmp_file_name = in_res_file_name + f'_{in_suffix}' + in_res_file_extension

    print(in_tmp_file_name)
    return in_tmp_file_name


folder_path = r'D:\working\data_conv\20240111(1)\20240111\5G\2'
res_dir_list = get_path_sub_dir(folder_path)
file_list = []

for i_p in res_dir_list:
    sub_path = os.path.join(folder_path, i_p)
    print_with_line_number(f'当前处理路径：{sub_path}', __file__)
    # print(i_p)
    out_put_path = os.path.join(sub_path, 'output')
    file_char = '5G'
    print_with_line_number(f'当前处理：{file_char} 数据', __file__)
    res_file_list = get_file_list_by_char(out_put_path, file_char)
    print_with_line_number(res_file_list, __file__)
    for i_f in res_file_list:
        res_new_name = file_add_specified_suffix(i_f, i_p)
        print_with_line_number(f'文件新名称：{res_new_name}', __file__)
        os.rename(i_f, res_new_name)
