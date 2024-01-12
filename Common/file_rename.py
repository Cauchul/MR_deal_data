# -*- coding: utf-8 -*-
# 结果文件重命名
import os
import shutil

from Common import get_path_sub_dir, get_file_list_by_char, copy_file


def copy_file_to_dir(in_file, in_out_path):
    shutil.copy(in_file, in_out_path)


def file_add_specified_suffix(in_file, in_suffix):
    in_res_file_name, in_res_file_extension = os.path.splitext(in_file)
    in_tmp_file_name = in_res_file_name + f'_{in_suffix}' + in_res_file_extension

    print(in_tmp_file_name)
    return in_tmp_file_name


folder_path = r'D:\working\data_conv\室外1\小米13'
res_dir_list = get_path_sub_dir(folder_path)
file_list = []

for i_p in res_dir_list:
    sub_path = os.path.join(folder_path, i_p)
    print(sub_path)
    # print(i_p)
    out_put_path = os.path.join(sub_path, 'output')
    res_file_list = get_file_list_by_char(out_put_path, '5G')
    print(res_file_list)
    for i_f in res_file_list:
        res_new_name = file_add_specified_suffix(i_f, i_p)
        print(res_new_name)
        os.rename(i_f, res_new_name)
