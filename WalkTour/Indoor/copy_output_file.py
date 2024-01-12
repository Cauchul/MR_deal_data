# -*- coding: utf-8 -*-
# 拷贝文件
import os
import shutil

from Common import get_path_sub_dir, get_file_list_by_char, copy_file, Common, print_with_line_number


def copy_file_to_dir(in_file, in_out_path):
    shutil.copy(in_file, in_out_path)


def file_add_specified_suffix(in_file, in_suffix):
    in_res_file_name, in_res_file_extension = os.path.splitext(in_file)
    in_tmp_file_name = in_res_file_name + f'_{in_suffix}' + in_res_file_extension

    print_with_line_number(in_tmp_file_name, __file__)
    return in_tmp_file_name


def copy_output_files(in_folder_path, in_char='4G'):
    in_res_dir_list = get_path_sub_dir(in_folder_path)
    for i_p in in_res_dir_list:
        sub_path = os.path.join(in_folder_path, i_p)
        out_put_path = os.path.join(sub_path, 'output')
        # 获取需要合并的所有的文件list
        res_list = get_file_list_by_char(out_put_path, in_char)
        # print(out_put_path)
        print_with_line_number(f'需要拷贝的文件：{res_list}', __file__)
        print_with_line_number(f'数据拷贝保存到目录：{sub_path}', __file__)

        for i_f in res_list:
            # print('i_p: ', i_p)
            # if i_p not in i_f:
            #     res_new_i_f = file_add_specified_suffix(i_f, i_p)
            #     print_with_line_number(f'文件重命名为：{res_new_i_f}', __file__)
            #     # 重命名文件
            #     os.rename(i_f, res_new_i_f)
            #     print_with_line_number(f'当前拷贝文件: {res_new_i_f}', __file__)
            #     copy_file(res_new_i_f, os.path.dirname(sub_path))
            # # 拷贝文件
            # else:
            print_with_line_number(f'当前拷贝文件: {i_f}', __file__)
            copy_file(i_f, os.path.dirname(sub_path))
        print('--' * 50)


def copy_files_to_pre_dir(in_folder_path):
    in_res_dir_list = get_path_sub_dir(in_folder_path)
    for i_p in in_res_dir_list:
        sub_path = os.path.join(in_folder_path, i_p)
        print(f'sub_path: {sub_path}')
        res_file_list = Common.list_files_in_directory(sub_path)
        print('res_file_list:', res_file_list)

        for i_f in res_file_list:
            print(i_f)
            # copy_file(i_f, r'D:\working\data_conv\data_outdoor\小米13')


file_char = '5G'
print_with_line_number(f'当前拷贝的数据为：{file_char}', __file__)
# 拷贝文件
copy_output_files(r'D:\working\data_conv\室外\mate40', file_char)
