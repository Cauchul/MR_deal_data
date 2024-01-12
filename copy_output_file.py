# -*- coding: utf-8 -*-
import os
import shutil

from Common import get_path_sub_dir, get_file_list_by_char, copy_file, Common


def copy_file_to_dir(in_file, in_out_path):
    shutil.copy(in_file, in_out_path)


# folder_path = r'D:\working\data_conv\data_outdoor\小米13'
# res_dir_list = get_path_sub_dir(folder_path)
# file_list = []


def copy_output_files(in_folder_path):
    in_res_dir_list = get_path_sub_dir(in_folder_path)
    for i_p in in_res_dir_list:
        sub_path = os.path.join(in_folder_path, i_p)
        out_put_path = os.path.join(sub_path, 'output')
        # 获取需要合并的所有的文件list
        res_list = get_file_list_by_char(out_put_path, '5G')
        # print(out_put_path)
        # print(res_list)
        print(sub_path)

        for i_f in res_list:
            print(i_f)
            copy_file(i_f, os.path.dirname(sub_path))


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


copy_output_files(r'D:\working\data_conv\室外1\小米13')
