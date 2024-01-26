# -*- coding: utf-8 -*-
import os

from Common import FindFile, find_output_dir, Common, clear_merge_path, clear_path


# 删除目录下指定文件

# data_path = r'E:\work\MR_Data\1月18号\20240118_源数据_clear\室内'


# res_file_list = get_data_path_by_char(data_path, 'WalkTour')
# 找到当前目录下，所有的包含 WalkTour 的文件 WeTest

def get_output_dir_csv(in_src_data):
    tmp_res_list = []
    in_output_dir_list = find_output_dir(in_src_data)
    for i_dir in in_output_dir_list:
        in_res_list = Common.list_files_in_directory(i_dir)
        tmp_res_list.extend(in_res_list)

    # 只获取finger文件
    # tmp_res_list = [x for x in tmp_res_list if 'finger' in x]
    return tmp_res_list


def clear_file(in_data_path, in_char):
    res_zip_file_list = FindFile.find_files_with_string(in_data_path, in_char)

    res_zip_file_list = [x for x in res_zip_file_list if 'unzip' not in x]

    for i_f in res_zip_file_list:
        print(i_f)
        os.remove(i_f)


def clear_output_dir(in_data_path):
    # 找到所有的output目录
    # res_dir_list = get_output_dir_csv(in_data_path)
    res_dir_list = find_output_dir(in_data_path)

    for i_dir in res_dir_list:
        print(i_dir)
        clear_path(i_dir)


if __name__ == '__main__':
    data_path = r'E:\work\MR_Data\1月26号\20240126室外上午_new_no_table\上午\岳云伟'
    # 删除output目录
    # clear_output_dir(data_path)
    # 删除目录下的所有含有某字符串的文件
    clear_file(data_path, 'WalkTour')
