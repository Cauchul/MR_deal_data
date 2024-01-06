# -*- coding: utf-8 -*-
import os

from Common import list_files_in_directory, get_file_by_str, copy_file, get_path_sub_dir, move_file

# 每个目录下递归创建目录
# 例子：递归创建目录 "parent/child/grandchild"
# pub_path = r'D:\working\1214\1214国际财经中心\国际财经中心\国际财经中心5G纵5_20231214'
#
# dir_name = get_last_character(pub_path, '\\').split('_')[0]
# src_char = dir_name[-2:]
# print(src_char)
# if '纵' in src_char:
#     char = src_char.replace('纵', 'V')
# else:
#     char = src_char.replace('横', 'H')
# print(char)

# wt_path_list = [f'walktour\{char}_S22', f'walktour\{char}_iqoo7']
# wetest_path_list = [f'wetest\{char}_487', f'wetest\{char}_729', f'wetest\{char}_748']
#
# for i_p in wt_path_list + wetest_path_list:
#     print(os.path.join(pub_path, i_p))
#     os.makedirs(os.path.join(pub_path, i_p))

# 获取多个目录
pub_path = r'D:\working\1214\test\国际财经中心'


# 后去当前目录下的所有子目录
def get_immediate_subdirectories(directory):
    res_sub_dir = [d for d in os.listdir(directory) if os.path.isdir(os.path.join(directory, d))]
    return res_sub_dir


# 获取所有子目录下的文件列表
def get_cur_path_file_list(in_path):
    # print(i_path)
    in_data_path = os.path.join(pub_path, in_path)
    print('in_data_path: ', in_data_path)
    in_res_list = list_files_in_directory(in_data_path)
    # print('in_res_list: ', in_res_list)
    return in_res_list


# 获取当前目录下的直接子目录
subdirectories = get_path_sub_dir(pub_path)

for i_path in subdirectories:
    print(i_path)
    dir_name = i_path.split('_')[0]
    src_char = dir_name[-2:]
    print(src_char)
    if '纵' in src_char:
        char = src_char.replace('纵', 'V')
    else:
        char = src_char.replace('横', 'H')
    print(char)
    wt_path_list = [f'walktour\{char}_S22', f'walktour\{char}_iQOO7']
    wetest_path_list = [f'wetest\{char}_487', f'wetest\{char}_729', f'wetest\{char}_748']

    # 获取当前目录下的zip文件
    data_path = os.path.join(pub_path, i_path)
    zcy_zip_file = get_file_by_str('zip', data_path)
    print('zcy_zip_file: ', zcy_zip_file)

    for i_p in wt_path_list + wetest_path_list:
        copy_char = i_p.split('_')[1]
        cur_create_path = os.path.join(data_path, i_p)
        print('cur_create_path: ', cur_create_path)

        # 后去目录下的目录下的
        print('zcy_zip_file: ', zcy_zip_file)
        if not os.path.exists(cur_create_path):
            os.makedirs(cur_create_path)

        # 移动UE数据
        file_list = get_cur_path_file_list(data_path)
        print('file_list: ', file_list)
        for i_f in file_list:
            print('i_f: ', i_f)
            if i_f.endswith('.csv') and copy_char in i_f:
                # print('i_f: ', i_f)
                move_file(i_f, cur_create_path)

        # if zcy_zip_file:
        copy_file(zcy_zip_file, cur_create_path)
    os.remove(zcy_zip_file)

