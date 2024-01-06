# -*- coding: utf-8 -*-
import os.path

from Common import list_files_in_directory, get_path_sub_dir, copy_file


# 获取所有子目录下的文件列表
def get_cur_path_file_list(in_path):
    # print(i_path)
    data_path = os.path.join(pub_path, in_path)
    print('data_path: ', data_path)
    in_res_list = list_files_in_directory(data_path)
    # print('in_res_list: ', in_res_list)
    return in_res_list


pub_path = r'D:\working\1214\1214国际财经中心\国际财经中心'

# 获取所有子目录
subdirectories = get_path_sub_dir(pub_path)
for i_path in subdirectories:
    res_file_list = get_cur_path_file_list(i_path)
    print('res_file_list: ', res_file_list)

    for i_file in res_file_list:
        if i_file.endswith('.csv'):
            if 'LTE' in os.path.basename(i_file):
                tmp_data_p = os.path.dirname(i_file)
                tmp_wetest_path = os.path.join(tmp_data_p, 'wetest')
                copy_file(i_file, tmp_wetest_path)
            else:
                tmp_data_p = os.path.dirname(i_file)
                tmp_wt_path = os.path.join(tmp_data_p, 'walktour')
                copy_file(i_file, tmp_wt_path)


# for i_path in subdirectories:
#     # print(i_path)
#     data_path = os.path.join(pub_path, i_path)
#     print('data_path: ', data_path)
#     res_list = list_files_in_directory(data_path)
#     print('res_list: ', res_list)
