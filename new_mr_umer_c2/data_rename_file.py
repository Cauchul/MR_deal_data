# -*- coding: utf-8 -*-
import os

from Common import get_path_sub_dir, list_files_in_directory

# 获取多个目录
pub_path = r'D:\working\1214\test\国际财经中心'
# 获取当前目录下的直接子目录
subdirectories = get_path_sub_dir(pub_path)

# print(subdirectories)

for i_path in subdirectories:
    data_path = os.path.join(pub_path, i_path)
    # print(data_path)
    in_res_list = list_files_in_directory(data_path)
    # print(in_res_list)
    for i_f in in_res_list:
        if i_f.endswith('.csv'):
            if '_tb' in i_f:
                os.rename(i_f, i_f.replace('_tb', '_table'))
            else:
                print('i_f: ', i_f)
                os.rename(i_f, i_f.replace('20231214\\', '20231214\\UE_flag_'))
