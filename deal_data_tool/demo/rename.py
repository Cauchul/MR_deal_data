# -*- coding: utf-8 -*-
import os

from Common import list_files_in_directory, get_all_data_path, split_path_get_list

data_path = r'D:\working\1226\国际财经中心_5G_7\output'


# res_path_list = split_path_get_list(data_path)
# print(res_path_list)
# in_v = res_path_list[-2].split('_')[-1]


def rename_file_name(in_data_path):
    f_list = list_files_in_directory(in_data_path)

    res_path_list = split_path_get_list(in_data_path)
    # print(res_path_list)
    in_v = res_path_list[-2].split('_')[-1]
    print(in_v)

    # print(f_list)

    for i_f in f_list:
        if '_原始文件' not in i_f:
            print(i_f)
            os.rename(i_f, os.path.join(data_path, os.path.basename(i_f).split('.')[0] + f'_{in_v}.csv'))


rename_file_name(data_path)

# data_path = r'D:\working\1226'
# res_list = get_all_data_path(data_path)
# # print(res_list)
# for i_path in res_list:
#     print(i_path)
