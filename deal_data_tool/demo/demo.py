# -*- coding: utf-8 -*-

# 获取目录下的所有的
import os

from Common import list_files_in_directory, copy_file, check_file_exists, split_path_get_list


def find_directory(in_directory, in_target_name):
    tmp_res_list = []
    for root, dirs, files in os.walk(in_directory):
        if in_target_name in dirs:
            tmp_res_list.append(os.path.abspath(os.path.join(root, in_target_name)))
    return tmp_res_list


# 示例用法
directory = r"D:\working\reno 8\5"
target_name = "output"

out_path = r'D:\working\merge\merge_tmp'

result = find_directory(directory, target_name)
for path in result:
    # print(path)
    res_list = list_files_in_directory(path)
    i = 0
    for i_f in res_list:
        if '原始文件' not in i_f:
            print('i_f： ', i_f)
            res_list = split_path_get_list(i_f)
            out_file = os.path.join(out_path, res_list[-1])
            if check_file_exists(out_file):
                while True:
                    i += 1
                    # if check_file_exists(res_list[-1].split('.')[0] + f'_{i}' + '.csv'):
                    if check_file_exists(os.path.join(out_path, res_list[-1].split('.')[0] + f'_{i}' + '.csv')):
                        continue
                    else:
                        out_file = os.path.join(out_path, res_list[-1].split('.')[0] + f'_{i}.csv')
                        # out_file = res_list[-1].split('.')[0] + f'_{i}.csv'
                        break

            print(out_file)
            copy_file(i_f, out_path)
