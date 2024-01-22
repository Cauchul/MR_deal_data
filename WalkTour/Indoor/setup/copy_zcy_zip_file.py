# -*- coding: utf-8 -*-
from Common import get_data_path_by_char, get_path_sub_absolute_dir, Common, FindFile, copy_file, move_file

folder_path = r'E:\work\MR_Data\1月18号\demo\20240118_源数据'
# 获取zip文件所在路径
res_list = get_data_path_by_char(folder_path)
print(res_list)
# 获取子目录list
for i_dir in res_list:
    # 获取当前目录下的所有文件
    # in_res_list = Common.list_files_in_directory(i_dir)
    res_zip_file_list = FindFile.find_files_with_string(i_dir, '*.zip')
    # print('res_zip_file_list: ', res_zip_file_list)

    if res_zip_file_list:
        res_sub_dir_list = get_path_sub_absolute_dir(i_dir)
        for i_sub_dir in res_sub_dir_list[:-1]:
            copy_file(res_zip_file_list[0], i_sub_dir)
            print(i_sub_dir)

        move_file(res_zip_file_list[0], res_sub_dir_list[-1])

# 拷贝当前路径下的zip文件到每一个子目录
# res_dir_list = get_path_sub_dir(r'E:\\work\\MR_Data\\1月18号\\demo\\20240118_源数据\\室内\\1')
# print(res_dir_list)
