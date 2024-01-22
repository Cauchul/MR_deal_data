# -*- coding: utf-8 -*-
import os.path
import shutil

from Common import FindFile, Common, check_file_exists, print_with_line_number


# 获取csv文件所在的所有子目录
def get_csv_path_list(in_data_path):
    in_res_path_list = FindFile.get_csv_file_dir_list(in_data_path)
    # 去除output和unzip目录；或许需要生成config文件的目录
    res_in_path_list = [in_i_string for in_i_string in in_res_path_list if
                        'output' not in in_i_string and 'unzip' not in in_i_string]
    # 列表排序
    res_in_path_list.sort()
    return res_in_path_list


def move_file(in_file_name):
    if '5G' in os.path.basename(in_file_name):
        in_dir = os.path.dirname(i_file_name)
        in_5G_dir = os.path.join(in_dir, '5G')
        print_with_line_number(f'当前文件：{in_file_name}，移动路径：{in_5G_dir}', __file__)
        if not check_file_exists(in_5G_dir):
            os.makedirs(in_5G_dir)
        # print(os.path.dirname(i_file_name))
        shutil.move(i_file_name, in_5G_dir)
    elif '4G' in os.path.basename(i_file_name):
        in_dir = os.path.dirname(i_file_name)
        in_4G_dir = os.path.join(in_dir, '4G')
        print_with_line_number(f'当前文件：{in_file_name}，移动路径：{in_4G_dir}', __file__)
        if not check_file_exists(in_4G_dir):
            os.makedirs(in_4G_dir)

        print(os.path.dirname(i_file_name))
        shutil.move(i_file_name, in_4G_dir)


if __name__ == '__main__':
    folder_path = r'E:\work\MR_Data\1月18号\demo\20240118_源数据'

    # 获取所有的csv文件所在目录
    csv_path_list = get_csv_path_list(folder_path)

    # 找到目录所有文件
    for i_dir in csv_path_list:
        print_with_line_number(f'当前处理路径:{i_dir}', __file__)
        in_res_list = Common.list_files_in_directory(i_dir)
        # 如果获取到的文件list中同时包含 4G 5G则需要把不同的文件分开
        # print('in_res_list: ', in_res_list)

        if len(in_res_list) > 3:
            # print('in_res_list: ', in_res_list)
            print('--' * 50)
            for i_file_name in in_res_list:
                move_file(i_file_name)
