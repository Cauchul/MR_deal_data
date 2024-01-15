# -*- coding: utf-8 -*-
# 拷贝文件到上一层目录
import os

from Common import find_output_dir, get_file_list_by_char, print_with_line_number, copy_file, Common


# data_path = r'D:\working\data_conv\data_back\4G输出'
# output_path = r'D:\working\data_conv\data_back\4G输出\out'


# def file_add_specified_suffix(in_file, in_suffix, in_ot_suffix):
#     in_res_file_name, in_res_file_extension = os.path.splitext(in_file)
#     # print('in_res_file_name: ', in_res_file_name)
#     if f'_{in_suffix}' not in in_res_file_name:
#         in_tmp_file_name = in_res_file_name + f'_{in_suffix}_{in_ot_suffix}' + in_res_file_extension
#     else:
#         in_tmp_file_name = in_res_file_name + in_res_file_extension
#
#     # print_with_line_number(f'拷贝文件名：{in_tmp_file_name}', __file__)
#     return in_tmp_file_name

def file_add_specified_suffix(in_file, *in_suffix):
    in_res_file_name, in_res_file_extension = os.path.splitext(in_file)
    # print('in_res_file_name: ', in_res_file_name)
    if f'_{in_suffix[0]}' not in in_res_file_name:
        suffix_str = "_".join(in_suffix)
        print_with_line_number(f'文件添加后缀为: {suffix_str}', __file__)
        in_tmp_file_name = in_res_file_name + f'_{suffix_str}' + in_res_file_extension
    else:
        in_tmp_file_name = in_res_file_name + in_res_file_extension

    # print_with_line_number(f'拷贝文件名：{in_tmp_file_name}', __file__)
    return in_tmp_file_name


def file_rename(in_file):
    # copy_output_file_to_dir()
    res_list = Common.split_path_get_list(os.path.dirname(in_file))
    print_with_line_number(f'返回的文件路径list：{res_list}', __file__)
    res_new_name = file_add_specified_suffix(in_file, res_list[-3], res_list[-2])
    print_with_line_number(f'原文件名：{in_file}', __file__)
    print_with_line_number(f'拷贝文件名：{res_new_name}', __file__)
    os.rename(in_file, res_new_name)
    return res_new_name


def copy_output_file_to_dir(in_char='4G'):
    # 找到output目录
    res_output_path_list = find_output_dir(data_path)
    # print_with_line_number(res_path_list, __file__)

    for i_output_path in res_output_path_list:
        print_with_line_number(f'当前查找文件的标志为：{in_char}', __file__)
        print_with_line_number(f'output路径：{i_output_path}', __file__)
        # 找到目录下的所有的包含4G 字段的文件
        in_res_list = get_file_list_by_char(i_output_path, in_char)
        if not in_res_list:
            print_with_line_number(f'output路径：{i_output_path} 没有包含：{in_char} 的文件', __file__)
            print_with_line_number(f'output路径：{i_output_path} 的文件列表为: {in_res_list}', __file__)
        for i_f in in_res_list:
            print('++' * 50)
            print_with_line_number(f'当前处理文件: {i_f}', __file__)
            # 文件重命名
            i_f = file_rename(i_f)
            # 根据新文件名拷贝文件
            copy_file(i_f, output_path)
            print_with_line_number(f'拷贝文件: {i_f} 输出目录：{output_path}', __file__)
        print('---' * 50)


# 找到目录下所有的output目录
def find_merge_file(in_path):
    output_directories = []

    # 遍历根目录及其子目录
    for in_res_folder_name, in_res_sub_folder, in_res_file_name in os.walk(in_path):
        # 检查当前目录是否包含 "output"
        print('in_res_folder_name', in_res_folder_name)
        print('in_res_sub_folder', in_res_sub_folder)
        print('in_res_file_name', in_res_file_name)
        if "merge" in in_res_file_name:
            print(in_res_file_name)
            # output_directories.append(os.path.join(in_res_folder_name, in_res_file_name))

    return output_directories


def copy_merge_file_to_dir(in_char='4G'):
    # 找到output目录
    res_output_path_list = find_merge_file(data_path)
    print_with_line_number(res_output_path_list, __file__)

    # for i_output_path in res_output_path_list:
    #     print_with_line_number(f'当前查找文件的标志为：{in_char}', __file__)
    #     print_with_line_number(f'output路径：{i_output_path}', __file__)
    #     # 找到目录下的所有的包含4G 字段的文件
    #     in_res_list = get_file_list_by_char(i_output_path, in_char)
    #     if not in_res_list:
    #         print_with_line_number(f'output路径：{i_output_path} 没有包含：{in_char} 的文件', __file__)
    #         print_with_line_number(f'output路径：{i_output_path} 的文件列表为: {in_res_list}', __file__)
    #     for i_f in in_res_list:
    #         print('++' * 50)
    #         print_with_line_number(f'当前处理文件: {i_f}', __file__)
    #         # 文件重命名
    #         i_f = file_rename(i_f)
    #         # 根据新文件名拷贝文件
    #         copy_file(i_f, output_path)
    #         print_with_line_number(f'拷贝文件: {i_f} 输出目录：{output_path}', __file__)
    #     print('---' * 50)


data_path = r'E:\work\MR_Data\1月15号\20240115数据'
output_path = data_path
copy_output_file_to_dir('4G')
copy_output_file_to_dir('5G')
# copy_merge_file_to_dir(data_path)