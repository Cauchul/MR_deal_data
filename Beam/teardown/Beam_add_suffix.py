# -*- coding: utf-8 -*-
import os

from Common import find_output_dir, Common, get_file_list_by_char, print_with_line_number, read_csv_get_df, \
    df_write_to_csv, file_add_specified_suffix


def get_cur_dir_all_csv(in_src_data):
    tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if
                     file.endswith('.csv') and 'finger' in file]
    return tmp_csv_files


# # 处理当前路径下的文件
# data_path = r'E:\work\MR_Data\1月15号\20240115数据\4G'
# set_f_sid_value('4G')
# set_f_sid_value('5G')
# 获取所有output下的所有文件
def get_output_dir_csv(in_src_data):
    tmp_res_list = []
    in_output_dir_list = find_output_dir(in_src_data)
    for i_dir in in_output_dir_list:
        in_res_list = Common.list_files_in_directory(i_dir)
        tmp_res_list.extend(in_res_list)

    # 只获取finger文件
    # tmp_res_list = [x for x in tmp_res_list if 'finger' in x]
    return tmp_res_list


def file_rename(in_file, in_cnt=2):
    res_list = Common.split_path_get_list(os.path.dirname(in_file))
    print_with_line_number(f'返回的文件路径list：{res_list}', __file__)
    # res_new_name = file_add_specified_suffix(in_file, res_list[-3], res_list[-2])
    res_new_name = file_add_specified_suffix(in_file, res_list[-(in_cnt + 1):-1])
    print_with_line_number(f'原文件名：{in_file}', __file__)
    print_with_line_number(f'拷贝文件名：{res_new_name}', __file__)
    os.rename(in_file, res_new_name)
    return res_new_name


# 给文件名添加后缀
def file_del_suffix(in_file):
    in_res_file_name, in_res_file_extension = os.path.splitext(in_file)
    # print(in_res_file_name)
    # 寻找最后一个下划线的索引
    last_underscore_index = in_res_file_name.rfind('_')

    # 去除字符串中最后一个下划线及其后的内容
    in_tmp_file_name = in_res_file_name[:last_underscore_index]
    os.rename(in_file, in_tmp_file_name)
    # return in_tmp_file_name


# 设置pid要所有需要排序的文件一起设置
if __name__ == '__main__':
    folder_path = r'E:\work\MR_Data\1月18号\20180118_new_no_table\室外'
    # 获取当前路径下的所有csv文件
    # res_file_list = get_cur_dir_all_csv(folder_path)
    # 获取output目录下的所有的csv文件
    res_file_list = get_output_dir_csv(folder_path)

    for i_f in res_file_list:
        print(i_f)
        # 通过路径list，生成新名称，重新命名
        # res_list = Common.split_path_get_list(os.path.dirname(i_f))
        # print(res_list)
        # res_new_name = file_add_specified_suffix(i_f, res_list[-1])
        # 自定义后缀名
        res_new_name = file_add_specified_suffix(i_f, 'XIAOMI_13')
        os.rename(i_f, res_new_name)

        # 删除后缀
        # file_del_suffix(i_f)
