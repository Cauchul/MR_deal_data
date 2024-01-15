# -*- coding: utf-8 -*-
import os

from Common import find_output_dir, Common, get_file_list_by_char, print_with_line_number, read_csv_get_df, \
    df_write_to_csv


# 修改f_sid值
# data_path = r'E:\work\MR_Data\1月12号\下午测试'


def set_f_sid_value(in_char='4G'):
    res_output_list = find_output_dir(data_path)

    print_with_line_number(f'当前查找文件的标志为：{in_char}', __file__)
    # print_with_line_number(res_output_list, __file__)
    for i_path in res_output_list:
        print_with_line_number(f'当前output路径: {i_path}', __file__)
        res_list = Common.split_path_get_list(os.path.dirname(i_path))
        print_with_line_number(f'路径list: {res_list}', __file__)
        res_csv_list = get_file_list_by_char(i_path, in_char)
        for i_f in res_csv_list:
            print_with_line_number(f'当前处理结果文件: {i_f}', __file__)
            res_df = read_csv_get_df(i_f)
            if 'f_sid' in res_df.columns:
                f_sid = int(res_list[-2])
                res_df['f_sid'] = f_sid
                # res_df['f_sid'] = 1
                print_with_line_number(f'当前 f_sid 设置的sid为: {f_sid}', __file__)
            else:
                u_sid = int(res_list[-2])
                res_df['u_sid'] = u_sid
                # res_df['u_sid'] = 1
                print_with_line_number(f'当前 u_sid 设置的sid为: {u_sid}', __file__)
            df_write_to_csv(res_df, i_f)
        print('---' * 50)


def set_f_sid_value_in_file_list(in_file_list):
    cnt = 0
    for i_path in in_file_list:
        cnt += 1
        print_with_line_number(f'当前处理结果文件: {i_path}', __file__)
        res_df = read_csv_get_df(i_path)
        if 'f_sid' in res_df.columns:
            f_sid = cnt
            res_df['f_sid'] = f_sid
            # res_df['f_sid'] = 1
            print_with_line_number(f'当前 f_sid 设置的sid为: {f_sid}', __file__)
        else:
            u_sid = cnt
            res_df['u_sid'] = u_sid
            # res_df['u_sid'] = 1
            print_with_line_number(f'当前 u_sid 设置的sid为: {u_sid}', __file__)
        df_write_to_csv(res_df, i_path)
        print('---' * 50)


def get_cur_dir_all_csv(in_src_data):
    tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if
                     file.endswith('.csv') and 'finger' in file]
    return tmp_csv_files


# # 处理当前路径下的文件
# data_path = r'E:\work\MR_Data\1月15号\20240115数据\4G'
# set_f_sid_value('4G')
# set_f_sid_value('5G')

def get_output_dir_csv(in_src_data):
    tmp_res_list = []
    in_output_dir_list = find_output_dir(in_src_data)
    for i_dir in in_output_dir_list:
        in_res_list = Common.list_files_in_directory(i_dir)
        tmp_res_list.extend(in_res_list)
    return tmp_res_list


if __name__ == '__main__':
    folder_path = r'E:\work\MR_Data\1月15号\20240115数据\5G'
    # 获取当前路径下的所有csv文件
    # res_file_list = get_cur_dir_all_csv(folder_path)
    # 获取所有的output
    # 获取output目录下的所有的csv文件
    res_file_list = get_output_dir_csv(folder_path)
    filtered_list = [x for x in res_file_list if 'finger' in x]
    print(filtered_list)
    set_f_sid_value_in_file_list(filtered_list)
    # for i_f in res_file_list:
    #     print_with_line_number(i_f, __file__)
