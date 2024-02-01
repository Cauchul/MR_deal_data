# -*- coding: utf-8 -*-
import os.path

import pandas as pd

from Common import print_with_line_number, find_output_dir, get_data_path_by_char, Common


def delete_empty_value_column(in_csv_file_list):
    # 读取CSV文件，指定第一行为标题行
    for i_f in in_csv_file_list:
        df = pd.read_csv(os.path.join(src_data, i_f), header=0)

        # 删除第二列（下标为1）中为空的行，保留其他所有列
        if 'f_server_sid' in df.columns:
            print_with_line_number(f'当前清理空行文件：{os.path.join(src_data, i_f)}', __file__)
            df = df.dropna(subset=['f_longitude', 'f_latitude'], how='any')
            print('---' * 50)
            # 遍历每一行数据
            for index, row in df.iterrows():
                # 获取 f_server_sid 列的值
                server_sid = int(row['f_server_sid'])

                print('server_sid: ', server_sid)

                # 检查 f_sid_1_rsrp 和 f_sid_1_rsrq 是否为空
                if pd.isna(row[f'f_sid_{server_sid}_rsrp']) or pd.isna(row[f'f_sid_{server_sid}_rsrq']):
                    # 如果任一为空，就删除这一行数据
                    df.drop(index, inplace=True)
        elif 'u_server_sid' in df.columns:
            print_with_line_number(f'当前清理空行文件：{os.path.join(src_data, i_f)}', __file__)
            df = df.dropna(subset=['u_longitude', 'u_latitude'], how='any')
            print('---' * 50)
            # 遍历每一行数据
            for index, row in df.iterrows():
                # 获取 f_server_sid 列的值
                server_sid = int(row['u_server_sid'])

                # print('server_sid: ', server_sid)

                # 检查 f_sid_1_rsrp 和 f_sid_1_rsrq 是否为空
                if pd.isna(row[f'u_sid_{server_sid}_rsrp']) or pd.isna(row[f'u_sid_{server_sid}_rsrq']) or pd.isna(row[f'u_sid_{server_sid}_sinr']):
                    # 如果任一为空，就删除这一行数据
                    df.drop(index, inplace=True)
        else:
            print_with_line_number(f'不是Beam文件不处理：{os.path.join(src_data, i_f)}', __file__)

        # 将结果写入到新的CSV文件中
        df.to_csv(os.path.join(src_data, i_f), index=False)


def get_cur_dir_all_csv(in_src_data):
    tmp_csv_files = [file for file in os.listdir(in_src_data) if file.endswith('.csv')]
    return tmp_csv_files


def get_output_dir_csv(in_src_data):
    tmp_res_list = []
    in_output_dir_list = find_output_dir(in_src_data)
    for i_dir in in_output_dir_list:
        in_res_list = Common.list_files_in_directory(i_dir)
        tmp_res_list.extend(in_res_list)
    return tmp_res_list


if __name__ == '__main__':
    src_data = r'E:\work\MR_Data\1月16号\20240116_new_no_table\20240116\5G'
    # 获取当前目录下的所有的csv文件
    # res_file_list = get_cur_dir_all_csv(src_data)
    # 获取output目录下的所有的csv文件
    res_file_list = get_output_dir_csv(src_data)
    delete_empty_value_column(res_file_list)
