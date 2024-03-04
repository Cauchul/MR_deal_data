# -*- coding: utf-8 -*-
import os.path

import pandas as pd

from Common import print_with_line_number, find_output_dir, get_data_path_by_char, Common, check_blank_line, \
    check_blank_line_in_df, read_csv_get_df


def check_column(in_f, in_df, in_column_list):
    print_with_line_number('in check_column', __file__)
    res_flag = False
    for i_col in in_column_list:
        # 检测文件
        if i_col in in_df.columns:
            threshold = len(in_df) / 3
            empty_rows_count = in_df[(in_df[i_col].isnull())].shape[0]
            if empty_rows_count > threshold:
                print_with_line_number(f'{i_col} 列，空行数： {empty_rows_count} 总行数： {len(in_df)}', __file__)
                error_records.append(
                    {"报错文件": in_f, "错误原因": f'{i_col} 列，空行数： {empty_rows_count} 总行数： {len(in_df)}'})
                res_flag = True
        #
        # if check_blank_line_in_df(in_df, i_col):
        #     print_with_line_number(f'文件 {in_f} 中的 {i_col} 列，空行数异常', __file__)
    return res_flag


def check_data_empty(in_check_file):
    # 读取CSV文件，指定第一行为标题行
    # df = pd.read_csv(in_check_file)
    print_with_line_number(f'当前检测文件： {in_check_file}', __file__)
    df = read_csv_get_df(in_check_file)

    # 删除第二列（下标为1）中为空的行，保留其他所有列
    if any('f_sinr' in col for col in df.keys()):
        print_with_line_number('5G 指纹', __file__)
        check_list = ['f_longitude', 'f_latitude', 'f_pci', 'f_freq', 'f_rsrp_n1', 'f_rsrq_n1', 'f_sinr_n1']
        check_column(in_check_file, df, check_list)
    elif any('u_sinr' in col for col in df.keys()):
        check_list = ['u_longitude', 'u_latitude', 'u_pci', 'u_freq', 'u_rsrp_n1', 'u_rsrq_n1', 'u_sinr_n1']
        check_column(in_check_file, df, check_list)
    elif 'f_longitude' in df.columns and 'f_sinr' not in df.columns:
        check_list = ['f_longitude', 'f_latitude', 'f_pci', 'f_freq', 'f_rsrp', 'f_rsrq']
        check_column(in_check_file, df, check_list)
    elif 'u_longitude' in df.columns and 'u_sinr' not in df.columns:
        check_list = ['u_longitude', 'u_latitude', 'u_pci', 'u_freq', 'u_rsrp', 'u_rsrq']
        check_column(in_check_file, df, check_list)
    print('---' * 50)

    if '_clear_empty' not in in_check_file:
        in_check_file = in_check_file.replace(".csv", "_clear_empty.csv")
    # 将结果写入到新的CSV文件中
    df.to_csv(os.path.join(src_data, in_check_file), index=False)


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


# 获取除output目录外的所有csv文件
def get_all_csv_files(directory):
    csv_files = []
    for root, dirs, i_files in os.walk(directory):
        if "output" in dirs:
            dirs.remove("output")  # 排除名为 "output" 的子目录
        for file in i_files:
            if file.endswith(".csv"):
                # csv_files.append(os.path.join(root, file))
                csv_files.append(root)
    return list(set(csv_files))


if __name__ == '__main__':
    error_records = []  # 用于存储报错的文件名和报错信息

    src_data = r'D:\MrData\2月27号\20240227'

    # 获取所有csv文件的目录
    res_list = get_all_csv_files(src_data)

    for i_dir in res_list:
        # 获取目录下的csv文件
        files = os.listdir(i_dir)
        # print(f'当前检查目录：{i_dir}')
        for i_file in files:
            if i_file.endswith('.csv'):
                check_file = os.path.join(i_dir, i_file)
                # print(check_file)
                check_data_empty(check_file)

    #
    # error_df = pd.DataFrame(error_records)
    # # 指定要保存到的 CSV 文件路径
    # output_csv_file = r"D:\empty_warning_files.csv"
    # # 将 DataFrame 写入 CSV 文件，不包含行索引
    # error_df.to_csv(output_csv_file, index=False)
