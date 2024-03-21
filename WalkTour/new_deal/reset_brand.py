# -*- coding: utf-8 -*-
# 设置csv文件中的品牌名称 # 处理当前路径下的文件
import os

from Common import read_csv_get_df, df_write_to_csv, print_with_line_number, Common
from WalkTour.new_deal.delete_empty_step_1_n1 import get_all_sub_dir


def get_cur_dir_all_csv(in_src_data):
    tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if file.endswith('.csv')]
    return tmp_csv_files


def set_brand(in_file_list):
    for cur_file in in_file_list:
        print_with_line_number(f'当前处理的文件为：{cur_file}', __file__)
        des_df = read_csv_get_df(cur_file)
        # f_device_brand = 'HUAWEI'
        # f_device_model = 'Mate40'
        print_with_line_number(f'设置的品牌：{f_device_brand}', __file__)
        print_with_line_number(f'设置的设备型号：{f_device_model}', __file__)
        if 'f_device_brand' in des_df.columns:
            print_with_line_number(f'finger文件：{cur_file}', __file__)
            des_df['f_device_brand'] = f_device_brand
            des_df['f_device_model'] = f_device_model
        else:
            print_with_line_number(f'UEMR文件：{cur_file}', __file__)
            des_df['u_device_brand'] = f_device_brand
            des_df['u_device_model'] = f_device_model
        df_write_to_csv(des_df, cur_file)
        print('---' * 50)


# 找到目录下所有的output目录
def find_output_dir(in_path):
    output_directories = []

    # 遍历根目录及其子目录
    for in_res_folder_name, in_res_sub_folder, in_res_file_name in os.walk(in_path):
        # 检查当前目录是否包含 "output"
        if "output" in in_res_sub_folder:
            output_directories.append(os.path.join(in_res_folder_name, "output"))

    return output_directories


def get_output_dir_csv(in_src_data):
    tmp_res_list = []
    in_output_dir_list = find_output_dir(in_src_data)
    for i_dir in in_output_dir_list:
        in_res_list = Common.list_files_in_directory(i_dir)
        tmp_res_list.extend(in_res_list)

    # 只获取finger文件
    # tmp_res_list = [x for x in tmp_res_list if 'finger' in x]
    return tmp_res_list


if __name__ == '__main__':
    src_data = r'E:\work\MrData\data_place\merge\5g'

    res_dir_list = get_all_sub_dir(src_data)
    for i_data_dir in res_dir_list:
        res_file_list = get_cur_dir_all_csv(i_data_dir)
        for i in res_file_list:
            print(i)
        if 'honor_90' in i_data_dir:
            f_device_brand = 'HONOR'
            f_device_model = '90'

        elif 'iqoo7' in i_data_dir:
            f_device_brand = 'VIVO'
            f_device_model = 'iQOO7'
        elif 'mate40' in i_data_dir:
            f_device_brand = 'HUAWEI'
            f_device_model = 'Mate40'
        elif 'P40' in i_data_dir:
            f_device_brand = 'HUAWEI'
            f_device_model = 'P40'
        elif 'reno8' in i_data_dir:
            f_device_brand = 'OPPO'
            f_device_model = 'Reno8'
        elif 's22' in i_data_dir:
            f_device_brand = 'SAMSUNG'
            f_device_model = 'S22'
        elif 'xiaomi_12' in i_data_dir:
            f_device_brand = 'XIAOMI'
            f_device_model = '12'
        else:
            f_device_brand = 'XIAOMI'
            f_device_model = '13'
        # elif 'xiaomi_13' in i_data_dir:
        #     f_device_brand = 'XIAOMI'
        #     f_device_model = '13'
        print('f_device_brand: ', f_device_brand)
        print('f_device_model: ', f_device_model)

        set_brand(res_file_list)
        print('--' * 50)

    # 获取当前目录
    # res_file_list = get_cur_dir_all_csv(src_data)
    # 获取output目录
    # res_file_list = get_output_dir_csv(src_data)
    # print(res_file_list)
    # set_brand(res_file_list)
