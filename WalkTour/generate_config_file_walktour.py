# -*- coding: utf-8 -*-
# 根据目录下的文件，生成自己的配置文件
import os
import glob
import configparser

from Common import get_path_sub_dir, set_WalkTour_config_save, set_WeTest_config_save, config, print_with_line_number, \
    check_char_in_file_list


def generate_WalkTour_config_file(in_file_list, in_config_out_path, in_data_type='finger'):
    # 添加节和键值对
    section_name = 'WalkTour'
    in_config = config.get_config()
    # config.add_section(section_name)
    in_config.set(section_name, 'is_enabled', 'true')
    in_config.set(section_name, 'data_type', in_data_type)
    test_area = 'outdoor'
    # in_config.set(section_name, 'test_area', 'outdoor')

    for i_f in in_file_list:
        if 'char' in i_f:
            in_config.set(section_name, 'zcy_chart_file', i_f)
            test_area = 'indoor'
            print_with_line_number(f'char文件： {i_f}', __file__)
        elif 'table' in i_f:
            in_config.set(section_name, '45g_table_file', i_f)
            print_with_line_number(f'table文件： {i_f}', __file__)
        else:
            in_config.set(section_name, '45g_test_log', i_f)
            print_with_line_number(f'ue log 文件： {i_f}', __file__)

    in_config.set(section_name, 'test_area', test_area)
    print_with_line_number(f'生成 {section_name} {in_data_type} {test_area} 配置文件', __file__)
    print('--' * 50)
    # 将配置写入文件
    with open(os.path.join(in_config_out_path, f'WalkTour_{in_data_type}_config.ini'), 'w', encoding='UTF-8') as f:
        in_config.write(f)


def generate_WeTest_config_file(in_file_list, in_config_out_path, in_data_type='finger'):
    if len(in_file_list) > 2:
        print_with_line_number(f'error,当前处理为：WeTest，但文件列表长度为：{len(in_file_list)}, 文件列表：{in_file_list}',
                               __file__)
        print('--' * 50)
        return

    if check_char_in_file_list(in_file_list, 'table'):
        print_with_line_number(f'error,当前处理为：WeTest，但文件列表中存在 table文件, 文件列表：{in_file_list}', __file__)
        print('--' * 50)
        return

    # 添加节和键值对
    section_name = 'WeTest'
    in_config = config.get_config()
    # config.add_section(section_name)
    in_config.set(section_name, 'is_enabled', 'true')
    in_config.set(section_name, 'data_type', in_data_type)
    test_area = 'outdoor'
    # in_config.set(section_name, 'test_area', 'outdoor')

    for i_f in in_file_list:
        if 'char' in i_f:
            in_config.set(section_name, 'zcy_chart_file', i_f)
            test_area = 'indoor'
            print_with_line_number(f'char文件： {i_f}', __file__)
        # elif 'table' in i_f:
        #     # in_config.set(section_name, '45g_table_file', i_f)
        #     print_with_line_number(f'error,当前处理为：WeTest，但文件列表中存在 table文件： {i_f}', __file__)
        else:
            in_config.set(section_name, '45g_test_log', i_f)
            print_with_line_number(f'ue log 文件： {i_f}', __file__)

    in_config.set(section_name, 'test_area', test_area)
    print_with_line_number(f'生成 {section_name} {in_data_type} {test_area} 配置文件', __file__)
    print('--' * 50)
    # 将配置写入文件
    with open(os.path.join(in_config_out_path, f'WeTest_{in_data_type}_config.ini'), 'w', encoding='UTF-8') as f:
        in_config.write(f)


def generate_wifi_bluetooth_config_file(in_file_list, in_config_out_path, in_data_type='finger'):
    file_char_list = ['char', 'table', 'WiFi_BlueTooth']
    for i_char in file_char_list:
        # print(i_char)
        if not check_char_in_file_list(in_file_list, i_char):
            print_with_line_number(f'error {i_char} 文件不存在', __file__)
            print('--' * 50)
            return

    if len(in_file_list) != 4:
        print_with_line_number(
            f'error,当前处理为：WIFI_BlueTooth，但文件列表长度，实际值：{len(in_file_list)} 期望值：4, 文件列表：{in_file_list}',
            __file__)
        print('--' * 50)
        return

    # 添加节和键值对
    section_name = 'WIFI_BlueTooth'
    in_config = config.get_config()
    # config.add_section(section_name)
    in_config.set(section_name, 'is_enabled', 'true')
    in_config.set(section_name, 'data_type', in_data_type)

    for i_f in in_file_list:
        if 'char' in i_f:
            in_config.set(section_name, 'zcy_chart_file', i_f)
            print_with_line_number(f'char文件： {i_f}', __file__)
        elif 'table' in i_f:
            in_config.set(section_name, '45g_table_file', i_f)
            print_with_line_number(f'table文件： {i_f}', __file__)
        elif 'WiFi_BlueTooth' in i_f:
            in_config.set(section_name, 'wifi_bluetooth_file', i_f)
            print_with_line_number(f'WiFi_BlueTooth文件： {i_f}', __file__)
        else:
            in_config.set(section_name, '45g_test_log', i_f)
            print_with_line_number(f'ue log 文件： {i_f}', __file__)

    print_with_line_number(f'生成 {section_name} {in_data_type} 配置文件', __file__)
    print('--' * 50)
    # 将配置写入文件
    with open(os.path.join(in_config_out_path, f'WIFI_BlueTooth_{in_data_type}_config.ini'), 'w',
              encoding='UTF-8') as f:
        in_config.write(f)


def generate_UEMR_config_file(in_file_list, in_config_out_path, in_data_type='finger'):
    if 2 < len(in_file_list):
        print_with_line_number(
            f'error,当前处理为：WIFI_BlueTooth，但文件列表长度，实际值：{len(in_file_list)} 期望值： 小于3, 文件列表：{in_file_list}',
            __file__)
        print('--' * 50)
        return

    # 添加节和键值对
    section_name = 'DealUEMR'
    in_config = config.get_config()

    in_config.set(section_name, 'is_enabled', 'true')
    in_config.set(section_name, 'data_type', in_data_type)

    for i_f in in_file_list:
        if 'char' in i_f:
            in_config.set(section_name, 'zcy_chart_file', i_f)
            print_with_line_number(f'char文件： {i_f}', __file__)
        else:
            in_config.set(section_name, 'uemr_src_file', i_f)
            print_with_line_number(f'uemr_src_file 文件： {i_f}', __file__)

    print_with_line_number(f'生成 {section_name} {in_data_type} 配置文件', __file__)
    print('--' * 50)
    # 将配置写入文件
    with open(os.path.join(in_config_out_path, f'DealUEMR_{in_data_type}_config.ini'), 'w',
              encoding='UTF-8') as f:
        in_config.write(f)


def get_all_csv_file(in_path):
    # 使用 glob 获取文件列表
    tmp_file_list = glob.glob(os.path.join(in_path, "*.csv"))
    return tmp_file_list


# 修改为你想要获取文件的目录
folder_path = r'D:\working\data_conv\室外\iqoo7'

res_dir_list = get_path_sub_dir(folder_path)
print(res_dir_list)
for i_p in res_dir_list:
    sub_path = os.path.join(folder_path, i_p)
    print_with_line_number(f'当前处理目录为：{sub_path}', __file__)
    res_file_list = get_all_csv_file(sub_path)
    print_with_line_number(res_file_list, __file__)

    # 生成WalkTour配置文件，默认finger
    # generate_WalkTour_config_file(res_file_list, sub_path, 'finger')
    # generate_WalkTour_config_file(res_file_list, sub_path, 'uemr')
    # generate_WeTest_config_file(res_file_list, sub_path, 'finger')
    # generate_wifi_bluetooth_config_file(res_file_list, sub_path, 'finger')
    # generate_UEMR_config_file(res_file_list, sub_path, 'finger')

# 打印文件列表
# print(get_all_csv_file(folder_path))
