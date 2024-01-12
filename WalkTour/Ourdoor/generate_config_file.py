# -*- coding: utf-8 -*-
# 找到csv所在的目录
import os

from Common import get_all_data_path, config, print_with_line_number, get_all_csv_file

folder_path = r'D:\working\data_conv\室外'


# 室外数据，获取csv所在的目录，list做自动化处理
def outdoor_get_csv_file_dir_list(in_folder_path):
    in_res_list = get_all_data_path(in_folder_path, '.csv')
    # list 去重
    in_res_list = list(set(in_res_list))
    # print(in_res_list)
    # print(len(in_res_list))
    return in_res_list


def generate_WalkTour_config_file(in_file_list, in_config_out_path, in_data_type='finger'):
    """
    :param in_file_list: 目录下的所有csv文件的list
    :param in_config_out_path: config.ini文件的生成目录
    :param in_data_type: 需要生成那种数据类型的config文件
    :return:
    """
    # 添加节和键值对
    section_name = 'WalkTour'
    in_config = config.get_config()

    in_config.set(section_name, 'is_enabled', 'true')
    in_config.set(section_name, 'data_type', in_data_type)
    test_area = 'outdoor'

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
    # print('--' * 50)

    with open(os.path.join(in_config_out_path, f'WalkTour_{in_data_type}_config.ini'), 'w', encoding='UTF-8') as f:
        in_config.write(f)


# 获取csv文件的路径
res_csv_path_list = outdoor_get_csv_file_dir_list(folder_path)

# 遍历每一个csv路径
for i_dir in res_csv_path_list:
    print_with_line_number(f'当前数据路径：{i_dir}', __file__)
    # 获取路径下的所有的csv文件，list
    res_csv_file_list = get_all_csv_file(i_dir)
    # print_with_line_number(res_csv_file_list, __file__)
    # 生成指纹
    generate_WalkTour_config_file(res_csv_file_list, i_dir, 'finger')
    print('--' * 50)
    # 生成uemr
    generate_WalkTour_config_file(res_csv_file_list, i_dir, 'uemr')
    print('==' * 50)
