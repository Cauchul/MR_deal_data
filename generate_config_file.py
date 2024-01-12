# -*- coding: utf-8 -*-
# 根据目录下的文件，生成自己的配置文件
import os
import glob
import configparser

from Common import get_path_sub_dir, set_WalkTour_config_save, set_WeTest_config_save


def generate_WalkTour_config_file(in_file_list, in_config_out_path):
    in_config = configparser.ConfigParser()

    # 添加节和键值对
    section_name = 'WalkTour'

    in_config.add_section(section_name)
    in_config.set(section_name, 'is_enabled', 'true')
    in_config.set(section_name, 'data_type', 'finger')
    in_config.set(section_name, 'test_area', 'indoor')

    for i_f in in_file_list:
        if 'char' in i_f:
            in_config.set(section_name, 'zcy_chart_file', i_f)
        elif 'table' in i_f:
            in_config.set(section_name, '45g_table_file', i_f)
        else:
            in_config.set(section_name, '45g_test_log', 'value2')

    # 将配置写入文件
    with open(os.path.join(in_config_out_path, 'config.ini'), 'w', encoding='UTF-8') as f:
        in_config.write(f)


def get_all_csv_file(in_path):
    # 使用 glob 获取文件列表
    tmp_file_list = glob.glob(os.path.join(in_path, "*.csv"))
    return tmp_file_list


# 修改为你想要获取文件的目录
folder_path = r'D:\working\data_conv\4G输出\6'

res_dir_list = get_path_sub_dir(folder_path)

for i_p in res_dir_list:
    sub_path = os.path.join(folder_path, i_p)
    print(sub_path)
    res_file_list = get_all_csv_file(sub_path)
    print(res_file_list)
    if len(res_file_list) > 2:
        set_WalkTour_config_save(res_file_list, sub_path)
    else:
        set_WeTest_config_save(res_file_list, sub_path)

# 打印文件列表
# print(get_all_csv_file(folder_path))
