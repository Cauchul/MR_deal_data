# -*- coding: utf-8 -*-
import configparser

# 创建ConfigParser对象
# config = configparser.ConfigParser()
#
# config_file = r'E:\work\mr_dea_data_c2\deal_data_tool\demo_config.ini'
#
# # 读取INI文件
# # config.read(config_file, encoding='UTF-8')
# try:
#     config.read(config_file, encoding='GBK')
# except UnicodeDecodeError:
#     config.read(config_file, encoding='UTF-8')
#
# # 修改字段内容
# config.set('WalkTour', '45g_test_log', 'new_value')
# config.set('WalkTour', '45g_table_file', 'new_value')
# config.set('WalkTour', 'zcy_chart_file', 'new_value')
#
# # 保存修改后的INI文件
# with open(config_file, 'w') as configfile:
#     config.write(configfile)

# class DealConfig:
#     def __init__(self, in_config_file):
#         self.config_file = in_config_file
#         self.config = configparser.ConfigParser()
#         try:
#             self.config.read(self.config_file, encoding='GBK')
#         except UnicodeDecodeError:
#             self.config.read(self.config_file, encoding='UTF-8')
#
#     def set_config_table(self, in_section_name, in_value):
#         self.config.set(in_section_name, '45g_table_file', in_value)
#
#     def set_config_ue_log(self, in_section_name, in_value):
#         self.config.set(in_section_name, '45g_test_log', in_value)
#
#     def set_config_char(self, in_section_name, in_value):
#         self.config.set(in_section_name, 'zcy_chart_file', in_value)
#
#     def save_config(self):
#         with open(self.config_file, 'w') as configfile:
#             self.config.write(configfile)
#
#
# config_file = r'E:\work\mr_dea_data_c2\deal_data_tool\demo_config.ini'
# config = DealConfig(config_file)
#
# file_list = [
#     'D:\\working\\1月3号_数据\\2024-01-03中兴室内数据\\定位\\iqoo 7\\4\\4横1\\2楼_财经_2_4G_20240102采样点数据-chart.csv',
#     'D:\\working\\1月3号_数据\\2024-01-03中兴室内数据\\定位\\iqoo 7\\4\\4横1\\4横1-IN20240102-220430-DouYin(1)_0103092921.csv',
#     'D:\\working\\1月3号_数据\\2024-01-03中兴室内数据\\定位\\iqoo 7\\4\\4横1\\table_4横1-IN20240102-220430-DouYin(1)_0103092921.csv']
#
# def set_WalkTour_config(in_file_list):
#     for i_f in in_file_list:
#         if 'char' in i_f:
#             config.set_config_char('WalkTour', i_f)
#         elif 'table' in i_f:
#             config.set_config_table('WalkTour', i_f)
#         else:
#             config.set_config_ue_log('WalkTour', i_f)
#
#     config.save_config()

import configparser
import os

# 创建一个配置文件对象
# config = configparser.ConfigParser()
#
# # 添加节和键值对
# section_name = 'WalkTour'
#
# config.add_section(section_name)
# config.set(section_name, 'is_enabled', 'true')
# config.set(section_name, 'data_type', 'finger')
# config.set(section_name, 'test_area', 'indoor')
# config.set(section_name, '45g_test_log', 'value2')
# config.set(section_name, '45g_table_file', 'value2')
# config.set(section_name, 'zcy_chart_file', 'value2')
#
# # 将配置写入文件
# with open(r'D:\working\data_conv\out_path\config.ini', 'w') as f:
#     config.write(f)

file_list = [
    'D:\\working\\1月3号_数据\\2024-01-03中兴室内数据\\定位\\iqoo 7\\4\\4纵3\\2楼纵3_财经_2_4G_20240102采样点数据-chart.csv',
    'D:\\working\\1月3号_数据\\2024-01-03中兴室内数据\\定位\\iqoo 7\\4\\4纵3\\4纵3-IN20240102-222003-DouYin(1)_0103093158.csv',
    'D:\\working\\1月3号_数据\\2024-01-03中兴室内数据\\定位\\iqoo 7\\4\\4纵3\\table_4纵3-IN20240102-222003-DouYin(1)_0103093158.csv']


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
    with open(os.path.join(in_config_out_path, 'config.ini'), 'w') as f:
        in_config.write(f)


generate_WalkTour_config_file(file_list, )