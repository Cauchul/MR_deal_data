# -*- coding: utf-8 -*-
import configparser

import chardet
import pandas as pd

config_file = r'E:\work\mr_dea_data_c2\mr_code_1216\demo_deal_config\config.ini'

demo_config = r'E:\work\mr_dea_data_c2\mr_code_1216\demo_deal_config\demo_config.ini'

config = configparser.ConfigParser()

with open(demo_config, 'rb') as f:
    result = chardet.detect(f.read())

if 'utf-8' in result['encoding']:
    print('UTF-8 config file')
    config.read(demo_config, encoding='UTF-8')
else:
    print('GBK config file')
    print(result['encoding'])
    config.read(demo_config, encoding='GBk')


def read_csv_get_df(in_df_path):
    in_df = pd.read_csv(in_df_path, low_memory=False)
    return in_df


# 走测仪数据转经纬度
def data_conversion(in_value, df_data):
    delta_d = df_data.max() - df_data.min()
    t_x = in_value / delta_d
    n_values = df_data * t_x
    res_dat = n_values - n_values.min()
    return res_dat


# 处理char文件
def deal_char(in_char_df):
    res_x1_values = data_conversion(len_east_x, char_df['x'])
    lon = res_x1_values / (111000 * math.cos(lon_O / 180 * math.pi)) + lon_O
    lon = 2 * max(lon) - lon

    res_y1_values = data_conversion(len_north_y, char_df['y'])
    lat = res_y1_values / 111000 + lat_O

    char_df['f_x'] = res_x1_values
    char_df['f_y'] = res_y1_values
    char_df['f_longitude'] = lon
    char_df['f_latitude'] = lat

    # 删除列
    columns_to_delete = ['map_width_pixel', 'map_height_pixel', 'map_width_cm', 'map_height_cm']
    char_data = char_df.drop(columns_to_delete, axis=1)


def read_config():
    WalkTour_flag = config.get('WalkTour', 'test_flag')
    WeTest_flag = config.get('WeTest', 'test_flag')
    wifi_bluetooth_flag = config.get('WIFI_BlueTooth', 'test_flag')

    if WalkTour_flag:
        ue_file = config.get('WalkTour', 'ue_file')
        table_file = config.get('WalkTour', 'table_file')
        char_file = config.get('WalkTour', 'char_file')

        ue_df = read_csv_get_df(ue_file)
        table_df = read_csv_get_df(table_file)
        char_df = read_csv_get_df(char_file)
        print(char_file)


read_config(demo_config)

# config = configparser.ConfigParser()
#
# with open(demo_config, 'rb') as f:
#     result = chardet.detect(f.read())
#
# if 'utf-8' in result['encoding']:
#     print('UTF-8 config file')
#     config.read(demo_config, encoding='UTF-8')
# elif 'GB' in result['encoding']:
#     print('GBK config file')
#     config.read(demo_config, encoding='GBK')
