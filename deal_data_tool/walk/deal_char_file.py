# -*- coding: utf-8 -*-

import configparser
import math
import os

from Common import get_file_by_string, read_csv_get_df, data_conversion, df_write_to_csv, generate_images


# 读取配置文件
def read_config_file():
    # in_con_file = os.path.join(in_config_path, 'config.ini')
    # in_con_file = os.path.join(r'D:\working\merge', 'config.ini')
    config = configparser.ConfigParser()
    # config.read(in_config_file, encoding='UTF-8')
    config.read(confile_file, encoding='GBK')
    in_lon_O = config.get('Coordinates', 'lon_O')
    in_lat_O = config.get('Coordinates', 'lat_O')
    in_len_east_x = config.get('Coordinates', 'len_east_x')
    in_len_north_y = config.get('Coordinates', 'len_north_y')
    return float(in_lon_O), float(in_lat_O), float(in_len_east_x), float(in_len_north_y)


def deal_char(in_path):
    # 获取配置文件信息
    lon_O, lat_O, len_east_x, len_north_y = read_config_file()
    # 处理char数据
    char_file = get_file_by_string('-chart', in_path)
    print('char_file: ', char_file)
    char_df = read_csv_get_df(char_file)

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

    out_f = char_file.split(".")[0] + f'_xyToLonLat_ZCY.csv'
    df_write_to_csv(char_data, os.path.join(in_path, out_f))

    generate_images(res_x1_values, res_y1_values, lon, lat, in_path, '_走侧仪')


if __name__ == '__main__':
    # 配置文件
    confile_file = r'D:\working\merge\config.ini'
    # char文件路径
    char_file_path = r'D:\working\1226\国际财经中心_5G_3'

    deal_char(char_file_path)
