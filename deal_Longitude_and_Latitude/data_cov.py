# -*- coding: utf-8 -*-

import configparser
import math
import os

from Common import read_csv_get_df, data_conversion, df_write_to_csv, generate_images, Common, demo_data_conversion


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


def data_cover(in_out_path, in_data_file):
    # 获取配置文件信息
    lon_O, lat_O, len_east_x, len_north_y = read_config_file()

    print('in_data_file: ', in_data_file)
    char_df = read_csv_get_df(in_data_file)

    res_x1_values = data_conversion(len_east_x, char_df['f_x'])
    lon = res_x1_values / (111000 * math.cos(lon_O / 180 * math.pi)) + lon_O
    lon = 2 * max(lon) - lon

    res_y1_values = data_conversion(len_north_y, char_df['f_y'])
    lat = res_y1_values / 111000 + lat_O

    # char_df['f_x'] = res_x1_values
    # char_df['f_y'] = res_y1_values
    char_df['f_longitude'] = lon
    char_df['f_latitude'] = lat

    res_list = Common.split_path_get_list(in_data_file)
    print('res_list: ', res_list)

    out_f = res_list[-1].split(".")[0] + f'_xyToLonLat_ZCY.csv'
    df_write_to_csv(char_df, os.path.join(in_out_path, out_f))

    # generate_images(res_x1_values, res_y1_values, lon, lat, in_out_path, res_list[-1].split(".")[0])


if __name__ == '__main__':
    # 配置文件
    confile_file = r'D:\working\merge\config.ini'
    out_path = r'D:\working\data_conv\out_path'

    file_res_list = Common.list_files_in_directory(r'D:\working\data_conv\试卷\OPPOreno8')
    for i_f in file_res_list:
        data_cover(out_path, i_f)
