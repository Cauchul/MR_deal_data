# -*- coding: utf-8 -*-

import configparser
import math
import os
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

from Common import *


# 读取配置文件中的数据
# def read_config_file(in_config_file):
#     config = configparser.ConfigParser()
#     # config.read(in_config_file, encoding='UTF-8')
#     config.read(in_config_file, encoding='GBK')
#     in_lon_O = config.get('Coordinates', 'lon_O')
#     in_lat_O = config.get('Coordinates', 'lat_O')
#     in_len_east_x = config.get('Coordinates', 'len_east_x')
#     in_len_north_y = config.get('Coordinates', 'len_north_y')
#     return in_lon_O, in_lat_O, in_len_east_x, in_len_north_y


# # 走测仪数据转经纬度
# def data_conversion(in_value, df_data):
#     delta_d = df_data.max() - df_data.min()
#     t_x = in_value / delta_d
#     n_values = df_data * t_x
#     res_dat = n_values - n_values.min()
#     return res_dat


# def generate_images(new_x1_values, new_y1_values, lon, lat):
#     plt.rcParams['axes.unicode_minus'] = False
#     plt.rcParams['font.sans-serif'] = 'SimHei'
#     # Mac系统字体
#     # plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
#     plt.subplot(2, 1, 1)
#     plt.plot(new_x1_values, new_y1_values)
#     plt.gca().set_aspect('equal', adjustable='box')
#     plt.xlabel('X')
#     plt.ylabel('Y')
#     plt.title('xy_相对位置轨迹图')
#     png_file = os.path.join(zcy_char_path, 'xy_相对位置轨迹图.png')
#     plt.savefig(png_file)
#
#     # 清除图形
#     plt.clf()
#
#     # plt.savefig(png_file)
#     plt.subplot(2, 1, 2)
#     plt.plot(lon, lat)
#     plt.gca().set_aspect('equal', adjustable='box')
#     plt.xlabel('lon')
#     plt.ylabel('lat')
#     plt.title('lonlat_经纬度轨迹图')
#     plt.tight_layout()
#     # plt.show()
#     png_file = os.path.join(zcy_char_path, 'lonlat_经纬度轨迹图.png')
#     plt.savefig(png_file)


def generate_zcy_data():
    # 读取配置文件值
    lon_O, lat_O, len_east_x, len_north_y = read_config_file(config_file)
    lon_O = float(lon_O)
    lat_O = float(lat_O)
    len_east_x = float(len_east_x)
    len_north_y = float(len_north_y)
    print('lon_O: ', lon_O)
    print('lat_O: ', lat_O)
    print('len_east_x: ', len_east_x)
    print('len_north_y: ', len_north_y)
    # 读取char数据
    char_data = pd.read_csv(os.path.join(zcy_char_path, wifi_bt_file_name))

    # 相对位置##x=10，y=47
    new_x1_values = data_conversion(len_east_x, char_data['f_x'])
    new_y1_values = data_conversion(len_north_y, char_data['f_y'])
    lon = new_x1_values / (111000 * math.cos(lon_O / 180 * math.pi)) + lon_O
    lon = 2 * max(lon) - lon
    lat = new_y1_values / 111000 + lat_O

    char_data['f_x'] = new_x1_values
    char_data['f_y'] = new_y1_values
    char_data['f_longitude'] = lon
    char_data['f_latitude'] = lat

    # 删除列
    # columns_to_delete = ['map_width_pixel', 'map_height_pixel', 'map_width_cm', 'map_height_cm']
    # char_data = char_data.drop(columns_to_delete, axis=1)

    print('zcy_char_path: ', zcy_char_path)

    # out_f = wifi_bt_file_name.split(".")[0] + '_{0}_xyToLonLat_ZCY.csv'.format(
    #     datetime.now().strftime("%Y-%m-%d"))
    out_f = wifi_bt_file_name.split(".")[0] + '_{0}_xyToLonLat_WIFI_BlueTooth.csv'.format(
        datetime.now().strftime("%Y-%m-%d"))

    df_write_to_csv(char_data, os.path.join(zcy_char_path, out_f))

    # char_data.to_csv(
    #     os.path.join(out_path, zcy_char_file_name.split(".")[0] + '_{0}_xyToLonLat_ZCY.csv'.format(
    #         datetime.now().strftime("%Y-%m-%d"))), encoding='gbk', index=False)

    generate_images(new_x1_values, new_y1_values, lon, lat, zcy_char_path)


def generate_zcy_data_in_(in_show_flag, in_config_file, in_zcy_char_file):
    # 读取配置文件值
    lon_O, lat_O, len_east_x, len_north_y = read_config_file(in_config_file)
    lon_O = float(lon_O)
    lat_O = float(lat_O)
    len_east_x = int(len_east_x)
    len_north_y = int(len_north_y)
    print('lon_O: ', lon_O)
    print('lat_O: ', lat_O)
    print('len_east_x: ', len_east_x)
    print('len_north_y: ', len_north_y)
    # 读取char数据
    char_data = pd.read_csv(in_zcy_char_file)

    # 相对位置##x=10，y=47
    new_x1_values = data_conversion(len_east_x, char_data['x'])
    new_y1_values = data_conversion(len_north_y, char_data['y'])
    lon = new_x1_values / (111000 * math.cos(lon_O / 180 * math.pi)) + lon_O
    lon = 2 * max(lon) - lon
    lat = new_y1_values / 111000 + lat_O

    char_data['f_x'] = new_x1_values
    char_data['f_y'] = new_y1_values
    char_data['f_longitude'] = lon
    char_data['f_latitude'] = lat

    if in_show_flag:
        generate_images(new_x1_values, new_y1_values, lon, lat)
    # 删除列
    columns_to_delete = ['map_width_pixel', 'map_height_pixel', 'map_width_cm', 'map_height_cm']
    char_data = char_data.drop(columns_to_delete, axis=1)

    char_data.to_csv(
        os.path.join(zcy_char_path, zcy_char_file_name.split(".")[0] + '_{0}_xyToLonLat_ZCY.csv'.format(
            datetime.now().strftime("%Y-%m-%d"))), encoding='gbk', index=False)


if __name__ == '__main__':
    zcy_char_path = r'D:\working\merge\wifi_data'
    wifi_f_list = list_files_in_directory(zcy_char_path)
    config_file = os.path.join(zcy_char_path, 'config.ini')

    for wifi_f_v in wifi_f_list:
        print(wifi_f_v)
        wifi_bt_file_name = wifi_f_v
        generate_zcy_data()


 