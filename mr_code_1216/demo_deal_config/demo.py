# -*- coding: utf-8 -*-
import configparser
import os


def read_config_file(in_config_path=r'D:\working\merge'):
    # in_con_file = os.path.join(in_config_path, 'config.ini')
    in_con_file = os.path.join(r'E:\work\mr_dea_data_c2\mr_code_1216\demo_deal_config', 'config.ini')
    config = configparser.ConfigParser()
    # config.read(in_config_file, encoding='UTF-8')
    config.read(in_con_file, encoding='GBK')
    in_lon_O = config.get('Coordinates', 'lon_O')
    in_lat_O = config.get('Coordinates', 'lat_O')
    in_len_east_x = config.get('Coordinates', 'len_east_x')
    in_len_north_y = config.get('Coordinates', 'len_north_y')
    return float(in_lon_O), float(in_lat_O), float(in_len_east_x), float(in_len_north_y)