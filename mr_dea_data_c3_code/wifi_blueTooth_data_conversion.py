# -*- coding: utf-8 -*-

import configparser
import math
import os
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

from Common import *


def generate_wifi_bluetooth_data():
    # 读取配置文件值
    lon_O, lat_O, len_east_x, len_north_y = read_config_file(config_file)
    # 读取char数据
    in_wifi_bluetooth_data = pd.read_csv(os.path.join(bluetooth_path, wifi_bt_file_name))

    # 相对位置##x=10，y=47
    new_x1_values = data_conversion(len_east_x, in_wifi_bluetooth_data['f_x'])
    new_y1_values = data_conversion(len_north_y, in_wifi_bluetooth_data['f_y'])
    lon = new_x1_values / (111000 * math.cos(lon_O / 180 * math.pi)) + lon_O
    lon = 2 * max(lon) - lon
    lat = new_y1_values / 111000 + lat_O

    in_wifi_bluetooth_data['f_x'] = new_x1_values
    in_wifi_bluetooth_data['f_y'] = new_y1_values
    in_wifi_bluetooth_data['f_longitude'] = lon
    in_wifi_bluetooth_data['f_latitude'] = lat

    print('bluetooth_path: ', bluetooth_path)

    out_f = wifi_bt_file_name.split(".")[0] + '_{0}_xyToLonLat_WIFI_BlueTooth.csv'.format(
        datetime.now().strftime("%Y-%m-%d"))

    df_write_to_csv(in_wifi_bluetooth_data, os.path.join(bluetooth_path, out_f))

    generate_images(new_x1_values, new_y1_values, lon, lat, bluetooth_path)


if __name__ == '__main__':
    bluetooth_path = r'E:\work\mr_dea_data_c2\test_data\12月4号\C1\LTE\2934'
    print('zcy_char_path: ', bluetooth_path)

    wifi_bt_file_name = get_file_by_string('_WiFi_BlueTooth', bluetooth_path)
    config_file = os.path.join(bluetooth_path, 'config.ini')

    generate_wifi_bluetooth_data()

