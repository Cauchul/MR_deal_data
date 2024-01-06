import math
import os

import pandas as pd

from Common import get_all_data_path, get_file_by_string, copy_file, read_config_file, read_csv_get_df, data_conversion, \
    formatted_date, df_write_to_csv, generate_images
from unzip_get_char import unzip, list_files

data_path = r'D:\working\1206_国际财经中心测试V1\场景2\8539'


def unzip_char_zip_file(in_path):
    in_extraction_path = os.path.join(in_path, 'unzip')
    # print(i_path)
    # 解压所有的zip
    in_zip_file = get_file_by_string('zip', in_path)
    # print('zip_file: ', in_zip_file)
    # 调用函数解压ZIP文件到当前目录
    unzip(in_zip_file, in_extraction_path)


def copy_char_file_to_data_path(in_path):
    in_unzip_path = os.path.join(in_path, 'unzip')
    for root, dirs, files in os.walk(in_unzip_path):
        for file in files:
            if '-chart' in file or '_pci_' in file:
                file_path = os.path.join(root, file)
                print('file_path: ', file_path)
                if os.path.exists(os.path.join(in_path, file)):
                    os.remove(os.path.join(in_path, file))
                copy_file(file_path, in_path)


def deal_char_file(in_path):
    # 获取配置文件信息
    lon_O, lat_O, len_east_x, len_north_y = read_config_file(in_path)
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

    out_f = char_file.split(".")[0] + f'_{formatted_date}_xyToLonLat_ZCY.csv'
    df_write_to_csv(char_data, os.path.join(in_path, out_f))

    generate_images(res_x1_values, res_y1_values, lon, lat, in_path)


# 单独解压所有的char zip文件
def unzip_all_char_zip(in_path_list):
    for in_path in in_path_list:
        unzip_char_zip_file(in_path)


# 单独拷贝png和char文件到当前数据路径中
def copy_all_char(in_path_list):
    for in_path in in_path_list:
        copy_char_file_to_data_path(in_path)


# 解压，拷贝，然后处理char生成走测仪数据
def unzip_copy_and_deal_char(in_path_list):
    for in_path in in_path_list:
        unzip_char_zip_file(in_path)
        # 拷贝png和char文件到当前数据路径中
        # unzip_path = os.path.join(in_path, 'unzip')
        copy_char_file_to_data_path(in_path)
        # 处理char数据，生成走测仪数据
        deal_char_file(in_path)


# 获取所有的数据路径
d_path_list = get_all_data_path(data_path)

print('d_path_list: ', d_path_list)
# unzip_all_char_zip(d_path_list)
# copy_all_char(d_path_list)
unzip_copy_and_deal_char(d_path_list)

# 处理数据

