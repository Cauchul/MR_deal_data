# -*- coding: utf-8 -*-
# 数据转换，把数据的xy值转成经纬度信息

import configparser
import math
import os

from Common import read_csv_get_df, df_write_to_csv, Common, demo_data_conversion, \
    Data_4G, Data_5G, UMER_Data_4G, UEMR_Data_5G, print_with_line_number, df_write_to_xlsx


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
    print_with_line_number(f'当前处理文件为：{in_data_file}', __file__)
    # 获取配置文件信息
    lon_O, lat_O, len_east_x, len_north_y = read_config_file()

    char_df = read_csv_get_df(in_data_file)
    if 'u_x' in char_df.columns:
        print_with_line_number('----UEMR数据-----', __file__)

        lon = char_df['u_x'] / (111000 * math.cos(lon_O / 180 * math.pi)) + lon_O
        lon = 2 * max(lon) - lon

        lat = char_df['u_y'] / 111000 + lat_O

        char_df['u_longitude'] = lon
        char_df['u_latitude'] = lat

        if 'u_enb_id' in char_df.columns:
            # 4G
            char_df = char_df.reindex(columns=UMER_Data_4G)
        else:
            char_df = char_df.reindex(columns=UEMR_Data_5G)
    else:
        print_with_line_number('-----指纹数据-----', __file__)

        lon = char_df['f_x'] / (111000 * math.cos(lon_O / 180 * math.pi)) + lon_O
        lon = 2 * max(lon) - lon

        lat = char_df['f_y'] / 111000 + lat_O

        char_df['f_longitude'] = lon
        char_df['f_latitude'] = lat

        if 'f_enb_id' in char_df.columns:
            # 4G
            char_df = char_df.reindex(columns=Data_4G)
        else:
            char_df = char_df.reindex(columns=Data_5G)

    res_list = Common.split_path_get_list(in_data_file)
    out_f = res_list[-1].split(".")[0] + f'_xyToLonLat_ZCY_UX.csv'
    df_write_to_csv(char_df, os.path.join(in_out_path, out_f))
    # df_write_to_csv(char_df, os.path.join(in_out_path, out_f))
    print_with_line_number(f'输出文件为:{os.path.join(in_out_path, out_f)}', __file__)

    # 生成图片
    # generate_images(res_x1_values, res_y1_values, lon, lat, in_out_path, res_list[-1].split(".")[0])


if __name__ == '__main__':
    # 配置文件
    confile_file = r'D:\working\merge\config.ini'
    out_path = r'D:\working\data_conv\out_path'

    file_res_list = Common.list_files_in_directory(r'D:\working\data_conv\src_data')
    for i_f in file_res_list:
        # file_name = r'D:\working\data_conv\out_path\finger_zte_5g_samsung_20240102.csv'
        data_cover(out_path, i_f)
        # data_cover(out_path, i_f)
        # break
    # file_name = r'D:\working\data_conv\out_path\'
    # # file_name = r'D:\working\data_conv\out_path\finger_5g_xiaomi.csv'
    # data_cover(out_path, file_name)
