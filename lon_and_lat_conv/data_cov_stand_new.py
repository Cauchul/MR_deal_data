# -*- coding: utf-8 -*-
# 数据转换，把数据的xy值转成经纬度信息

import configparser
import math
import os

from Common import read_csv_get_df, data_conversion, df_write_to_csv, generate_images, Common, demo_data_conversion, \
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
        res_x1_values = data_conversion(len_east_x, char_df['u_x'])
        lon = res_x1_values / (111000 * math.cos(lat_O / 180 * math.pi)) + lon_O
        lon = 2 * max(lon) - lon

        res_y1_values = data_conversion(len_north_y, char_df['u_y'])
        lat = res_y1_values / 111000 + lat_O

        char_df['u_longitude'] = lon
        char_df['u_latitude'] = lat

        if 'u_enb_id' in char_df.columns:
            # 4G
            char_df = char_df.reindex(columns=UMER_Data_4G)
        else:
            char_df = char_df.reindex(columns=UEMR_Data_5G)
    else:
        print_with_line_number('-----指纹数据-----', __file__)
        res_x1_values = data_conversion(len_east_x, char_df['f_x'])
        lon = res_x1_values / (111000 * math.cos(lat_O / 180 * math.pi)) + lon_O
        lon = 2 * max(lon) - lon

        res_y1_values = data_conversion(len_north_y, char_df['f_y'])
        lat = res_y1_values / 111000 + lat_O

        char_df['f_longitude'] = lon
        char_df['f_latitude'] = lat

        if 'f_enb_id' in char_df.columns:
            # 4G
            char_df = char_df.reindex(columns=Data_4G)
        else:
            char_df = char_df.reindex(columns=Data_5G)

    res_list = Common.split_path_get_list(in_data_file)
    out_f = res_list[-1].split(".")[0] + f'_xyToLonLat_ZCY_stand.csv'
    df_write_to_csv(char_df, os.path.join(in_out_path, out_f))
    # if 'csv' in in_data_file:
    #     res_list = Common.split_path_get_list(in_data_file)
    #     out_f = res_list[-1].split(".")[0] + f'_xyToLonLat_ZCY_stand.csv'
    #     df_write_to_csv(char_df, os.path.join(in_out_path, out_f))
    # else:
    #     res_list = Common.split_path_get_list(in_data_file)
    #     out_f = res_list[-1].split(".")[0] + f'_xyToLonLat_ZCY_stand.xlsx'
    #     df_write_to_xlsx(char_df, os.path.join(in_out_path, out_f))

    print_with_line_number(f'输出文件为：{os.path.join(in_out_path, out_f)}', __file__)


def data_cover_new_formula(in_out_path, in_char):
    print_with_line_number(f'当前处理文件为：{in_char}', __file__)
    # 获取配置文件信息
    lon_O, lat_O, len_east_x, len_north_y = read_config_file()

    char_df = read_csv_get_df(in_char)

    # PixelWidth 屏幕的宽度（以像素为单位）
    # SamplingPointRadar.x和SamplingPointRadar.y 扫描点的坐标
    # BaseCoordinate.x和BaseCoordinate.y 基准坐标
    # Resolution.x和Resolution.y 分辨率

    x_width = 1200  # 屏幕宽
    y_height = 800  # 屏幕高

    # PixelWidth - ((SamplingPointRadar.x - BaseCoordinate.x) / Resolution.x)  # 求像素点
    # y_height - ((SamplingPointRadar.y - BaseCoordinate.y) / Resolution.y)

    if 'u_x' in char_df.columns:
        print_with_line_number('----UEMR数据-----', __file__)
        res_x1_values = data_conversion(len_east_x, char_df['u_x'])
        lon = res_x1_values / (111000 * math.cos(lon_O / 180 * math.pi)) + lon_O
        lon = 2 * max(lon) - lon

        res_y1_values = data_conversion(len_north_y, char_df['u_y'])
        lat = res_y1_values / 111000 + lat_O

        char_df['u_longitude'] = lon
        char_df['u_latitude'] = lat

        if 'u_enb_id' in char_df.columns:
            # 4G
            char_df = char_df.reindex(columns=UMER_Data_4G)
        else:
            char_df = char_df.reindex(columns=UEMR_Data_5G)
    else:
        print_with_line_number('-----指纹数据-----', __file__)
        res_x1_values = data_conversion(len_east_x, char_df['f_x'])
        lon = res_x1_values / (111000 * math.cos(lon_O / 180 * math.pi)) + lon_O
        lon = 2 * max(lon) - lon

        res_y1_values = data_conversion(len_north_y, char_df['f_y'])
        lat = res_y1_values / 111000 + lat_O

        char_df['f_longitude'] = lon
        char_df['f_latitude'] = lat

        if 'f_enb_id' in char_df.columns:
            # 4G
            char_df = char_df.reindex(columns=Data_4G)
        else:
            char_df = char_df.reindex(columns=Data_5G)

    if 'csv' in in_data_file:
        res_list = Common.split_path_get_list(in_data_file)
        out_f = res_list[-1].split(".")[0] + f'_xyToLonLat_ZCY_stand.csv'
        df_write_to_csv(char_df, os.path.join(in_out_path, out_f))
    else:
        res_list = Common.split_path_get_list(in_data_file)
        out_f = res_list[-1].split(".")[0] + f'_xyToLonLat_ZCY_stand.xlsx'
        df_write_to_xlsx(char_df, os.path.join(in_out_path, out_f))

    print_with_line_number(f'输出文件为：{os.path.join(in_out_path, out_f)}', __file__)


def data_cover_char(in_out_path, in_char):
    print_with_line_number(f'当前处理文件为：{in_char}', __file__)
    # 获取配置文件信息
    lon_O, lat_O, len_east_x, len_north_y = read_config_file()

    char_df = read_csv_get_df(in_char)

    print_with_line_number('----UEMR数据-----', __file__)
    res_x1_values = data_conversion(len_east_x, char_df['x'])
    lon = res_x1_values / (111000 * math.cos(lat_O / 180 * math.pi)) + lon_O
    lon = 2 * max(lon) - lon

    res_y1_values = data_conversion(len_north_y, char_df['y'])
    lat = res_y1_values / 111000 + lat_O

    char_df['longitude'] = lon
    char_df['latitude'] = lat

    res_list = Common.split_path_get_list(in_char)
    out_f = res_list[-1].split(".")[0] + f'_xyToLonLat_ZCY.csv'
    df_write_to_csv(char_df, os.path.join(in_out_path, out_f))

    print_with_line_number(f'输出文件为：{os.path.join(in_out_path, out_f)}', __file__)


if __name__ == '__main__':
    # 配置文件
    confile_file = r'D:\working\config.ini'
    out_path = r'D:\working\data_merge'

    # file_res_list = Common.list_files_in_directory(r'D:\working\data_conv\小米\小米13')
    # for i_f in file_res_list:
    #     # data_cover(out_path, i_f)
    #     # file_name = r'D:\working\data_conv\out_path\finger_5g_xiaomi.csv'
    #     file_name = r'D:\working\data_conv\out_path\finger_4g_xiaomi.csv'
    #     data_cover(out_path, file_name)
    #     break

    file_name = r'D:\working\data_merge\OPPOR85G_财经_2_5G_20240103采样点数据-chart.csv'
    data_cover_char(out_path, file_name)
