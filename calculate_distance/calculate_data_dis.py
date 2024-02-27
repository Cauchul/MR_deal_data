# -*- coding: utf-8 -*-
import os
from itertools import combinations

import pandas as pd
from geopy.distance import geodesic

from Common import df_write_to_csv, check_path


def calculate_distance(in_lat1, in_lon1, in_lat2, in_lon2):
    # 经纬度点1
    point1 = (in_lat1, in_lon1)

    # 经纬度点2
    point2 = (in_lat2, in_lon2)

    # 使用geopy库计算距离
    res_distance = geodesic(point1, point2).meters
    return res_distance


def in_df_calculate_dis(in_df_data):
    # tmp_dis_dict = {}
    max_dis = 0
    for pair in combinations(in_df_data, 2):
        coord_1, coord2 = pair
        res_dis = calculate_distance(coord_1[1], coord_1[0], coord2[1], coord2[0])
        # print('res_dis: ', res_dis)
        # if int(res_dis) > 5 or res_dis >= max_dis:
        if res_dis >= max_dis:
            max_dis = res_dis
            # print('res_dis: ', res_dis)
            # tmp_dis_dict[in_f_time] = max_dis

    return max_dis


def group_csv_data(in_csv_file, in_group_char):
    # 读取 CSV 文件
    in_df = pd.read_csv(in_csv_file)

    # 按照 'Category' 列的值进行分组
    in_grouped = in_df.groupby(in_group_char)

    res_dict = {}

    for i_group_name, i_group_data in in_grouped:
        # 提取分组后的df中的经纬度值
        in_df_data = i_group_data[['f_longitude', 'f_latitude']].values.tolist()
        print('f_time: ', i_group_name)
        in_res_dis = in_df_calculate_dis(in_df_data)
        print('max_dis: ', in_res_dis)
        print('--' * 15)
        # res_list.append(in_res_dis)
        res_dict[i_group_name] = in_res_dis
        # in_df['max_distance'] = in_res_dis

    in_df['max_distance'] = in_df['f_time'].map(res_dict)

    # 输出到当前目录
    # new_csv_file = in_csv_file.replace(".csv", "_add_nax_dis.csv")

    # 输出到output目录
    output_dir = os.path.join(os.path.dirname(in_csv_file), "max_dis_output")
    check_path(output_dir)
    tmp_csv_file = os.path.join(output_dir, os.path.basename(in_csv_file))
    new_csv_file = tmp_csv_file.replace(".csv", "_add_nax_dis.csv")

    df_write_to_csv(in_df, new_csv_file)


def get_cur_dir_all_csv(in_src_data):
    tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if
                     file.endswith('.csv') and 'finger' in file]
    return tmp_csv_files


if __name__ == '__main__':
    while True:
        in_data = input("输入文件目录或者文件名（绝对路径），输入out退出：")
        if 'out' == in_data.lower():
            break

        # 获取当前路径下的所有csv文件
        if os.path.isfile(in_data):
            group_csv_data(in_data, 'f_time')
        else:
            res_file_list = get_cur_dir_all_csv(in_data)
            print(res_file_list)
            for i_file in res_file_list:
                group_csv_data(i_file, 'f_time')
