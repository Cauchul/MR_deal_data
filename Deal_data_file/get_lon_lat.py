# -*- coding: utf-8 -*-
import pandas as pd

from Common import read_csv_get_df, print_with_line_number


def lon_lat_interpolation(befor_time, befor_lon, befor_lat, after_time, after_lon, after_lat, want_time):
    all_time = float(after_time - befor_time)
    pre_lon = (after_lon - befor_lon) / all_time
    pre_lat = (after_lat - befor_lat) / all_time
    chane_time = float(want_time - befor_time)
    want_lon = befor_lon + pre_lon * chane_time
    want_lat = befor_lat + pre_lat * chane_time
    return want_lon, want_lat


def generate_lon_lat(in_time, in_df):
    in_df['时间差'] = abs(in_df['u_time'] - in_time)

    # 按照时间差列进行排序
    sorted_df = in_df.sort_values(by='时间差')

    # print(sorted_df)

    pre_flag = next_flag = False
    next_row = prev_row = None
    if sorted_df.iloc[0]['u_time'] > in_time:
        # print('获取到后面的时间')
        next_flag = True
        next_row = sorted_df.iloc[0]
    else:
        # print('获取到前面的时间')
        pre_flag = True
        prev_row = sorted_df.iloc[0]

    for index, row in sorted_df[1:].iterrows():

        if not pre_flag and row['u_time'] < in_time:
            # print('前面时间', row['u_time'])
            pre_flag = True
            prev_row = row

        if not next_flag and row['u_time'] > in_time:
            # print('后面时间', row['u_time'])
            next_flag = True
            next_row = row

        if pre_flag and next_flag:
            break

    res_lon, res_lat = lon_lat_interpolation(prev_row['u_time'], prev_row['predicted_lon'], prev_row['predicted_lat'],
                                             next_row['u_time'], next_row['predicted_lon'], next_row['predicted_lat'],
                                             in_time)

    print_with_line_number(f'u_time: {in_time} 生成的经纬度值：{res_lon}, {res_lat}', __file__)
    print('--' * 50)
    return res_lon, res_lat


if __name__ == '__main__':
    data_file = r'G:\数据整理\out\MATE40_4g_outdoor_20240411050651.xlsx'
    df = read_csv_get_df(data_file)
    generate_lon_lat(1712631054, df)
