# -*- coding: utf-8 -*-
import os.path

import pandas as pd

from Common import df_write_to_csv, df_write_to_xlsx, read_csv_get_df
from Deal_data_file.get_lon_lat import generate_lon_lat


def get_time_list(in_file):
    in_tmp_list = []
    cnt = 0

    in_df = pd.read_excel(in_file, skiprows=4, sheet_name='测试记录表')

    in_df.columns = in_df.iloc[0]
    in_df = in_df[1:]

    for t_value in in_df.iloc[:, 1]:
        if not pd.isna(t_value):
            cnt += 1
            in_tmp_list.append(t_value)

    print(f'数据总条数为： {cnt}')
    return in_tmp_list, in_df


def get_lon_lat_in_uemr(in_df, in_time_list):
    in_res_dict = {}
    for in_i_time in in_time_list:
        filtered_row = in_df[in_df['u_time'] == in_i_time]
        if not filtered_row.empty:
            in_longitude = filtered_row['predicted_lon'].values[0]
            in_latitude = filtered_row['predicted_lat'].values[0]
            # print(f"时间戳为：{in_i_time} 经度值：{in_longitude} 纬度值：{in_latitude}")
            if pd.isna(in_longitude):
                in_longitude = 0
            if pd.isna(in_latitude):
                in_latitude = 0
            in_res_dict[in_i_time] = [in_longitude, in_latitude]
        else:
            # in_error_list.append(in_i_time)
            # res_lon, res_lat = generate_lon_lat(in_i_time, in_df)
            in_res_dict[in_i_time] = generate_lon_lat(in_i_time, in_df)
    return in_res_dict


def get_need_data_from_uemr(in_uemr_file, in_xls_file):
    # uemr_df = pd.read_csv(in_uemr_file)
    uemr_df = read_csv_get_df(in_uemr_file)

    res_time_list, out_df = get_time_list(in_xls_file)

    # 获取经纬度信息
    res_dict = get_lon_lat_in_uemr(uemr_df, res_time_list)

    for key_time_i in res_dict:
        # print(res_dict[key_time_i])
        out_df.loc[out_df['时间（精确到秒）'] == key_time_i, '定位结果横坐标'] = res_dict[key_time_i][0]
        out_df.loc[out_df['时间（精确到秒）'] == key_time_i, '定位结果纵坐标'] = res_dict[key_time_i][1]

    if 'csv' in in_uemr_file:
        out_file = in_uemr_file.replace('.csv', '_考核结果.csv')
        print(f'输出文件为： {out_file}')
        df_write_to_csv(out_df, out_file)
    else:
        out_file = in_uemr_file.replace('.xlsx', '_考核结果.xlsx')
        print(f'输出文件为： {out_file}')
        df_write_to_xlsx(out_df, out_file)


if __name__ == '__main__':
    xls_file = r'G:\数据整理\out\华为Mate40MR定位能力考核表_室外4G.xls'
    output_file = r'G:\数据整理\out\MATE40_4g_outdoor_20240411050651.xlsx'
    get_need_data_from_uemr(output_file, xls_file)
