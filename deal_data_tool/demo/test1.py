# -*- coding: utf-8 -*-
from Common import read_csv_get_df, df_write_to_csv

src_data = r'D:\working\reno 8\5\v3\财经中心纵3_😄_2_5G_20240102采样点数据-chart.csv'
des_data = r'D:\working\2024-01-03中兴室内数据\定位\mate 40\5\5纵3\财经中心纵3_财经中心纵3_2_5G_20240102采样点数据-chart.csv'

src_df = read_csv_get_df(src_data)
des_df = read_csv_get_df(des_data)

des_df['created_by_ue_time'] = src_df['created_by_ue_time']


df_write_to_csv(des_df, des_data)