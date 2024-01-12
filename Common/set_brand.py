# -*- coding: utf-8 -*-
# 设置csv文件中的品牌名称
import os

from Common import read_csv_get_df, df_write_to_csv

src_data = r'D:\working\data_conv\室外1\小米13'

csv_files = [file for file in os.listdir(src_data) if file.endswith('.csv')]

for i_f in csv_files:
    cur_file = os.path.join(src_data, i_f)
    print(cur_file)
    des_df = read_csv_get_df(cur_file)
    des_df['f_device_brand'] = 'XIAOMI'
    des_df['f_device_model'] = '13'
    df_write_to_csv(des_df, cur_file)
