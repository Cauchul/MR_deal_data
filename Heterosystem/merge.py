# -*- coding: utf-8 -*-
import os.path

import pandas as pd

from Common import read_csv_get_df, df_write_to_csv

# 读取df数据
finger_file = r'E:\work\MrData\data_place\out\上午\1\5G_HaiDian_indoor_WT_LOG_DT_UE_0304_finger_20240304_1.csv'
table_file = os.path.join(os.path.dirname(finger_file), 'demo_out_res.csv')

print(f'异系统数据文件： {table_file}')

finger_df = read_csv_get_df(finger_file)
table_df = read_csv_get_df(table_file)

tmp_merger_df = pd.merge(finger_df, table_df, left_on="f_time", right_on="f_time", how='left')
# tmp_merger_df = pd.merge(finger_df, table_df, how='left')

out_file = finger_file.replace('.csv', '_hetero_sys.csv')
# tmp_merger_df = tmp_merger_df.drop(columns='Seconds')
df_write_to_csv(tmp_merger_df, out_file)
