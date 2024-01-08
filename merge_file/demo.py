# -*- coding: utf-8 -*-
# 置空某列数据
import os

from Common import read_csv_get_df, df_write_to_csv, df_write_to_xlsx

file_name = r'D:\working\data_conv\out_path\finger_zte_5g_samsung_20240102.csv'
char_df = read_csv_get_df(file_name)

char_df['f_longitude'] = None
char_df['f_latitude'] = None

out_path = r'D:\working\data_conv\out_path'

df_write_to_csv(char_df, file_name)
# df_write_to_xlsx(char_df, os.path.join(out_path, 'xiaomi_src.xlsx'))
