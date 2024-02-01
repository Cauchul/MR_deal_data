# -*- coding: utf-8 -*-
import pandas as pd

from Common import read_csv_get_df, df_write_to_csv

data_file = r'E:\work\MR_Data\new_format_data\小米13-1-5G--IN20240112-141903-FTPD(1)_0129091830.csv'

res_df = read_csv_get_df(data_file)

# 删除 C 列的最后一行
# res_df['PC Time'] = res_df['PC Time'].iloc[:-1]

# 删除 C 列的最后一个值
# res_df.iloc[-1, res_df.columns.get_loc('PC Time')] = pd.NA  # 将最后一个值设置为缺失值
# res_df.dropna(subset=['PC Time'], inplace=True)  # 删除包含缺失值的行

# mask = ('1970-01-01' in res_df['PC Time'])
# res_df = res_df[~mask]

res_df = res_df[~res_df['PC Time'].str.contains('1970-01-01')]

for i in res_df['PC Time']:
    if '1970-01-01' in i:
        print('hello')
    print(i)

df_write_to_csv(res_df, r'E:\work\MR_Data\new_format_data\demo.csv')
