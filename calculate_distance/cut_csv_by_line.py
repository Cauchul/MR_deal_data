# -*- coding: utf-8 -*-
import pandas as pd

from Common import df_write_to_csv

df = pd.read_csv(r'E:\work\MR_Data\data_place\demo\test.csv')

# 筛选出csv文件中的前一百行数据
df_res_data = df.head(100)

df_write_to_csv(df_res_data, r'E:\work\MR_Data\data_place\demo\out.csv')
