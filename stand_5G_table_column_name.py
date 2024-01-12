# -*- coding: utf-8 -*-
# 标准化5G table表数据的列名称
from Common import read_csv_get_df, df_write_to_csv

# src_file = r'D:\working\data_conv\20240110\4\P40_2934\table_P40-1--IN20240110-143249-FTPD(1)_0110150942.csv'
des_file = r'D:\working\data_conv\20240110\6\P40_5864\table_P40-2--IN20240110-143248-FTPD(1)_0110150951.csv'
# src_df = read_csv_get_df(src_file)
des_df = read_csv_get_df(des_file)

# print(src_df.columns)
print(des_df.columns)

columns_list = ['No.', 'UETime', 'PCTime', 'UTCTime', 'Lon', 'Lat', 'SS-RSRP', 'IMEI',
                'IMSI', 'NR C-RNTI', 'NCI', 'NR gNodeB ID(24bit)']

# 使用reindex()方法添加列名到DataFrame
des_df = des_df.reindex(columns=columns_list)

# 打印DataFrame
print(des_df.columns)
df_write_to_csv(des_df, des_file)
