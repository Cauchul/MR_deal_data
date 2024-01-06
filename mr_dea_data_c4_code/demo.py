# -*- coding: utf-8 -*-
import os.path
from datetime import datetime

from Common import check_file_exists

# from Common import merge_mult_csv_file
#
file_1 = 'E:\work\demo_merge\output\LTE_HaiDian_Indoor_WT_DT_UE1_1204_12月4号_G19_LTE_WT_LOG_DT_UE_2934v.csv'
# file_2 = 'E:\\work\\demo_merge\\output\\LTE_HaiDian_Indoor_WT_DT_UE1_1206_场景2_8539_LTE_WT_LOG_DT_UE_V1.csv'
# # 字符串表示的日期时间
# df = merge_mult_csv_file(file_1)
# print(df)

# p_list = ['working', '12月4号', 'G19', 'NR', '8539v']

if check_file_exists(file_1):
    print('存在')
else:
    print('不存在')
