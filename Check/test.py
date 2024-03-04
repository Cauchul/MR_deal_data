# -*- coding: utf-8 -*-
import os.path

import pandas as pd

from Common import read_csv_get_df

# data_file = r'D:\MrData\2月28号\20240228\5G\WalkTour\IQOO7\1\iQOO7-5G--OUT20240228-113819-Ping(1)_0228160727.csv'


def check_netwalk_type(in_data_file):
    in_file_name = os.path.basename(in_data_file)
    if '5G' in in_file_name:
        in_f_net_type = 'NR'
    elif '4G' in in_file_name:
        in_f_net_type = 'LTE'
    else:
        in_f_net_type = ''

    # 读取文件中的network type；指读取前五行的数据
    in_res_df = pd.read_csv(in_data_file, nrows=5)
    in_net_type = in_res_df['Network Type'][0]

    if in_f_net_type.lower() == in_net_type.lower():
        return True
    else:
        return False
