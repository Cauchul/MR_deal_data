# -*- coding: utf-8 -*-
# 删除空行
import os.path

import pandas as pd

from Common import Common

src_data = r'D:\working\data_conv\out_path'
out_path = r'D:\working\merge\out_data'

data_name = '5G_HaiDian_indoor_WeTest_LOG_DT_UE_0102_finger_merge_Reno8.csv'

file_list = Common.list_files_in_directory(src_data)


for i_f in file_list:
    print('i_f: ', i_f)
    res_list = Common.split_path_get_list(i_f)
    out_file = os.path.join(out_path, res_list[-1])
    print('out_file: ', out_file)
    # 读取CSV文件，指定第一行为标题行
    df = pd.read_csv(i_f, header=0)

    # 删除第二列（下标为1）中为空的行，保留其他所有列
    df = df.dropna(subset=['f_longitude', 'f_latitude'], how='any')

    # 将结果写入到新的CSV文件中
    df.to_csv(out_file, index=False)

