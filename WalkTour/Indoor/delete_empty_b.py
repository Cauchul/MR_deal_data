# -*- coding: utf-8 -*-
import os.path

import pandas as pd
# 处理当前路径下的文件
src_data = r'E:\work\MR_Data\1月12号\下午测试'

csv_files = [file for file in os.listdir(src_data) if file.endswith('.csv')]

# print(csv_files)

# out_path = r'D:\working\merge\out_data'
#
# data_name = '5G_HaiDian_indoor_WeTest_LOG_DT_UE_0102_finger_merge_Reno8.csv'

# 读取CSV文件，指定第一行为标题行
for i_f in csv_files:
    print(os.path.join(src_data, i_f))
    df = pd.read_csv(os.path.join(src_data, i_f), header=0)

    # 删除第二列（下标为1）中为空的行，保留其他所有列
    if 'f_longitude' in df.columns:
        df = df.dropna(subset=['f_longitude', 'f_latitude'], how='any')
    else:
        df = df.dropna(subset=['u_longitude', 'u_latitude'], how='any')

    # 将结果写入到新的CSV文件中
    df.to_csv(os.path.join(src_data, i_f), index=False)

