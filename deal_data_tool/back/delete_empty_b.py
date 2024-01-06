# -*- coding: utf-8 -*-
import os.path

import pandas as pd

src_data = r'D:\working\merge\OUT'
out_path = r'D:\working\merge\out_data'

data_name = '5G_HaiDian_indoor_WeTest_LOG_DT_UE_0102_finger_merge_Reno8.csv'

# 读取CSV文件，指定第一行为标题行
df = pd.read_csv(os.path.join(src_data, data_name), header=0)

# 删除第二列（下标为1）中为空的行，保留其他所有列
df = df.dropna(subset=['f_longitude', 'f_latitude'], how='any')

# 将结果写入到新的CSV文件中
df.to_csv(os.path.join(out_path, data_name), index=False)

