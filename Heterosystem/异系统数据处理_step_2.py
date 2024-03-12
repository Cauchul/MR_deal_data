# -*- coding: utf-8 -*-
# 处理同秒数据到新列
import os.path

import numpy as np
import pandas as pd

from Common import df_write_to_csv

data_file = r'D:\MrData\3月4号_new\demo.csv'

read_list = ['UE Time', 'ARFCN', 'PCI', 'RSRP', 'RSRQ']
deal_list = ['ARFCN', 'PCI', 'RSRP', 'RSRQ']

df = pd.read_csv(data_file, usecols=read_list)

# 去除空行
df = df.dropna(subset=['ARFCN', 'PCI', 'RSRP', 'RSRQ'], how='any')

# 根据时间 UETime 分组，逐行遍历并赋值到新列
for time, group in df.groupby("UE Time"):
    for index, row in group.iterrows():
        new_col_index = np.int64(index) - group.index[0]  # 计算新列的索引
        # print('new_col_index', new_col_index)
        if new_col_index > 0:
            for i_column in deal_list:
                new_col_name = f"{i_column}{new_col_index}"
                df.loc[group.index[0], new_col_name] = row[i_column]
            # 删除行
            if np.int64(index) != group.index[0]:
                df.drop(index=index, inplace=True)

df = df.rename(
    columns={
        'ARFCN': 'f_freq_4g_n1',
        'PCI': 'f_pci_4g_n1',
        'RSRP': 'f_rsrp_4g_n1',
        'RSRQ': 'f_rsrq_4g_n1',
    })

# 列重命名
i = 0
while True:
    i += 1
    if f'ARFCN{i}' in df.columns:
        df = df.rename(
            columns={
                f'ARFCN{i}': f'f_freq_4g_n{i + 1}',
                f'PCI{i}': f'f_pci_4g_n{i + 1}',
                f'RSRP{i}': f'f_rsrp_4g_n{i + 1}',
                f'RSRQ{i}': f'f_rsrq_4g_n{i + 1}',
            })
    else:
        break

heterogeneous_system_data = ['UE Time', 'f_freq_4g_n1', 'f_pci_4g_n1', 'f_rsrp_4g_n1',
                             'f_rsrq_4g_n1', 'f_freq_4g_n2', 'f_pci_4g_n2', 'f_rsrp_4g_n2',
                             'f_rsrq_4g_n2', 'f_freq_4g_n3', 'f_pci_4g_n3', 'f_rsrp_4g_n3',
                             'f_rsrq_4g_n3', 'f_freq_4g_n4', 'f_pci_4g_n4', 'f_rsrp_4g_n4',
                             'f_rsrq_4g_n4', 'f_freq_4g_n5', 'f_pci_4g_n5', 'f_rsrp_4g_n5',
                             'f_rsrq_4g_n5', 'f_freq_4g_n6', 'f_pci_4g_n6', 'f_rsrp_4g_n6',
                             'f_rsrq_4g_n6', 'f_freq_4g_n7', 'f_pci_4g_n7', 'f_rsrp_4g_n7',
                             'f_rsrq_4g_n7', 'f_freq_4g_n8', 'f_pci_4g_n8', 'f_rsrp_4g_n8',
                             'f_rsrq_4g_n8']

df = df.reindex(columns=heterogeneous_system_data)

out_file = os.path.join(os.path.dirname(data_file), 'demo_out.csv')

df_write_to_csv(df, out_file)
