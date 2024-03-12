# -*- coding: utf-8 -*-
# 处理同秒数据到新列
from datetime import datetime

import numpy as np
import pandas as pd

from Common import df_write_to_csv, print_with_line_number

data_file = r'D:\MrData\3月4号_new\下午\NR_MR_Detail_20240309121859.csv'

deal_list = ['ARFCN', 'PCI', 'RSRP', 'RSRQ']

# 只读取指定列的数据
df = pd.read_csv(data_file, usecols=['UE Time', 'ARFCN', 'PCI', 'RSRP', 'RSRQ'])
# 去除空行
df = df.dropna(subset=deal_list, how='any')

# 根据时间 UETime 分组，逐行遍历并赋值到新列
for time, group in df.groupby("UE Time"):
    for index, row in group.iterrows():
        new_col_index = np.int64(index) - group.index[0]  # 计算新列的索引
        # print_with_line_number(new_col_index, __file__)
        if new_col_index > 0:
            for i_column in deal_list:
                new_col_name = f"{i_column}{new_col_index}"
                df.loc[group.index[0], new_col_name] = row[i_column]
            # 删除行
            if np.int64(index) != group.index[0]:
                df.drop(index=index, inplace=True)

# 列重命名
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

# 输出标准化
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


# 定义一个函数来将时间字符串转换为秒数的整数类型
def time_to_seconds(time_str):
    time_obj = datetime.strptime(time_str, "%M:%S.%f")
    seconds = (time_obj.minute * 60 + time_obj.second) + (time_obj.microsecond / 1000000)
    return int(seconds)


# 将 "UE Time" 列中的时间字符串转换为整数类型的秒数
df['Seconds'] = df['UE Time'].apply(time_to_seconds)

loop_value = df['Seconds'].iloc[0]
end_time = df['Seconds'].iloc[-1]

res_df = pd.DataFrame(columns=df.columns)

i_index = 0
row_data = df.iloc[0]

while True:
    if loop_value > end_time:
        break

    loop_value += 1
    # row_data = df.loc[0]
    i_index += 1
    print_with_line_number(f'loop_value: {loop_value}', __file__)
    if loop_value in df['Seconds'].values:
        print_with_line_number(f'{loop_value} 在数据中', __file__)
        # if df.loc[df['Seconds'] == loop_value].index.empty:
        res_index = df[df['Seconds'] == loop_value].index[0]
        print('res_index', res_index)
        # row_data = df.iloc[res_index]
        # row_data = df.iloc[df.loc[df['Seconds'] == loop_value].index[0]]

        # row_data = df.loc[i_index, 'UE Time']
    print_with_line_number(type(row_data), __file__)
    res_df.loc[i_index] = row_data
    res_df.loc[i_index, 'UE Time'] = datetime.fromtimestamp(loop_value).strftime('%M:%S.%f')
    print('---' * 50)
    # res_df = res_df.concat(row_data, ignore_index=True)

res_df = res_df.drop(columns='Seconds')
df_write_to_csv(df, r'D:\MrData\3月4号_new\下午\demo_res.csv')
