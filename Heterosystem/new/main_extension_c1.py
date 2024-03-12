# -*- coding: utf-8 -*-
import datetime

import numpy as np
import pandas as pd
from Common import df_write_to_csv, print_with_line_number


def convert_datetime_to_seconds(dtime):
    return [int((datetime.datetime.strptime(x, "%y-%m-%d %H:%M:%S.%f") - datetime.datetime(1970, 1, 1)).total_seconds())
            for x in dtime]


def data_line_extension(in_df):
    loop_value = in_df['f_time'][0]

    res_df = pd.DataFrame(columns=in_df.columns)

    i_index = old_index = 0
    res_df.loc[0] = in_df.loc[0]

    while True:
        if loop_value > in_df['f_time'].iloc[-1]:
            break

        loop_value += 1
        i_index += 1
        # print_with_line_number(f'loop_value: {loop_value}', __file__)
        if loop_value in in_df['f_time'].values:
            print_with_line_number(f'{loop_value} 在数据中', __file__)
            print('---' * 50)
            old_index += 1

        res_df.loc[i_index] = in_df.loc[old_index]
        res_df.loc[i_index, 'f_time'] = loop_value

    res_df = res_df.drop(columns='UE Time')
    return res_df


def data_column_extension(in_df):
    in_deal_list = ['ARFCN', 'PCI', 'RSRP', 'RSRQ']
    # in_df['UE Time'] = pd.to_datetime(in_df['UE Time'], format='%y-%m-%d %H:%M:%S.%f').dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    # 时间转时间戳
    in_df['f_time'] = convert_datetime_to_seconds(in_df['UE Time'])

    for i_time, group in in_df.groupby("f_time"):
        for index, row in group.iterrows():
            new_col_index = np.int64(index) - group.index[0]  # 计算新列的索引
            if new_col_index > 0:
                for i_column in in_deal_list:
                    new_col_name = f"{i_column}{new_col_index}"
                    in_df.loc[group.index[0], new_col_name] = row[i_column]
                # 删除行
                if np.int64(index) != group.index[0]:
                    in_df.drop(index=index, inplace=True)

    for i in in_df.columns:
        print_with_line_number(i, __file__)

    in_df = in_df.rename(
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
        if f'ARFCN{i}' in in_df.columns:
            in_df = in_df.rename(
                columns={
                    f'ARFCN{i}': f'f_freq_4g_n{i + 1}',
                    f'PCI{i}': f'f_pci_4g_n{i + 1}',
                    f'RSRP{i}': f'f_rsrp_4g_n{i + 1}',
                    f'RSRQ{i}': f'f_rsrq_4g_n{i + 1}',
                })
        else:
            break

    heterogeneous_system_data = ['UE Time', 'f_time', 'f_freq_4g_n1', 'f_pci_4g_n1', 'f_rsrp_4g_n1',
                                 'f_rsrq_4g_n1', 'f_freq_4g_n2', 'f_pci_4g_n2', 'f_rsrp_4g_n2',
                                 'f_rsrq_4g_n2', 'f_freq_4g_n3', 'f_pci_4g_n3', 'f_rsrp_4g_n3',
                                 'f_rsrq_4g_n3', 'f_freq_4g_n4', 'f_pci_4g_n4', 'f_rsrp_4g_n4',
                                 'f_rsrq_4g_n4', 'f_freq_4g_n5', 'f_pci_4g_n5', 'f_rsrp_4g_n5',
                                 'f_rsrq_4g_n5', 'f_freq_4g_n6', 'f_pci_4g_n6', 'f_rsrp_4g_n6',
                                 'f_rsrq_4g_n6', 'f_freq_4g_n7', 'f_pci_4g_n7', 'f_rsrp_4g_n7',
                                 'f_rsrq_4g_n7', 'f_freq_4g_n8', 'f_pci_4g_n8', 'f_rsrp_4g_n8',
                                 'f_rsrq_4g_n8']

    in_df = in_df.reindex(columns=heterogeneous_system_data)

    # 索引重置
    in_df.reset_index(drop=True, inplace=True)
    return in_df


def main(in_data_file):
    print_with_line_number(f'当前处理文件：{in_data_file}', __file__)
    # 数据读取和清理
    df = pd.read_csv(in_data_file, low_memory=False, usecols=['UE Time', 'ARFCN', 'PCI.1', 'RSRP.1', 'RSRQ.1'])
    df.rename(columns={'PCI.1': 'PCI', 'RSRP.1': 'RSRP', 'RSRQ.1': 'RSRQ', }, inplace=True)
    df = df.dropna(subset=['ARFCN', 'PCI', 'RSRP', 'RSRQ'], how='any').reset_index(drop=True)

    # 列拓展
    df = data_column_extension(df)

    # 列拓展
    df = data_line_extension(df)

    out_file = in_data_file.replace('.csv', '_extension_final_result.csv')
    print_with_line_number(f'输出文件：{out_file}', __file__)
    # 数据输出
    df_write_to_csv(df, out_file)


if __name__ == '__main__':
    data_file = r'D:\MrData\3月4号_new\NR_MR_Detail_20240311095550_demo.csv'
    main(data_file)
