# -*- coding: utf-8 -*-
import datetime

import numpy as np
import pandas as pd

from Common import print_with_line_number, df_write_to_csv


def data_column_extension(in_df, in_group_flag, in_columns_list):
    # 按照列'A'的值进行分组
    data_list = []
    index_list = []
    for i_c, i_group in in_df.groupby(in_group_flag):
        new_data = {}
        cnt = 0
        # 遍历每一行数据
        for i_idx, i_data in i_group.iterrows():
            if cnt > 0:
                # print('i_data', i_data)
                for i_in_c in in_columns_list:
                    new_c = f'{i_in_c}{cnt}'
                    # in_df.loc[i_group.index[0], new_c] = i_data[i_in_c]
                    new_data[new_c] = i_data[i_in_c]
                in_df.drop(i_idx, inplace=True)
            cnt += 1
            if cnt > 8:
                break
        index_list.append(i_group.index[0])
        data_list.append(new_data)
        # print('--' * 50)
    in_df = pd.merge(in_df, pd.DataFrame(data_list, index=index_list), left_index=True, right_index=True)

    return in_df


def data_line_extension(in_df, in_columns_flag):
    new_df = pd.DataFrame(columns=in_df.columns)

    # 根据某一列进行行拓展
    start = in_df[in_columns_flag].iloc[0]
    cn_index = inter_circ_index = 0

    new_df.loc[cn_index] = in_df.iloc[inter_circ_index]
    new_df.loc[cn_index, in_columns_flag] = start

    while True:
        if start >= in_df[in_columns_flag].iloc[-1]:
            # print('start', start)
            break

        cn_index += 1
        start += 1

        if start in in_df[in_columns_flag].values:
            print_with_line_number(f'{start} 在数据中', __file__)
            print('---' * 50)
            inter_circ_index += 1

        new_df.loc[cn_index] = in_df.iloc[inter_circ_index]
        new_df.loc[cn_index, in_columns_flag] = start

    # print('--' * 50)
    return new_df


def convert_datetime_to_seconds(dtime):
    return [round(int(datetime.datetime.strptime(x, "%y-%m-%d %H:%M:%S.%f").timestamp() * 1e3) / 1e3) for x in dtime]
    # return [int((datetime.datetime.strptime(x, "%y-%m-%d %H:%M:%S.%f") - datetime.datetime(1970, 1, 1)).total_seconds())
    #         for x in dtime]


def nr_stand_df(in_df):
    in_df = in_df.rename(
        columns={
            'NARFCN': 'f_freq_5g_n1',
            'PCI': 'f_pci_5g_n1',
            'RSRP': 'f_rsrp_5g_n1',
            'RSRQ': 'f_rsrq_5g_n1',
            'SINR': 'f_sinr_5g_n1',
        })

    # 列重命名
    i = 0
    while True:
        i += 1
        if f'NARFCN{i}' in in_df.columns:
            in_df = in_df.rename(
                columns={
                    f'NARFCN{i}': f'f_freq_5g_n{i + 1}',
                    f'PCI{i}': f'f_pci_5g_n{i + 1}',
                    f'RSRP{i}': f'f_rsrp_5g_n{i + 1}',
                    f'RSRQ{i}': f'f_rsrq_5g_n{i + 1}',
                    f'SINR{i}': f'f_sinr_5g_n{i + 1}',
                })
        else:
            break

    heterogeneous_system_data = ['UE Time', 'f_time', 'f_freq_5g_n1', 'f_pci_5g_n1', 'f_rsrp_5g_n1', 'f_sinr_5g_n1',
                                 'f_rsrq_5g_n1', 'f_freq_5g_n2', 'f_pci_5g_n2', 'f_rsrp_5g_n2',
                                 'f_sinr_5g_n2', 'f_rsrq_5g_n2', 'f_freq_5g_n3', 'f_pci_5g_n3',
                                 'f_rsrp_5g_n3', 'f_sinr_5g_n3', 'f_rsrq_5g_n3', 'f_freq_5g_n4',
                                 'f_pci_5g_n4', 'f_rsrp_5g_n4', 'f_sinr_5g_n4', 'f_rsrq_5g_n4',
                                 'f_freq_5g_n5', 'f_pci_5g_n5', 'f_rsrp_5g_n5', 'f_sinr_5g_n5',
                                 'f_rsrq_5g_n5', 'f_freq_5g_n6', 'f_pci_5g_n6', 'f_rsrp_5g_n6',
                                 'f_sinr_5g_n6', 'f_rsrq_5g_n6', 'f_freq_5g_n7', 'f_pci_5g_n7',
                                 'f_rsrp_5g_n7', 'f_sinr_5g_n7', 'f_rsrq_5g_n7', 'f_freq_5g_n8',
                                 'f_pci_5g_n8', 'f_rsrp_5g_n8', 'f_sinr_5g_n8', 'f_rsrq_5g_n8']

    in_df = in_df.reindex(columns=heterogeneous_system_data)

    return in_df


def lte_stand_df(in_df):
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

    return in_df


def lte_extension(in_data_file):
    print_with_line_number(f'开始处理4G数据', __file__)
    # 数据读取和清理
    df = pd.read_csv(in_data_file, low_memory=False, usecols=['UE Time', 'ARFCN', 'PCI.1', 'RSRP.1', 'RSRQ.1'])
    df.rename(columns={'PCI.1': 'PCI', 'RSRP.1': 'RSRP', 'RSRQ.1': 'RSRQ', }, inplace=True)
    df = df.dropna(subset=['ARFCN', 'PCI', 'RSRP', 'RSRQ'], how='any').reset_index(drop=True)

    df['f_time'] = convert_datetime_to_seconds(df['UE Time'])
    columns_list = ['ARFCN', 'PCI', 'RSRP', 'RSRQ']
    # 列拓展
    df = data_column_extension(df, 'f_time', columns_list)

    # 列拓展
    df = data_line_extension(df, 'f_time')

    # 标准化输出
    df = lte_stand_df(df)

    # 删除列
    df = df.drop(columns='UE Time')

    return df

    # out_file = in_data_file.replace('.csv', '_extension_final_result_4G.csv')
    # print_with_line_number(f'输出文件：{out_file}', __file__)
    # # 数据输出
    # df_write_to_csv(df, out_file)


def nr_extension(in_data_file):
    print_with_line_number(f'开始处理5G数据', __file__)
    # 数据读取和清理
    df = pd.read_csv(in_data_file, low_memory=False, usecols=['UE Time', 'NARFCN', 'PCI', 'RSRP', 'RSRQ', 'SINR'])
    # df = df.dropna(subset=['NARFCN', 'PCI', 'RSRP', 'RSRQ'], how='any').reset_index(drop=True)

    df['f_time'] = convert_datetime_to_seconds(df['UE Time'])
    columns_list = ['NARFCN', 'PCI', 'RSRP', 'RSRQ', 'SINR']
    # 列拓展
    df = data_column_extension(df, 'f_time', columns_list)

    # 行拓展
    df = data_line_extension(df, 'f_time')

    # 标准化输出
    df = nr_stand_df(df)

    # 删除列
    df = df.drop(columns='UE Time')
    return df

    # out_file = in_data_file.replace('.csv', '_extension_final_result_5G.csv')
    # print_with_line_number(f'输出文件：{out_file}', __file__)
    # # 数据输出
    # df_write_to_csv(df, out_file)


if __name__ == '__main__':
    data_file = r'D:\MrData\0321\20240321\s22\NR_MR_Detail_20240321164150.csv'

    # res_df = pd.read_csv(data_file, low_memory=False)

    lte_res_df = lte_extension(data_file)
    nr_res_df = nr_extension(data_file)

    tmp_merger_df = pd.merge(lte_res_df, nr_res_df, left_on="f_time",
                             right_on="f_time")

    out_file = data_file.replace('.csv', '_hetero_sys_final_result_add_5G.csv')
    print_with_line_number(f'输出文件：{out_file}', __file__)
    df_write_to_csv(tmp_merger_df, out_file)
