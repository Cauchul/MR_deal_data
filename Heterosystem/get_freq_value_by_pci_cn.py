# -*- coding: utf-8 -*-

# 通过PCI，查找finger文件中对应的freq值

import numpy as np
import pandas as pd

from Common import print_with_line_number


def get_df_by_column_list(in_data_file, in_column_list):
    return pd.read_csv(in_data_file, usecols=in_column_list)


def get_freq_by_pci(in_data_file, in_pci):
    tmp_list = []

    df_columns = pd.read_csv(in_data_file, nrows=0)

    exchange_dict = {'f_pci': 'f_freq',
                     'f_pci_n1': 'f_freq_n1', 'f_pci_n2': 'f_freq_n2', 'f_pci_n3': 'f_freq_n3', 'f_pci_n4': 'f_freq_n4',
                     'f_pci_n5': 'f_freq_n5', 'f_pci_n6': 'f_freq_n6', 'f_pci_n7': 'f_freq_n7', 'f_pci_n8': 'f_freq_n8'}

    pci_columns = [col for col in df_columns if 'pci' in col]

    for i_pci_column in pci_columns:
        pci_data = get_df_by_column_list(in_data_file, [i_pci_column])
        # if np.isin(find_pci, data[i].values):
        #     print('成功')

        find_pci_arr = np.array([in_pci])
        # 找到pci，然后通过pci找到频点
        if np.isin(find_pci_arr, pci_data):
            # 如果找到了pci，然后就根据pci找到
            all_df = get_df_by_column_list(in_data_file, [i_pci_column, exchange_dict[i_pci_column]])
            pci_to_freq_dict = all_df.set_index(i_pci_column)[exchange_dict[i_pci_column]].to_dict()

            tmp_list.append(int(pci_to_freq_dict[in_pci]))
            # return int(pci_to_freq_dict[in_pci])
        else:
            print_with_line_number(f'未在 {i_pci_column} 列中找到 {in_pci}', __file__)

    return list(set(tmp_list))


if __name__ == '__main__':
    data_file = r'E:\work\MrData\1月24号\20240124_new_no_table\20240124\孙晨\IQOO7\4G\1\output\4G_HaiDian_indoor_WT_LOG_DT_UE_0124_finger_iQOO7_4G_1.csv'

    # 需要查询的pci值列表
    find_pci_list = [381, 382, 329]
    res_dict = {}

    for i_pci in find_pci_list:
        # 插叙当个pci值的freq值
        res_freq = get_freq_by_pci(data_file, i_pci)
        if res_freq:
            print_with_line_number(f'{i_pci}: {res_freq}', __file__)
            res_dict[i_pci] = res_freq

    print_with_line_number(f'{res_dict}', __file__)
