# -*- coding: utf-8 -*-

import pandas as pd

from Common import get_add_file_data, copy_file


# from Common import read_csv_get_df
#
# data_path = r'E:\work\mr_dea_data_c1\test_data\12月4号\C1\NR\8539\tmp_path\UE-F4-C1-5G-HUAWEI P40-8539-OUT20231204-143054-DouYin(1)_1204195549.csv'
# df = read_csv_get_df(data_path)
# print('所有列： ', df.columns)
#
# filename = r'E:\work\mr_dea_data_c1\test_data\12月4号\C1\NR\8539\tmp_path\NR_columns.txt'
# with open(filename, 'w') as file:
#     file.write('\n'.join(df.columns))
#
# print('列名已写入到文件', filename)
#
# data_path = r'E:\work\mr_dea_data_c1\test_data\12月4号\C1\NR\8539\tmp_path\UE-F4-C-4G-HUAWEI P40-2934-IN20231204-143047-DouYin(1)_1204195329.csv'
# df = read_csv_get_df(data_path)
# print('所有列： ', df.columns)
#
# filename = r'E:\work\mr_dea_data_c1\test_data\12月4号\C1\NR\8539\tmp_path\LTE_columns.txt'
# with open(filename, 'w') as file:
#     file.write('\n'.join(df.columns))

def copy_one_file_to_out():
    data = get_add_file_data()

    print('data: ', data.split('\n')[0])
    print('type: ', type(data))

    # 将合并后的结果保存为新的 CSV 文件
    # merged_csv.to_csv('merged_file.csv', index=False)

    copy_file(data.split('\n')[0], r'D:\working\merge\OUT')


def merge_v_h_data():
    data = get_add_file_data()

    print('data: ', data.split('\n')[0])
    print('data: ', data.split('\n')[2])
    print('data: ', data.split('\n')[3])
    print('type: ', type(data))

    csv1 = pd.read_csv(data.split('\n')[0])

    # 读取第二个 CSV 文件
    csv2 = pd.read_csv(data.split('\n')[2])

    # 合并两个 CSV 文件
    merged_csv = pd.concat([csv1, csv2], ignore_index=True)

    out_file = 'Merge_' + data.split('\n')[3].split('.')[0] + '_横纵.csv'
    # 将合并后的结果保存为新的 CSV 文件
    print('out_file: ', out_file)
    merged_csv.to_csv(r'D:\working\merge\OUT\{}'.format(out_file), index=False)


merge_v_h_data()

# data = get_add_file_data()
#
# print('data: ', data.split('\n')[0])
# print('data: ', data.split('\n')[2])
# print('data: ', data.split('\n')[3])
# print('type: ', type(data))
#
# tmp_out = data.split('\n')[0].split('\\')
#
# csv1 = pd.read_csv(data.split('\n')[0])
#
# # 读取第二个 CSV 文件
# csv2 = pd.read_csv(data.split('\n')[2])
#
# # 合并两个 CSV 文件
# merged_csv = pd.concat([csv1, csv2], ignore_index=True)
#
# out_file ='Merge_' + data.split('\n')[3].split('.')[0] + '_横纵.csv'
# # 将合并后的结果保存为新的 CSV 文件
# # merged_csv.to_csv('merged_file.csv', index=False)
# print('out_file: ', out_file)
# merged_csv.to_csv(r'D:\working\merge\OUT\{}'.format(out_file), index=False)
