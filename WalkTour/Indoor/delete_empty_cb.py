# -*- coding: utf-8 -*-
import os.path

import pandas as pd

from Common import print_with_line_number

# 处理当前路径下的文件
# src_data = r'E:\work\MR_Data\1月12号\下午测试'


# csv_files = [file for file in os.listdir(src_data) if file.endswith('.csv')]
#
# # print(csv_files)
#
# # out_path = r'D:\working\merge\out_data'
# #
# # data_name = '5G_HaiDian_indoor_WeTest_LOG_DT_UE_0102_finger_merge_Reno8.csv'
#
# # 读取CSV文件，指定第一行为标题行
# for i_f in csv_files:
#     print_with_line_number(f'当前处理文件：{os.path.join(src_data, i_f)}', __file__)
#     df = pd.read_csv(os.path.join(src_data, i_f), header=0)
#
#     # 删除第二列（下标为1）中为空的行，保留其他所有列
#     if 'f_sinr' in df.columns:
#         print_with_line_number(f'5G finger 数据', __file__)
#         df = df.dropna(subset=['f_longitude', 'f_latitude', 'f_pci', 'f_freq', 'f_rsrp', 'f_rsrq', 'f_sinr'], how='any')
#     elif 'u_sinr' in df.columns:
#         print_with_line_number(f'5G UEMR 数据', __file__)
#         df = df.dropna(subset=['u_longitude', 'u_latitude', 'u_pci', 'u_freq', 'u_rsrp', 'u_rsrq', 'u_sinr'], how='any')
#     elif 'f_longitude' in df.columns and 'f_sinr' not in df.columns:
#         print_with_line_number(f'4G finger 数据', __file__)
#         df = df.dropna(subset=['f_longitude', 'f_latitude', 'f_pci', 'f_freq', 'f_rsrp', 'f_rsrq'], how='any')
#     elif 'u_longitude' in df.columns and 'u_sinr' not in df.columns:
#         print_with_line_number(f'4G UEMR 数据', __file__)
#         df = df.dropna(subset=['u_longitude', 'u_latitude', 'u_pci', 'u_freq', 'u_rsrp', 'u_rsrq'], how='any')
#     print('---' * 50)
#
#     # 将结果写入到新的CSV文件中
#     df.to_csv(os.path.join(src_data, i_f), index=False)

def delete_empty_value_column():
    csv_files = [file for file in os.listdir(src_data) if file.endswith('.csv')]
    # 读取CSV文件，指定第一行为标题行
    for i_f in csv_files:
        print_with_line_number(f'当前处理文件：{os.path.join(src_data, i_f)}', __file__)
        df = pd.read_csv(os.path.join(src_data, i_f), header=0)

        # 删除第二列（下标为1）中为空的行，保留其他所有列
        if 'f_sinr' in df.columns:
            print_with_line_number(f'5G finger 数据', __file__)
            df = df.dropna(subset=['f_longitude', 'f_latitude', 'f_pci', 'f_freq', 'f_rsrp', 'f_rsrq', 'f_sinr'],
                           how='any')
        elif 'u_sinr' in df.columns:
            print_with_line_number(f'5G UEMR 数据', __file__)
            df = df.dropna(subset=['u_longitude', 'u_latitude', 'u_pci', 'u_freq', 'u_rsrp', 'u_rsrq', 'u_sinr'],
                           how='any')
        elif 'f_longitude' in df.columns and 'f_sinr' not in df.columns:
            print_with_line_number(f'4G finger 数据', __file__)
            df = df.dropna(subset=['f_longitude', 'f_latitude', 'f_pci', 'f_freq', 'f_rsrp', 'f_rsrq'], how='any')
        elif 'u_longitude' in df.columns and 'u_sinr' not in df.columns:
            print_with_line_number(f'4G UEMR 数据', __file__)
            df = df.dropna(subset=['u_longitude', 'u_latitude', 'u_pci', 'u_freq', 'u_rsrp', 'u_rsrq'], how='any')
        print('---' * 50)

        # 将结果写入到新的CSV文件中
        df.to_csv(os.path.join(src_data, i_f), index=False)


if __name__ == '__main__':
    src_data = r'E:\work\MR_Data\data_place\4G'
    delete_empty_value_column()