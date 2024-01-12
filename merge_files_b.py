# -*- coding: utf-8 -*-

import os
import shutil

import pandas as pd


def clear_directory(directory_path):
    # 确保目录存在
    if os.path.exists(directory_path):
        # 遍历目录中的所有文件和子目录
        for item in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item)

            # 如果是文件，直接删除
            if os.path.isfile(item_path):
                os.remove(item_path)
            # 如果是目录，递归删除
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)


# folder_path = r'D:\working\merge\merge_tmp'
# csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]
# print('csv_files: ', csv_files)
# tmp_out_file = csv_files[0].split('.')[0]
# out_file = 'Merge_' + tmp_out_file[:tmp_out_file.rfind('_')] + '_横纵.csv'
# print('out_file: ', out_file)
# data = pd.concat([pd.read_csv(os.path.join(folder_path, file)).assign(FileName=file) for file in csv_files])
#
# data.to_csv(r'D:\working\merge\merge_data\{}'.format(out_file), index=False)
#
# # 清理merge_tmp目录
# clear_directory(folder_path)


def merge_res_data():
    folder_path = r'D:\working\data_conv\src_data\4G\XIAOMI'
    csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv') and 'finger' in file]
    print('csv_files: ', csv_files)
    # out_file ='Merge_' + csv_files[0].split('.')[0] + '_横纵.csv'
    out_file = csv_files[1].split('.')[0] + '_merge.csv'
    # out_file = 'Merge_' + tmp_out_file[:tmp_out_file.rfind('_')] + '_横纵.csv'
    print('out_file: ', out_file)
    data = pd.concat([pd.read_csv(os.path.join(folder_path, file)).assign(FileName=file) for file in csv_files])

    # data.to_csv(r'D:\working\merge\merge_data\{}'.format(out_file), index=False)
    # data.to_csv(fr'D:\working\data_conv\src_data\{out_file}', index=False)
    data.to_csv(os.path.join(folder_path, out_file), index=False)

    # 清理merge_tmp目录
    # clear_directory(folder_path)


merge_res_data()
