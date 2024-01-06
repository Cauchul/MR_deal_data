# -*- coding: utf-8 -*-

import os
import shutil

import pandas as pd

from Common import delete_last_character, check_file_exists, get_last_character, list_files_in_directory


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


def merge_res_data():
    folder_path = r'E:\work\demo_merge\output'

    file_list = list_files_in_directory(folder_path)
    print('file_list: ', file_list)

    # net_char = '5G'
    # for i in range(5):
    #     i += 1
    #     v_char = f'V{i}_S22'
    #     h_char = f'H{i}_S22'
    #     dev_char = h_char.split('_')[1]
    #     print('v_char: ', v_char)
    #     print('h_char: ', h_char)
    #     # csv_file_list = [file for file in os.listdir(folder_path) if file.endswith('.csv')]
    #     csv_file_list = [file for file in file_list if
    #                      net_char in file and v_char in file or net_char in file and h_char in file]
    #     print('csv_file_list: ', csv_file_list)
    #     out_file = delete_last_character(csv_file_list[0].split('.')[0]) + f'_HV_{dev_char}.csv'
    #
    #     print('out_file: ', out_file)
    #     data = pd.concat([pd.read_csv(os.path.join(folder_path, file)).assign(FileName=os.path.basename(file)) for file in csv_file_list])
    #
    #     data.to_csv(r'E:\work\demo_merge\merged\{}'.format(os.path.basename(out_file)), index=False)

    # for file in csv_file_list:
    #     os.remove(os.path.join(folder_path, file))

    net_char = '4G'
    for i in range(5):
        i += 1
        v_char = f'V{i}_748'
        h_char = f'H{i}_748'
        dev_char = h_char.split('_')[1]
        print('v_char: ', v_char)
        print('h_char: ', h_char)
        # csv_file_list = [file for file in os.listdir(folder_path) if file.endswith('.csv')]
        csv_file_list = [file for file in file_list if
                         net_char in file and v_char in file or net_char in file and h_char in file]
        print('csv_file_list: ', csv_file_list)
        out_file = delete_last_character(csv_file_list[0].split('.')[0]) + f'_HV_{dev_char}.csv'

        print('out_file: ', out_file)
        data = pd.concat(
            [pd.read_csv(os.path.join(folder_path, file)).assign(FileName=os.path.basename(file)) for file in
             csv_file_list])

        data.to_csv(r'E:\work\demo_merge\merged\{}'.format(os.path.basename(out_file)), index=False)


merge_res_data()
