# -*- coding: utf-8 -*-

import os
import shutil

import pandas as pd

from Common import delete_last_character, check_file_exists, get_last_character


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
    # folder_path = r'E:\work\demo_merge\output'
    # net_char = 'LTE'
    # v_char = 'V1'
    # h_char = 'H1'
    # # csv_file_list = [file for file in os.listdir(folder_path) if file.endswith('.csv')]
    # csv_file_list = [file for file in os.listdir(folder_path) if net_char in file and v_char in file or net_char in file and h_char in file]
    # print('csv_file_list: ', csv_file_list)
    # out_file = delete_last_character(csv_file_list[0].split('.')[0]) + '_HV.csv'
    #
    # print('out_file: ', out_file)
    # data = pd.concat([pd.read_csv(os.path.join(folder_path, file)).assign(FileName=file) for file in csv_file_list])
    #
    # data.to_csv(r'E:\work\demo_merge\merged\{}'.format(out_file), index=False)
    #
    # for file in csv_file_list:
    #     os.remove(os.path.join(folder_path, file))

    # 清理merge_tmp目录
    # clear_directory(folder_path)

    folder_path = r'E:\work\demo_merge\output'
    merge_path = r'E:\work\demo_merge\merged'
    net_char_list = ['5G']
    for net_char in net_char_list:
        for i in range(5):
            print(i + 1)
            v_char = f'V{i + 1}'
            h_char = F'H{i + 1}'
            csv_file_list = [file for file in os.listdir(folder_path) if
                             net_char in file and v_char in file or net_char in file and h_char in file]
            if csv_file_list:
                print('csv_file_list: ', csv_file_list)
                out_file = delete_last_character(csv_file_list[0].split('.')[0])
                res_char1 = get_last_character(csv_file_list[0].split('.')[0])
                res_char2 = get_last_character(csv_file_list[1].split('.')[0])
                print('out_file111: ', out_file)
                # print('out_file111: ', os.path.exists(os.path.join(folder_path, out_file + '_HV.csv')))

                if os.path.exists(os.path.join(merge_path, out_file + f'_{res_char1}{res_char2}.csv')):
                    i_c = 0
                    print('here')
                    while True:
                        i_c += 1
                        if check_file_exists(os.path.join(merge_path, out_file + f'_{res_char1}{res_char2}{i_c}.csv')):
                            continue
                        else:
                            out_file = out_file + f'_{res_char1}{res_char2}{i_c}.csv'
                            break
                else:
                    out_file = out_file + f'_{res_char1}{res_char2}.csv'

                print('out_file: ', out_file)
                data = pd.concat([pd.read_csv(os.path.join(folder_path, file)).assign(FileName=file) for file in csv_file_list])

                data.to_csv(r'E:\work\demo_merge\merged\{}'.format(out_file), index=False)

                # for file in csv_file_list:
                #     os.remove(os.path.join(folder_path, file))


merge_res_data()
