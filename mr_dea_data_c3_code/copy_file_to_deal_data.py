# -*- coding: utf-8 -*-
import os

import pandas as pd

from Common import get_all_data_path, copy_file, get_file_by_string, check_path

data_path = r'E:\work\mr_dea_data_c2\test_data\12月4号'
out_path = r'D:\working\merge\OUT'
test_file_list = ['UE', 'zcy', 'table']  # 数据文件唯一标识


# 根据test_file_list，拷贝指定的数据到deal_data路径下
def get_deal_data_to_dir(in_path_list):
    for in_path in in_path_list:
        in_deal_data_path = os.path.join(in_path, 'deal_data')
        check_path(in_deal_data_path)
        # print(i_path)
        # 获取当前目录下，需要的文件，发入当前目录下的deal_data目录下
        for in_d_char in test_file_list:
            in_get_file = get_file_by_string(in_d_char, in_path)
            print(in_get_file)
            copy_file(in_get_file, in_deal_data_path)
        print('==' * 50)


d_path_list = get_all_data_path(data_path)

# 获取指定数据到deal_data
get_deal_data_to_dir(d_path_list)

# # 处理deal_data的数据
# for i_path in d_path_list:
#     deal_d_path = os.path.join(i_path, 'deal_data')
#     merged_df = pd.DataFrame()
#     for in_d_char in test_file_list:
#         in_get_file = get_file_by_string(in_d_char, i_path)
#         print(in_get_file)
#     print('--' * 50)



# for i_path in d_path_list:
#     deal_data_path = os.path.join(i_path, 'deal_data')
#     check_path(deal_data_path)
#     # print(i_path)
#     # 获取当前目录下，需要的文件，发入当前目录下的deal_data目录下
#     for data_f_char in test_file_list:
#         get_file = get_file_by_string(data_f_char, i_path)
#         print(get_file)
#         copy_file(get_file, deal_data_path)
#     print('==' * 50)
