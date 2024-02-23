# -*- coding: utf-8 -*-
import os

import pandas as pd

from Common import df_write_to_csv, check_path
from calculate_distance.get_max_dis import in_df_calculate_dis


def group_csv_data(in_csv_file, in_group_char):
    # 读取 CSV 文件
    in_df = pd.read_csv(in_csv_file)

    # 按照 'Category' 列的值进行分组
    in_grouped = in_df.groupby(in_group_char)

    res_dict = {}

    for i_group_name, i_group_data in in_grouped:
        # 提取分组后的df中的经纬度值
        in_df_data = i_group_data[['f_longitude', 'f_latitude']].values.tolist()
        print('f_time: ', i_group_name)
        in_res_dis = in_df_calculate_dis(in_df_data)
        print('max_dis: ', in_res_dis)
        print('--' * 15)
        # res_list.append(in_res_dis)
        res_dict[i_group_name] = in_res_dis
        # in_df['max_distance'] = in_res_dis

    in_df['max_distance'] = in_df['f_time'].map(res_dict)

    # 输出到当前目录
    # new_csv_file = in_csv_file.replace(".csv", "_add_nax_dis.csv")

    # 输出到output目录
    output_dir = os.path.join(os.path.dirname(in_csv_file), "max_dis_output")
    check_path(output_dir)
    tmp_csv_file = os.path.join(output_dir, os.path.basename(in_csv_file))
    new_csv_file = tmp_csv_file.replace(".csv", "_add_nax_dis.csv")

    df_write_to_csv(in_df, new_csv_file)


# # 读取 CSV 文件
# df = pd.read_csv(r'E:\work\MR_Data\data_place\demo\5G_HaiDian_indoor_WT_LOG_DT_UE_0124_finger_Mate_40_5G_3.csv')
#
# # 按照 'Category' 列的值进行分组
# grouped = df.groupby('f_time')
#
#
# res_list = []
#
# for group_name, group_data in grouped:
#     # print(f"Group {group_name}:")
#     # print(group_data.columns)
#     # df_write_to_csv(group_data, r'E:\work\MR_Data\data_place\demo\out.csv')
#
#     # print('===== :', group_data)
#     df_data = group_data[['f_longitude', 'f_latitude']].values.tolist()
#     res_dis = calculate_dis_by_df(df_data)
#     # print(res_dis)
#     res_list.append(res_dis)
#
# print(res_list)

def get_cur_dir_all_csv(in_src_data):
    tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if
                     file.endswith('.csv') and 'finger' in file]
    return tmp_csv_files


if __name__ == '__main__':
    # res_list = []
    folder_path = r'E:\work\MR_Data\data_place\demo'
    # 获取当前路径下的所有csv文件
    res_file_list = get_cur_dir_all_csv(folder_path)

    print(res_file_list)

    # csv_file = r'E:\work\MR_Data\data_place\demo\test.csv'

    for i_file in res_file_list:
        group_csv_data(i_file, 'f_time')
