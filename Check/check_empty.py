# -*- coding: utf-8 -*-
import os.path

import pandas as pd

from Common import print_with_line_number


def check_empty_line(in_csv_file, in_column):
    # print_with_line_number(f'当前检测文件 {in_csv_file}', __file__)
    res_columns = pd.read_csv(in_csv_file, nrows=0).columns

    # 获取两列数据的空值情况
    if in_column in res_columns:
        in_res_df = pd.read_csv(in_csv_file, usecols=[in_column])
        threshold = len(in_res_df) / 3
        empty_rows_count = in_res_df[(in_res_df[in_column].isnull())].shape[0]

        if empty_rows_count > threshold:
            error_records.append({"报错文件": os.path.basename(in_csv_file),
                                  "错误原因": f'{in_column} 列的空行数 {empty_rows_count} 超过总行数 {len(in_res_df)} 的三分之一，数据异常'})
            print_with_line_number(
                f"检测异常  {in_csv_file} 文件 {in_column} 空行数 {empty_rows_count}， 总行数 {len(in_res_df)}",
                __file__)
        else:
            print_with_line_number(f'检测列 {in_column} 通过', __file__)


# 获取除output目录外的所有csv文件
def get_all_csv_files(directory):
    csv_files = []
    for root, dirs, i_files in os.walk(directory):
        if "output" in dirs:
            dirs.remove("output")  # 排除名为 "output" 的子目录
        for file in i_files:
            if file.endswith(".csv"):
                # csv_files.append(os.path.join(root, file))
                csv_files.append(root)
    return list(set(csv_files))


def get_all_check_file(in_src_data):
    tmp_list = []
    in_res_list = get_all_csv_files(in_src_data)

    for in_i_dir in in_res_list:
        # 获取目录下的csv文件
        in_files = os.listdir(in_i_dir)
        for in_i_file in in_files:
            if in_i_file.endswith('.csv'):
                in_check_file = os.path.join(in_i_dir, in_i_file)
                # print(in_check_file)
                tmp_list.append(in_check_file)

    return tmp_list


def count_csv_files_exclude_output(directory):
    csv_count = 0
    for root, dirs, files in os.walk(directory):
        if "output" in dirs:
            dirs.remove("output")  # 排除名为 "output" 的子目录
        for file in files:
            if file.endswith(".csv"):
                csv_count += 1
    return csv_count


if __name__ == '__main__':
    error_records = []  # 用于存储报错的文件名和报错信息

    # 'startlocation_longitude': 'f_longitude',
    # 'startlocation_latitude': 'f_latitude',
    # 'lte_serving_cell_pci': 'f_pci',
    # 'lte_serving_cell_freq': 'f_freq',
    # 'lte_serving_cell_rsrp': 'f_rsrp',
    # 'lte_serving_cell_rsrq': 'f_rsrq',
    WeTest_4g = ['startlocation_longitude', 'startlocation_latitude', 'lte_serving_cell_pci', 'lte_serving_cell_freq',
                 'lte_serving_cell_rsrp', 'lte_serving_cell_rsrq']

    # 'startlocation_longitude': 'f_longitude',
    # 'startlocation_latitude': 'f_latitude',
    # 'nr_serving_cell_pci': 'f_pci',
    # 'nr_serving_cell_freq': 'f_freq',
    # 'nr_serving_cell_ssb_rsrp': 'f_rsrp',
    # 'nr_serving_cell_ssb_rsrq': 'f_rsrq',
    # 'nr_serving_cell_ssb_sinr': 'f_sinr',
    WeTest_5g = ['startlocation_longitude', 'startlocation_latitude', 'nr_serving_cell_pci', 'nr_serving_cell_freq',
                 'nr_serving_cell_ssb_rsrp', 'nr_serving_cell_ssb_rsrq', 'nr_serving_cell_ssb_sinr']

    # 'Longitude': 'f_longitude',
    # 'Latitude': 'f_latitude',
    # 'PCell PCI': 'f_pci',
    # 'PCell EARFCN': 'f_freq',
    # 'PCell RSRP': 'f_rsrp',
    # 'PCell RSRQ': 'f_rsrq',
    WalkTour_4g = ['Longitude', 'Latitude', 'PCell PCI', 'PCell EARFCN', 'PCell RSRP', 'PCell RSRQ']

    # 'Longitude': 'f_longitude',
    # 'Latitude': 'f_latitude',
    # 'PCell1 -Beam PCI': 'f_pci',
    # 'PCell1 -Beam NARFCN': 'f_freq',
    # 'PCell1 -Beam SS-RSRP': 'f_rsrp',
    # 'PCell1 -Beam SS-RSRQ': 'f_rsrq',
    # 'PCell1 -Beam SS-SINR': 'f_sinr',
    WalkTour_5g = ['Longitude', 'Latitude', 'PCell1 -Beam PCI', 'PCell1 -Beam NARFCN', 'PCell1 -Beam SS-RSRP',
                   'PCell1 -Beam SS-RSRQ', 'PCell1 -Beam SS-SINR']

    # 'Longitude': 'f_longitude',
    # 'Latitude': 'f_latitude',
    # 'NR PCI': 'f_pci',
    # 'SSB ARFCN': 'f_freq',
    # 'SS-RSRP': 'f_rsrp',
    # 'SS-RSRQ': 'f_rsrq',
    # 'SS-SINR': 'f_sinr',
    WalkTour_beam_5g = ['Longitude', 'Latitude', 'NR PCI', 'SSB ARFCN', 'SS-RSRP', 'SS-RSRQ', 'SS-SINR']

    check_list = list(set(WeTest_4g + WeTest_5g + WalkTour_4g + WalkTour_5g + WalkTour_beam_5g))

    src_data = r'D:\MrData'

    res_check_list = get_all_check_file(src_data)
    cnt = 0
    for i_f in res_check_list:
        # print(i)
        print_with_line_number(f'当前检测文件 {i_f}', __file__)
        cnt += 1
        for i_c in check_list:
            # print_with_line_number(f'当前检测列 {i_c}', __file__)
            check_empty_line(i_f, i_c)
        print('--' * 50)

    error_df = pd.DataFrame(error_records)
    # 指定要保存到的 CSV 文件路径
    output_csv_file = r"D:\check_RSRP_RSRQ_and_so_on.csv"
    print_with_line_number(
        f'检测文件总数：{cnt}, 目录 {src_data} 下csv文件总数：{count_csv_files_exclude_output(src_data)}', __file__)
    print_with_line_number(f'输出文件：{output_csv_file}', __file__)
    # 将 DataFrame 写入 CSV 文件，不包含行索引
    error_df.to_csv(output_csv_file, index=False)
