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


def check_netwalk_type(in_data_file):
    # print('检查网络类型')
    in_file_name = os.path.basename(in_data_file)
    if '5G' in in_file_name:
        in_f_net_type = 'NR'
    elif '4G' in in_file_name:
        in_f_net_type = 'LTE'
    else:
        in_f_net_type = ''

    # 读取文件中的network type；指读取前五行的数据
    in_res_df = pd.read_csv(in_data_file, nrows=5)
    if 'Network Type' not in in_res_df.columns:
        if 'startlocation_longitude' in in_res_df.columns:
            print_with_line_number('WeTest 文件，不检测网络类型', __file__)
            return
        else:
            print_with_line_number(f'文件 {in_data_file} 中没有 Network Type 字段', __file__)
            return
    in_net_type = in_res_df['Network Type'][0]

    if in_f_net_type.lower() != in_net_type.lower():
        error_records.append({"报错文件": os.path.basename(in_data_file),
                              "错误原因": f'期望值 Network Type：{in_f_net_type}, 实际值 Network Type：{in_net_type}'})
        print_with_line_number(
            f'Error 网络类型异常： {in_data_file}  期望 Network Type：{in_f_net_type}, 实际 Network Type：{in_net_type}',
            __file__)
    else:
        print_with_line_number(f'检测网络类型正常, 期望 Network Type：{in_f_net_type}, 实际 Network Type：{in_net_type}',
                               __file__)


def check_blank_line(in_csv_file):
    # print('检查空行')
    # in_res_df = read_csv_get_df(in_csv_file)
    res_columns = pd.read_csv(in_csv_file, nrows=0).columns

    # 获取两列数据的空值情况
    if 'Longitude' in res_columns and 'Latitude' in res_columns:
        in_res_df = pd.read_csv(in_csv_file, usecols=['Longitude', 'Latitude'])
        threshold = len(in_res_df) / 3
        empty_rows_count = in_res_df[(in_res_df['Longitude'].isnull()) & (in_res_df['Latitude'].isnull())].shape[0]
    else:
        in_res_df = pd.read_csv(in_csv_file, usecols=['startlocation_longitude', 'startlocation_latitude'])
        threshold = len(in_res_df) / 3
        empty_rows_count = in_res_df[
            (in_res_df['startlocation_longitude'].isnull()) & (in_res_df['startlocation_latitude'].isnull())].shape[0]

    if empty_rows_count > threshold:
        error_records.append({"报错文件": os.path.basename(in_csv_file),
                              "错误原因": f'csv文件经纬度，空行数超过总行数的三分之一，数据异常, 空行数：{empty_rows_count}，数据总行数：{len(in_res_df)}'})
        print_with_line_number(f"文件 GPS 空行数异常：{in_csv_file} 文件空行数超过三分之一，数据异常！", __file__)
    else:
        print_with_line_number(f'检测文件 GPS 空行数正常， 空行数：{empty_rows_count}， 数据总行数：{len(in_res_df)}',
                               __file__)


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

        # 检查网络类型
        check_netwalk_type(i_f)
        # 检查空行数量
        check_blank_line(i_f)

        cnt += 1
        for i_c in check_list:
            # print_with_line_number(f'当前检测列 {i_c}', __file__)
            check_empty_line(i_f, i_c)
        print('--' * 50)

    error_df = pd.DataFrame(error_records)
    # 指定要保存到的 CSV 文件路径
    output_csv_file = r"D:\check_res.csv"
    print_with_line_number(
        f'检测文件总数：{cnt}, 目录 {src_data} 下csv文件总数：{count_csv_files_exclude_output(src_data)}', __file__)
    print_with_line_number(f'输出文件：{output_csv_file}', __file__)
    # 将 DataFrame 写入 CSV 文件，不包含行索引
    error_df.to_csv(output_csv_file, index=False)
