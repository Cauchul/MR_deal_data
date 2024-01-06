# -*- coding: utf-8 -*-


# import pandas as pd
#
# from Common import read_csv_get_df
#
# data_path = r'E:\work\mr_dea_data_c1\test_data\12月4号\C1\NR\8539\tmp_path\UE-F4-C1-5G-HUAWEI P40-8539-OUT20231204-143054-DouYin(1)_1204195549.csv'
#
# df = read_csv_get_df(data_path)
#
# print('所有列： ', df.columns)
#
# # df = pd.DataFrame(data)
# #
# # # 使用 filter 和 like 进行列名的模式匹配和修改
# # df.rename(columns=lambda x: x.replace('Old_', 'New_') if 'Old_' in x else x, inplace=True)
# #
# # print(df)

import pandas as pd

# 创建一个示例 DataFrame
data = {
    'NCell1 -Beam NARFCN': [1, 2, 3],
    'NCell1 -Beam PCI': [4, 5, 6],
    'NCell1 -Beam SS-RSRP': [7, 8, 9],
    'NCell1 -Beam SS-RSRQ': [10, 11, 12],
    'NCell1 -Beam SS-SINR': [13, 14, 15],
    'NCell2 -Beam NARFCN': [16, 17, 18],
    'NCell2 -Beam PCI': [19, 20, 21],
    'NCell2 -Beam SS-RSRP': [22, 23, 24],
    'NCell2 -Beam SS-RSRQ': [25, 26, 27],
    'NCell2 -Beam SS-SINR': [28, 29, 30],
}

log_df_5g = pd.DataFrame(data)


i = 0
while True:
    i += 1
    if f'NCell{i} -Beam NARFCN' in log_df_5g.columns:
        prefix = f'NCell{i} -Beam'
        suffixes = ['NARFCN', 'PCI', 'SS-RSRP', 'SS-RSRQ', 'SS-SINR']
        change_dict = {'NARFCN': 'f_freq_n', 'PCI': 'f_pci_n', 'SS-RSRP': 'f_rsrp_n', 'SS-RSRQ': 'f_rsrq_n',
                       'SS-SINR': 'f_sinr_n'}

        for suffix in suffixes:
            old_column_name = f'{prefix} {suffix}'
            new_column_name = f'{change_dict[suffix]}{i}'

            log_df_5g.rename(columns={old_column_name: new_column_name}, inplace=True)
    else:
        break

# # 循环进行批量重命名
# for i in range(1, 5):
#     prefix = f'NCell{i} -Beam'
#     suffixes = ['NARFCN', 'PCI', 'SS-RSRP', 'SS-RSRQ', 'SS-SINR']
#     change_dict = {'NARFCN': 'f_freq_n', 'PCI': 'f_pci_n', 'SS-RSRP': 'f_rsrp_n', 'SS-RSRQ': 'f_rsrq_n',
#                    'SS-SINR': 'f_sinr_n'}
#
#     for suffix in suffixes:
#         old_column_name = f'{prefix} {suffix}'
#         new_column_name = f'{change_dict[suffix]}{i}'
#
#         log_df_5g.rename(columns={old_column_name: new_column_name}, inplace=True)

print(log_df_5g)


# 创建一个示例 DataFrame
# test_data_path = []
# for root, dirs, files in os.walk(dir):
#     for file in files:
#         if 'zip' in file:
#             test_data_path.append(root)
#             file_path = os.path.join(root, file)
#             print('file_path: ', file_path)

# 解压所有的zip文件到unzip路径
for i_path in d_path_list:
    unzip_char_zip_file(i_path)
    # extraction_path = os.path.join(i_path, 'unzip')
    # print(i_path)
    # # 解压所有的zip
    # zip_file = get_file_by_string('zip', i_path)
    # print('zip_file: ', zip_file)
    # # 调用函数解压ZIP文件到当前目录
    # unzip(zip_file, extraction_path)


    # for root, dirs, files in os.walk(extraction_path):
    #     for file in files:
    #         if '-chart' in file or '_pci_' in file:
    #             file_path = os.path.join(root, file)
    #             print('file_path: ', file_path)
    #             copy_file(file_path, i_path)

    # char_f_n = list_files(extraction_path)
    # char_file = os.path.join(i_path, char_f_n)
    # print('char_file: ', char_file)


    # lon_O, lat_O, len_east_x, len_north_y = read_config_file(i_path)
    # # 处理char数据
    # char_file = get_file_by_string('-chart', i_path)
    # print('char_file: ', char_file)
    # char_df = read_csv_get_df(char_file)
    #
    # res_x1_values = data_conversion(len_east_x, char_df['x'])
    # lon = res_x1_values / (111000 * math.cos(lon_O / 180 * math.pi)) + lon_O
    # lon = 2 * max(lon) - lon
    #
    # res_y1_values = data_conversion(len_north_y, char_df['y'])
    # lat = res_y1_values / 111000 + lat_O
    #
    # char_df['f_x'] = res_x1_values
    # char_df['f_y'] = res_y1_values
    # char_df['f_longitude'] = lon
    # char_df['f_latitude'] = lat
    #
    # # 删除列
    # columns_to_delete = ['map_width_pixel', 'map_height_pixel', 'map_width_cm', 'map_height_cm']
    # char_data = char_df.drop(columns_to_delete, axis=1)
    #
    # out_f = char_file.split(".")[0] + f'_{formatted_date}_xyToLonLat_ZCY.csv'
    # df_write_to_csv(char_data, os.path.join(i_path, out_f))
    #
    # generate_images(res_x1_values, res_y1_values, lon, lat, i_path)

    # # 解压所有的zip文件到unzip路径
    # for i_path in d_path_list:
    #     unzip_char_zip_file(i_path)
    #
    # # 拷贝png和char文件到当前数据路径中
    # for i_path in d_path_list:
    #     unzip_path = os.path.join(i_path, 'unzip')
    #     copy_char_file_to_data_path(unzip_path)

    # # 处理char数据，生成走测仪数据
    # for i_path in d_path_list:
    #     # 获取配置文件信息
    #     deal_char_file(i_path)