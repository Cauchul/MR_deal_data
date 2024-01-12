# -*- coding: utf-8 -*-
# 设置csv文件中的品牌名称 # 处理当前路径下的文件
import os

from Common import read_csv_get_df, df_write_to_csv, print_with_line_number

src_data = r'E:\work\MR_Data\1月12号\下午测试'

csv_files = [file for file in os.listdir(src_data) if file.endswith('.csv')]

for i_f in csv_files:
    cur_file = os.path.join(src_data, i_f)
    # print(cur_file)
    print_with_line_number(f'当前处理的文件为：{cur_file}', __file__)
    des_df = read_csv_get_df(cur_file)
    f_device_brand = 'XIAOMI'
    f_device_model = '13'
    print_with_line_number(f'设置的品牌：{f_device_brand}', __file__)
    print_with_line_number(f'设置的设备型号：{f_device_model}', __file__)
    if 'f_device_brand' in des_df.columns:
        print_with_line_number(f'当前处理的文件为：{i_f}', __file__)
        des_df['f_device_brand'] = f_device_brand
        des_df['f_device_model'] = f_device_model
    else:
        print_with_line_number(f'当前处理的文件为：{i_f}', __file__)
        des_df['u_device_brand'] = f_device_brand
        des_df['u_device_model'] = f_device_model
    df_write_to_csv(des_df, cur_file)
    print('---' * 50)
