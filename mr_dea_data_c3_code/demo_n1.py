# -*- coding: utf-8 -*-
import os

import pandas as pd

from Common import get_all_data_path, copy_file, get_file_by_string, check_path, read_csv_get_df, df_write_to_csv, \
    f_msisdn_dict, find_nth_occurrence, get_dir_base_name
from DealData import DealData

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


def set_scene_data(log_df):
    # 设置场景信息
    log_df['f_device_brand'] = f_device_brand
    log_df['f_device_model'] = f_device_model
    log_df['f_area'] = f_area
    log_df['f_floor'] = f_floor
    log_df['f_scenario'] = f_scenario
    log_df['f_province'] = "北京"
    log_df['f_city'] = "北京"
    log_df['f_district'] = f_district
    log_df['f_street'] = f_street
    log_df['f_building'] = f_building
    log_df['f_prru_id'] = 0
    log_df['f_source'] = f_source
    log_df['f_msisdn'] = f_msisdn

    return log_df


f_floor = '4F'
f_device_brand = "HUAWEI"
f_device_model = "P40"
f_scenario = 1
f_district = '大兴区'
f_street = '大兴国际机场'
f_building = '航站楼'
f_source = '测试log'  # 测试log；2：MR软采；3：扫频仪；4：WIFI；5：OTT；6：蓝牙
f_msisdn = '7314E1BE6DF72134E285D6AC1A99D8B7'
f_area = '值机岛'

d_path_list = get_all_data_path(data_path)

# 处理deal_data的数据
for i_path in d_path_list:
    # 读取所有的数据
    deal_d_path = os.path.join(i_path, 'deal_data')
    ue_file = get_file_by_string('ue', deal_d_path)
    # 获取flag,4G还是5G，室内还是室外，wetest还是walktour

    zcy_df = read_csv_get_df(get_file_by_string('zcy', deal_d_path))
    table_df = read_csv_get_df(get_file_by_string('table', deal_d_path))
    ue_df = read_csv_get_df(ue_file)

    # 合并三个读取到的数据
    ue_df = pd.merge(ue_df, table_df, left_on="PC Time", right_on="PCTime", how='left')
    ue_merge_df = pd.merge(ue_df,
                           zcy_df[['test_time', 'f_x', 'f_y', 'f_longitude', 'f_latitude', 'direction', 'altitude']], left_on="PC Time", right_on="test_time", how='left')

    # ue_df = DealData.deal_WalkTour_indoor_4g(ue_merge_df, set_scene_data)
    #
    # file_name = 'Merge_{}_WT_LOG_DT_UE'.format(ue_file.split('.')[0])
    # out_file = os.path.join(out_path, file_name + 'test_demo.csv')
    # df_write_to_csv(ue_df, out_file)

    # 输出路径
    out_path = os.path.join(i_path, 'output')
    check_path(out_path)

    # 通过文件名imei，查找对应的f_msisdn
    imei_v = ue_file.split('-')[5].split('_')[0]
    print('imei_v: ', imei_v)
    print('type: ', type(imei_v))

    # 根据输入文件生成输出文件
    index = find_nth_occurrence(os.path.basename(ue_file), '-', 4)
    tmp_v_f = os.path.basename(ue_file)[:index].replace('-', '_') + '_'
    tmp_v_f = tmp_v_f.replace(' ', '_')

    file_name = 'Merge_{}_WT_LOG_DT_UE'.format(tmp_v_f)
    out_file = os.path.join(out_path, file_name + '.csv')
    print('out_file: ', out_file)

    try:
        if 'LTE' in i_path:
            ue_df = DealData.deal_WalkTour_indoor_4g(ue_merge_df, set_scene_data)
            df_write_to_csv(ue_df, out_file)
        elif 'NR' in i_path:
            ue_df = DealData.deal_WalkTour_indoor_5g(ue_merge_df, set_scene_data)
            df_write_to_csv(ue_df, out_file)
    except ValueError:
        with open(r'D:\working\merge\log_error.txt', 'a', encoding='utf-8') as e_file:
            e_file.write(ue_file)
            e_file.write('\n')
