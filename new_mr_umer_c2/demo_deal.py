# -*- coding: utf-8 -*-
import os

import pandas as pd

from Common import list_files_in_directory, get_file_by_str, check_path, read_csv_get_df, deal_df_object, \
    df_write_to_csv
from DataPreprocessing import DataPreprocessing, convert_timestamp_to_date
from GlobalConfig import WeTest_table_format_dict
from WeTest import WeTest
from unzip_file import unzip_zcy_data


def wetest_merge_ue_zcy_by_file(in_ue_file, in_zcy_file):
    tmp_ue_df = read_csv_get_df(in_ue_file)
    tmp_zcy_df = read_csv_get_df(in_zcy_file)
    in_ue_df = pd.merge(tmp_ue_df, tmp_zcy_df, left_on="create_time", right_on="created_by_ue_time", how='left')
    # in_ue_df = pd.merge(tmp_ue_df, tmp_zcy_df)
    # in_ue_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp_v1(in_ue_df['pc_time'])
    return in_ue_df


def demo_deal_wetest_4g(log_df_4g):
    # 删除测试log中 秒级重复数据，同秒取第一条。
    deal_df_object.delete_second_level_duplicate_data(log_df_4g, log_df_4g['create_time'])

    cell_cnt = 0
    while True:
        cell_cnt += 1
        if f'lte_neighbor_cell_{cell_cnt}_freq' in log_df_4g.columns:
            log_df_4g = log_df_4g.rename(
                columns={
                    f'lte_neighbor_cell_{cell_cnt}_freq': f'f_freq_n{cell_cnt}',
                    f'lte_neighbor_cell_{cell_cnt}_pci': f'f_pci_n{cell_cnt}',
                    f'lte_neighbor_cell_{cell_cnt}_rsrp': f'f_rsrp_n{cell_cnt}',
                    f'lte_neighbor_cell_{cell_cnt}_rsrq': f'f_rsrq_n{cell_cnt}',
                })
        else:
            break

    # 列重命名
    log_df_4g = log_df_4g.rename(
        columns={
            'imsi': 'f_imsi',
            'imei': 'f_imei',
            'lte_eci': 'f_cell_id',
            'create_time': 'f_time',
            'lte_serving_cell_pci': 'f_pci',
            'lte_serving_cell_freq': 'f_freq',
            'lte_serving_cell_rsrp': 'f_rsrp',
            'lte_serving_cell_rsrq': 'f_rsrq',
        })
    # 删除重复列
    log_df_4g = deal_df_object.delete_duplicate_columns(log_df_4g)

    # 时间转上海时区
    sh_timez = deal_df_object.change_to_Shanghai_time_zone(log_df_4g['f_time'])
    log_df_4g['f_time_1'] = sh_timez
    # 生成finger_id
    finger_id = deal_df_object.generate_finger_id(log_df_4g['f_time_1'], log_df_4g['f_msisdn'])
    log_df_4g['finger_id'] = finger_id
    # 获取领区数
    num_list = deal_df_object.get_cell_number(log_df_4g)
    log_df_4g['f_neighbor_cell_number'] = num_list
    # 置空 UEMR 数据
    log_df_4g = deal_df_object.add_and_empty_UEMR_data(log_df_4g)

    log_df_4g['f_enb_id'] = log_df_4g['f_cell_id'] // 256

    log_df_4g[['f_year', 'f_month', 'f_day']] = log_df_4g['f_time'].apply(convert_timestamp_to_date).to_list()
    log_df_4g['f_eci'] = log_df_4g['f_cell_id']

    # SID暂时都赋值1
    log_df_4g['f_sid'] = ''
    log_df_4g['f_pid'] = (log_df_4g.index + 1).astype(str)
    log_df_4g = log_df_4g.reindex(columns=WeTest_table_format_dict['LTE'])
    DataPreprocessing.data_filling(log_df_4g, 'f_cell_id')

    # # 标题统一小写
    log_df_4g = log_df_4g.rename(str.lower, axis='columns')
    return log_df_4g


def wetest_set_scene_data(log_df):
    # 设置场景信息
    log_df['f_device_brand'] = f_device_brand
    log_df['f_device_model'] = f_device_model
    log_df['f_area'] = f_area
    log_df['f_floor'] = f_floor
    log_df['f_scenario'] = f_scenario
    log_df['f_province'] = f_province
    log_df['f_city'] = f_city
    log_df['f_district'] = f_district
    log_df['f_street'] = f_street
    log_df['f_building'] = f_building
    log_df['f_prru_id'] = 0
    log_df['f_source'] = f_source
    log_df['f_msisdn'] = f_msisdn
    return log_df


if __name__ == '__main__':
    f_device_brand = 'HUAWEI'
    f_device_model = "P40"
    f_area = '国际财经中心'
    f_floor = '1F'
    f_scenario = 1
    f_district = '海淀区'
    f_street = '西三环北路玲珑路南蓝靛厂南路北洼西街'
    f_building = '国际财经中心'
    f_source = '测试log'  # 测试log；2：MR软采；3：扫频仪；4：WIFI；5：OTT；6：蓝牙;7.WeTest_Log
    f_province = "北京"
    f_city = "北京"
    f_prru_id = 0

    f_msisdn = '533F8040D9351F4A9499FC7825805B14'

    wetest_data_path = r'D:\working\1214\1214国际财经中心(1)\国际财经中心\国际财经中心5G横1_20231214\wetest'
    zcy_data_path = r'D:\working\1214\1214国际财经中心(1)\国际财经中心\国际财经中心5G横1_20231214\zcy_data'
    zcy_zip_path = r'D:\working\1214\1214国际财经中心(1)\国际财经中心\国际财经中心5G横1_20231214'
    out_data_path = os.path.join(zcy_zip_path, 'output')
    check_path(out_data_path)
    check_path(zcy_data_path)

    file_list = list_files_in_directory(wetest_data_path)

    # 解压处理zcy数据
    unzip_zcy_data(zcy_zip_path, zcy_data_path)
    # 获取zcy文件
    zcy_file = get_file_by_str('xyToLonLat_ZCY', zcy_data_path)
    print(zcy_file)

    print(file_list)
    i = 0
    for i_f in file_list:
        if i_f.endswith('.csv'):
            i += 1
            print(i_f)
            res_df = wetest_merge_ue_zcy_by_file(i_f, zcy_file)
            print(res_df)
            # 设置场景数据
            res_df = wetest_set_scene_data(res_df)
            res_df = demo_deal_wetest_4g(res_df)
            df_write_to_csv(res_df, out_data_path + f'{i}.csv')

