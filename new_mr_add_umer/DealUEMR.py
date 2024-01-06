# -*- coding: utf-8 -*-
import os

import numpy as np
import pandas as pd

from Common import deal_df_object, df_write_to_csv, clear_merge_path, check_path, split_path_get_list
from DataPreprocessing import DataPreprocessing, convert_timestamp_to_date
from GlobalConfig import WeTest_table_format_dict, tmp_res_out_path
from standard_output_data_name import standard_out_file


class DealUEMR:
    def deal_uemr_4g(self, log_df_4g):
        cell_cnt = 0
        while True:
            cell_cnt += 1
            if f'uemr.neighbor_{cell_cnt}_cell_pci' in log_df_4g.columns:
                log_df_4g = log_df_4g.rename(
                    columns={
                        f'uemr.neighbor_{cell_cnt}_freq': f'f_freq_n{cell_cnt}',
                        f'uemr.neighbor_{cell_cnt}_cell_pci': f'f_pci_n{cell_cnt}',
                        f'uemr.neighbor_{cell_cnt}_rsrp': f'f_rsrp_n{cell_cnt}',
                        f'uemr.neighbor_{cell_cnt}_rsrq': f'f_rsrq_n{cell_cnt}',
                    })
            else:
                break

        # 重命名table数据
        log_df_4g = log_df_4g.rename(
            columns={
                'uemr.imsi': 'f_imsi',
                'uemr.imeisv': 'f_imei',
                'uemr.msisdn': 'f_msisdn',
                'uemr.cell_id': 'f_cell_id',
                'uemr.year': 'f_year',
                'uemr.month': 'f_month',
                'uemr.day': 'f_day',
                'uemr.enb_id': 'f_enb_id',
                'uemr.ta': 'f_ta',
                'uemr.aoa': 'f_aoa',
                'uemr.enb_received_power': 'f_enb_received_power',
                'uemr.roaming_type': 'f_roaming_type',
                'uemr.phr': 'f_phr',
                'uemr.location_source': 'f_source',
                'uemr.neighbor_cell_number': 'f_neighbor_cell_number',
                'uemr.serving_freq': 'f_freq',
                'uemr.serving_rsrp': 'f_rsrp',
                'uemr.serving_rsrq': 'f_rsrq',
                'uemr.pci': 'f_pci',
                'time': 'pc_time',
                'uemr.location_latitude': 'f_latitude',
                'uemr.location_longitude': 'f_longitude',
                'uemr.location_altitude': 'f_altitude',
            })

        log_df_4g['f_time'] = DataPreprocessing.convert_timestamp_to_datetime(log_df_4g['pc_time'])

        # 删除测试log中 秒级重复数据，同秒取第一条。
        log_df_4g = deal_df_object.delete_second_level_duplicate_data(log_df_4g, log_df_4g['f_time'])

        log_df_4g['f_sid'] = 1
        log_df_4g['f_pid'] = (log_df_4g.index + 1).astype(str)
        # 设置场景信息
        log_df_4g = uemr_set_scene_data(log_df_4g)

        finger_id = 'F' + str(log_df_4g['uemr.t'][0]) + '_' + log_df_4g['f_msisdn'].str[-4:]
        log_df_4g['finger_id'] = finger_id

        log_df_4g = log_df_4g.reindex(columns=WeTest_table_format_dict['LTE'])
        # 计算领区数
        cell_number = deal_df_object.get_cell_number(log_df_4g)
        log_df_4g['f_neighbor_cell_number'] = cell_number
        DataPreprocessing.data_filling(log_df_4g, 'f_cell_id')

        log_df_4g = log_df_4g.rename(str.lower, axis='columns')
        return log_df_4g

    def deal_uemr_5g(self, log_df_5g):
        i = 0
        while True:
            i += 1
            if f'uemr5g.neighbor_{i}_etura_cell_pci' in log_df_5g.columns:
                log_df_5g = log_df_5g.rename(
                    columns={
                        f'uemr5g.neighbor_nr_cell_{i}_freq': f'f_freq_n{i}',
                        f'uemr5g.neighbor_nr_cell_{i}_pci': f'f_pci_n{i}',
                        f'uemr5g.neighbor_nr_cell_{i}_ssb_rsrp': f'f_rsrp_n{i}',
                        f'uemr5g.neighbor_nr_cell_{i}_ssb_rsrq': f'f_rsrq_n{i}',
                        f'uemr5g.neighbor_nr_cell_{i}_ssb_sinr': f'f_sinr_n{i}',
                    })
            else:
                break

        # 重命名table数据
        log_df_5g = log_df_5g.rename(
            columns={
                'uemr5g.imsi': 'f_imsi',
                'uemr5g.pei': 'f_imei',
                'uemr5g.msisdn': 'f_msisdn',
                'uemr5g.cell_id': 'f_cell_id',
                'uemr5g.year': 'f_year',
                'uemr5g.month': 'f_month',
                'uemr5g.day': 'f_day',
                'uemr.enb_id': 'f_enb_id',
                'uemr5g.ta': 'f_ta',
                'uemr5g.aoa': 'f_aoa',
                'uemr5g.enb_received_power': 'f_enb_received_power',
                'uemr5g.roaming_type': 'f_roaming_type',
                'uemr5g.phr': 'f_phr',
                'uemr5g.location_source': 'f_source',
                'uemr5g.neighbor_nr_cell_number': 'f_neighbor_cell_number',
                'uemr5g.create_time': 'pc_time',
                'uemr5g.startlocation_latitude': 'f_latitude',
                'uemr5g.startlocation_longitude': 'f_longitude',
                'uemr5g.startlocation_altitude': 'f_altitude',
                'uemr5g.servingcell_1_freq': 'f_freq',
                'uemr5g.servingcell_1_ssb_rsrp': 'f_rsrp',
                'uemr5g.servingcell_1_ssb_rsrq': 'f_rsrq',
                'uemr5g.servingcell_1_pci': 'f_pci',
                'uemr5g.servingcell_1_ssb_sinr': 'f_sinr',
            })
        log_df_5g['f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)

        log_df_5g['f_time'] = DataPreprocessing.convert_timestamp_to_datetime(log_df_5g['pc_time'])

        # 删除测试log中 秒级重复数据，同秒取第一条。
        log_df_5g = deal_df_object.delete_second_level_duplicate_data(log_df_5g, log_df_5g['f_time'])

        # 删除重复行
        log_df_5g = deal_df_object.delete_duplicate_columns(log_df_5g)
        # 设置场景信息
        log_df_5g = uemr_set_scene_data(log_df_5g)

        finger_id = 'F' + str(log_df_5g['uemr5g.t'][0]) + '_' + log_df_5g['f_msisdn'].str[-4:]
        log_df_5g['finger_id'] = finger_id

        log_df_5g['f_gnb_id'] = log_df_5g['f_cell_id'] // 4096

        log_df_5g = log_df_5g.reindex(columns=WeTest_table_format_dict['NR'])
        # 获取领区数
        num_list = deal_df_object.get_cell_number(log_df_5g)
        log_df_5g['f_neighbor_cell_number'] = num_list
        DataPreprocessing.data_filling(log_df_5g, 'f_cell_id')

        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        return log_df_5g


def uemr_set_scene_data(log_df):
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
    return log_df


f_source = '测试log'  # 测试log；2：MR软采；3：扫频仪；4：WIFI；5：OTT；6：蓝牙;7.WeTest_Log
f_province = "北京"
f_city = "北京"
f_prru_id = 0


def standard_output_name(in_path, in_net_type, in_name_d_time):
    tmp_cur_out_path = os.path.join(in_path, 'output')
    check_path(tmp_cur_out_path)

    p_list = split_path_get_list(in_path)
    print('p_list: ', p_list)

    if 1 == f_scenario:
        n_scenario = 'Indoor'
    else:
        n_scenario = 'Outdoor'

    if '海淀' in f_district:
        n_area = 'HaiDian'
    elif '朝阳' in f_district:
        n_area = 'CaoYang'
    else:
        n_area = 'DaXin'

    file_name = f'{in_net_type}_{n_area}_{n_scenario}_WT_{in_net_type}_uemr_{in_name_d_time}_{p_list[-3]}_{p_list[-4]}_{p_list[-2]}_LOG_UE_{p_list[-1]}'
    tmp_out_file = os.path.join(tmp_res_out_path, file_name + '.csv')
    tmp_cur_p_out_file = os.path.join(tmp_cur_out_path, file_name + '.csv')
    print('tmp_out_file: ', tmp_out_file)
    return tmp_out_file, tmp_cur_p_out_file


if __name__ == '__main__':
    f_device_brand = 'HUAWEI'
    f_device_model = "P40"
    f_area = '国际财经中心'
    f_floor = '1F'
    f_scenario = 1
    f_district = '海淀区'
    f_street = '西三环北'
    f_building = '国际财经中心'

    src_file_4g = r'E:\work\demo_merge\demo_test_data\uemr\4G_UEMR.xlsx'
    src_file_5g = r'E:\work\demo_merge\demo_test_data\uemr\5G_UEMR.xlsx'

    in_data_path = r'E:\work\demo_merge\demo_test_data\uemr'
    net_type = '5G'
    deal_uemr = DealUEMR()
    uemr_data_df = pd.read_excel(src_file_5g, header=0)
    res_df = deal_uemr.deal_uemr_5g(uemr_data_df)

    # 获取测试时间
    name_d_time = str(res_df['pc_time'][0]).split(' ')[0]
    name_d_time = name_d_time[name_d_time.find('-'):].replace('-', '')
    print('name_d_time: ', name_d_time)

    # 生成数据文件名
    out_file, cur_p_out_f = standard_output_name(in_data_path, net_type, name_d_time)

    # 设置输出路径
    out_data_path = r'E:\work\demo_merge\merged'
    clear_merge_path(out_data_path)
    check_path(out_data_path)

    df_write_to_csv(res_df, out_file)
    df_write_to_csv(res_df, cur_p_out_f)
    # 标准化输出文件的文件名
    standard_out_file(out_data_path, in_clear_flag=False)
    # df_write_to_csv(res_df, r'E:\work\demo_merge\demo_test_data\uemr\umer_nr.csv')
