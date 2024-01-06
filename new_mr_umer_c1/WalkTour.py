# -*- coding: utf-8 -*-
import os

import numpy as np
import pandas as pd

from Common import deal_df_object, get_file_dict, get_zcy_data, get_file_by_string, df_write_to_csv, get_all_data_path, \
    generate_output_file_name, \
    check_path, check_file_exists, read_csv_get_df, get_wifi_bluetooth_data, get_zcy_merge_wifi_bluetooth_data
from DataPreprocessing import convert_timestamp_to_date, DataPreprocessing
from GlobalConfig import WalkTour_table_format_dict, f_msisdn_dict, tmp_res_out_path, TableFormat
from unzip_file import unzip_zcy_data, unzip_zcy_wifi_data


class WalkTour:
    # 合并UE table数据
    @staticmethod
    def deal_ue_table_df(in_ue_file, in_table_file):
        in_ue_df = read_csv_get_df(in_ue_file)
        if os.path.exists(in_table_file):
            in_table_df = read_csv_get_df(in_table_file)
            res_tmp_merge_df = pd.merge(in_ue_df, in_table_df, left_on="PC Time", right_on="PCTime", how='left')
            return res_tmp_merge_df
        else:
            return in_ue_df

    @staticmethod
    def outdoor_get_df(in_data_path):
        file_list = ['UE', 'table']
        file_dict = get_file_dict(in_data_path, file_list)
        print(file_dict)
        res_tmp_df = WalkTour.deal_ue_table_df(file_dict['UE'], file_dict['table'])
        res_tmp_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(res_tmp_df['PC Time'])
        return res_tmp_df

    @staticmethod
    def indoor_get_df_normal_zcy(in_data_path):
        unzip_zcy_data(in_data_path)
        file_list = ['UE', 'table', 'xyToLonLat_ZCY']
        # 获取需要的文件字典
        file_dict = get_file_dict(in_data_path, file_list)
        print(file_dict)
        # 获取zcy数据
        zcy_df = get_zcy_data(file_dict['xyToLonLat_ZCY'])
        # 获取ue table数据
        ue_table_merge_df = WalkTour.deal_ue_table_df(file_dict['UE'], file_dict['table'])
        res_tmp_df = WalkTour.merge_ue_zcy_df(ue_table_merge_df, zcy_df)
        return res_tmp_df

    @staticmethod
    def indoor_get_df_wifi_bluetooth_zcy(in_data_path):
        print('unzip_path: ', in_data_path)
        unzip_zcy_wifi_data(in_data_path)
        file_list = ['UE', 'table', 'xyToLonLat_ZCY', 'xyToLonLat_WIFI_BlueTooth']
        # 获取需要的文件字典
        file_dict = get_file_dict(in_data_path, file_list)
        print(file_dict)
        # 获取zcy数据
        zcy_wifi_bluetooth_df = get_zcy_merge_wifi_bluetooth_data(file_dict['xyToLonLat_ZCY'], file_dict['xyToLonLat_WIFI_BlueTooth'])
        # 获取ue table数据
        ue_table_merge_df = WalkTour.deal_ue_table_df(file_dict['UE'], file_dict['table'])
        res_tmp_df = WalkTour.merge_ue_zcy_df(ue_table_merge_df, zcy_wifi_bluetooth_df)
        return res_tmp_df

    # 合并ue和zcy
    @staticmethod
    def merge_ue_zcy_df(in_ue_df, in_zcy_df):
        in_ue_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_ue_df['PC Time'])
        if not in_zcy_df.empty:
            in_zcy_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_zcy_df['test_time'])
            tmp_df = pd.merge(in_ue_df, in_zcy_df)
            return tmp_df
        else:
            return in_ue_df

    @staticmethod
    def deal_4g_df_data(log_df_4g, in_set_scene_data, in_wifi_flag=False):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        log_df_4g = deal_df_object.delete_second_level_duplicate_data(log_df_4g, log_df_4g['ts'])

        cell_cnt = 0
        while True:
            cell_cnt += 1
            if f'NCell{cell_cnt} EARFCN' in log_df_4g.columns:
                log_df_4g = log_df_4g.rename(
                    columns={
                        f'NCell{cell_cnt} EARFCN': f'f_freq_n{cell_cnt}',
                        f'NCell{cell_cnt} PCI': f'f_pci_n{cell_cnt}',
                        f'NCell{cell_cnt} RSRP': f'f_rsrp_n{cell_cnt}',
                        f'NCell{cell_cnt} RSRQ': f'f_rsrq_n{cell_cnt}',
                    })
            else:
                break

        # 重命名table数据
        log_df_4g = log_df_4g.rename(
            columns={
                'IMSI': 'f_imsi',  # table
                'IMEI': 'f_imei',  # table
            })
        # 重命名zcy数据
        log_df_4g = log_df_4g.rename(
            columns={
                'altitude': 'f_altitude',
                'direction': 'f_direction',
            })

        # 重命名ue数据
        log_df_4g = log_df_4g.rename(
            columns={
                'PCell ECI': 'f_cell_id',
                'ts': 'f_time',
                'PCell PCI': 'f_pci',
                'PCell EARFCN': 'f_freq',
                'PCell RSRP': 'f_rsrp',
                'PCell RSRQ': 'f_rsrq',
                'PC Time': 'pc_time',
            })

        # 删除重复列
        log_df_4g = deal_df_object.delete_duplicate_columns(log_df_4g)

        # 设置场景信息
        log_df_4g = in_set_scene_data(log_df_4g)
        # 时间转上海时区
        sh_timez = deal_df_object.change_to_Shanghai_time_zone(log_df_4g['f_time'])
        log_df_4g['f_time_1'] = sh_timez
        # 生成finger_id
        finger_id = deal_df_object.generate_finger_id(log_df_4g['f_time_1'], log_df_4g['f_msisdn'])
        log_df_4g['finger_id'] = finger_id
        # 置空 UEMR 数据
        log_df_4g = deal_df_object.add_and_empty_UEMR_data(log_df_4g)

        log_df_4g['f_enb_id'] = log_df_4g['f_cell_id'] // 256
        log_df_4g[['f_year', 'f_month', 'f_day']] = log_df_4g['f_time'].apply(convert_timestamp_to_date).to_list()

        log_df_4g['f_eci'] = log_df_4g['f_cell_id']

        # SID暂时都赋值1
        log_df_4g['f_sid'] = ''
        log_df_4g['f_pid'] = (log_df_4g.index + 1).astype(str)

        if in_wifi_flag:
            out_standard_header = WalkTour_table_format_dict['LTE'] + TableFormat.WIFI_BlueTooth
        else:
            out_standard_header = WalkTour_table_format_dict['LTE']

        log_df_4g = log_df_4g.reindex(columns=out_standard_header)
        # 计算领区数
        cell_number = deal_df_object.get_cell_number(log_df_4g)
        log_df_4g['f_neighbor_cell_number'] = cell_number
        DataPreprocessing.data_filling(log_df_4g, 'f_cell_id')

        log_df_4g = log_df_4g.rename(str.lower, axis='columns')
        return log_df_4g

    @staticmethod
    def deal_5g_df_data(log_df_5g, in_set_scene_data, in_wifi_flag=False):
        # 删除测试log中 秒级重复数据，同秒取第一条
        log_df_5g = log_df_5g.groupby(log_df_5g['ts']).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。

        i = 0
        while True:
            i += 1
            if f'NCell{i} -Beam NARFCN' in log_df_5g.columns:
                log_df_5g = log_df_5g.rename(
                    columns={
                        f'NCell{i} -Beam NARFCN': f'f_freq_n{i}',
                        f'NCell{i} -Beam PCI': f'f_pci_n{i}',
                        f'NCell{i} -Beam SS-RSRP': f'f_rsrp_n{i}',
                        f'NCell{i} -Beam SS-RSRQ': f'f_rsrq_n{i}',
                        f'NCell{i} -Beam SS-SINR': f'f_sinr_n{i}',
                    })
            else:
                break

        log_df_5g = log_df_5g.rename(
            columns={
                'IMSI': 'f_imsi',
                'IMEI': 'f_imei',
                'NCI': 'f_cell_id',
                'ts': 'f_time',
                'PCell1 -Beam PCI': 'f_pci',
                'PCell1 -Beam NARFCN': 'f_freq',
                'PCell1 -Beam SS-RSRP': 'f_rsrp',
                'PCell1 -Beam SS-RSRQ': 'f_rsrq',
                'PCell1 -Beam SS-SINR': 'f_sinr',
                'PC Time': 'pc_time',
            })

        # 重命名zcy数据
        log_df_5g = log_df_5g.rename(
            columns={
                'altitude': 'f_altitude',
                'direction': 'f_direction',
            })

        # 删除重复行
        log_df_5g = deal_df_object.delete_duplicate_columns(log_df_5g)
        # 设置场景信息
        log_df_5g = in_set_scene_data(log_df_5g)
        # 时间转上海时区
        sh_timez = deal_df_object.change_to_Shanghai_time_zone(log_df_5g['f_time'])
        log_df_5g['f_time_1'] = sh_timez
        # 生成finger_id
        finger_id = deal_df_object.generate_finger_id(log_df_5g['f_time_1'], log_df_5g['f_msisdn'])
        log_df_5g['finger_id'] = finger_id
        # 置空 UEMR 数据
        log_df_5g = deal_df_object.add_and_empty_UEMR_data(log_df_5g)

        log_df_5g['f_imsi'] = np.array(log_df_5g['f_imsi'])
        log_df_5g['f_gnb_id'] = log_df_5g['f_cell_id'] // 4096
        # SID暂时都赋值1
        log_df_5g['f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)
        log_df_5g[['f_year', 'f_month', 'f_day']] = log_df_5g['f_time'].apply(convert_timestamp_to_date).to_list()
        log_df_5g['f_eci'] = log_df_5g['f_cell_id']

        if in_wifi_flag:
            out_standard_header = WalkTour_table_format_dict['NR'] + TableFormat.WIFI_BlueTooth
        else:
            out_standard_header = WalkTour_table_format_dict['NR']
        log_df_5g = log_df_5g.reindex(columns=out_standard_header)
        # 获取领区数
        num_list = deal_df_object.get_cell_number(log_df_5g)
        log_df_5g['f_neighbor_cell_number'] = num_list
        DataPreprocessing.data_filling(log_df_5g, 'f_cell_id')

        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        return log_df_5g


def walk_tour_outdoor(in_data_path, in_set_scene_data):
    if get_file_by_string('zip', in_data_path):
        print(f'error, 路径:{in_data_path}，下存在 ZCY zip 文件，不是outdoor数据路径')
        return
    print('室外')
    n_scene = 'Outdoor'
    res_df = WalkTour.outdoor_get_df(in_data_path)
    net_type = res_df['Network Type'][0]
    print('net_type: ', net_type)
    if 'LTE' == net_type:
        print('4G')
        res_df = WalkTour.deal_4g_df_data(res_df, in_set_scene_data)
    elif 'NR' == net_type:
        print('5G')
        res_df = WalkTour.deal_5g_df_data(res_df, in_set_scene_data)
    else:
        print(f'net_type:{net_type} error')

    out_file, cur_p_out_f = generate_output_file_name(in_data_path, res_df, net_type, n_scene, 'WT')
    if check_file_exists(out_file + '.csv'):
        i = 0
        while True:
            i += 1
            if check_file_exists(out_file + f'_v{i}' + '.csv'):
                continue
            else:
                out_file = out_file + f'_v{i}' + '.csv'
                break
    else:
        out_file = out_file + '.csv'

    df_write_to_csv(res_df, out_file)
    df_write_to_csv(res_df, cur_p_out_f)


def walk_tour_indoor(in_data_path, in_set_scene_data, wifi_flag):
    if not get_file_by_string('zip', in_data_path):
        print(f'error, 路径:{in_data_path} 下没有找到 ZCY zip文件，不是indoor数据路径')
        return
    n_scene = 'indoor'
    # wifi_flag，如果为真则添加wifi数据
    if wifi_flag:
        res_df = WalkTour.indoor_get_df_wifi_bluetooth_zcy(in_data_path)
    else:
        res_df = WalkTour.indoor_get_df_normal_zcy(in_data_path)
    net_type = res_df['Network Type'][0]
    print('net_type: ', net_type)
    if 'LTE' == net_type:
        print('室内 4G')
        res_df = WalkTour.deal_4g_df_data(res_df, in_set_scene_data, wifi_flag)
    elif 'NR' == net_type:
        print('室内 5G')
        res_df = WalkTour.deal_5g_df_data(res_df, in_set_scene_data, wifi_flag)
    else:
        print(f'net_type:{net_type} error')

    out_file, cur_p_out_f = generate_output_file_name(in_data_path, res_df, net_type, n_scene, 'WT')
    if check_file_exists(out_file + '.csv'):
        i = 0
        while True:
            i += 1
            if check_file_exists(out_file + f'_v{i}' + '.csv'):
                continue
            else:
                out_file = out_file + f'_v{i}' + '.csv'
                break
    else:
        out_file = out_file + '.csv'

    df_write_to_csv(res_df, out_file)
    df_write_to_csv(res_df, cur_p_out_f)


def walk_tour(in_data_path, in_set_scene_data, in_wifi_bluetooth_flag):
    # 根据目录中是否zcy，zip数据判断是室内还是室外
    if get_file_by_string('zip', in_data_path):
        walk_tour_indoor(in_data_path, in_set_scene_data, in_wifi_bluetooth_flag)
    else:
        walk_tour_outdoor(in_data_path, in_set_scene_data)

# def walk_tour(in_data_path, in_set_scene_data):
#     # 根据目录中是否zcy，zip数据判断是室内还是室外
#     if get_file_by_string('zip', in_data_path):
#         n_scene = 'indoor'
#         res_df = WalkTour.indoor_get_df_normal_zcy(in_data_path)
#         net_type = res_df['Network Type'][0]
#         print('net_type: ', net_type)
#         if 'LTE' == net_type:
#             print('室内 4G')
#             res_df = WalkTour.deal_4g_df_data(res_df, in_set_scene_data)
#         elif 'NR' == net_type:
#             print('室内 5G')
#             res_df = WalkTour.deal_5g_df_data(res_df, in_set_scene_data)
#         else:
#             print(f'net_type:{net_type} error')
#     else:
#         n_scene = 'Outdoor'
#         res_df = WalkTour.outdoor_get_df(in_data_path)
#         net_type = res_df['Network Type'][0]
#         print('net_type: ', net_type)
#         if 'LTE' == net_type:
#             print('室外 4G')
#             net_type = '4G'
#             res_df = WalkTour.deal_4g_df_data(res_df, in_set_scene_data)
#         elif 'NR' == net_type:
#             print('室外 5G')
#             net_type = '5G'
#             res_df = WalkTour.deal_5g_df_data(res_df, in_set_scene_data)
#         else:
#             print(f'net_type:{net_type} error')
#
#     out_file, cur_p_out_f = generate_output_file_name(in_data_path, res_df, net_type, n_scene, 'WT')
#     if check_file_exists(out_file + '.csv'):
#         i = 0
#         while True:
#             i += 1
#             if check_file_exists(out_file + f'_v{i}' + '.csv'):
#                 continue
#             else:
#                 out_file = out_file + f'_v{i}' + '.csv'
#                 break
#     else:
#         out_file = out_file + '.csv'
#
#     df_write_to_csv(res_df, out_file)
#     df_write_to_csv(res_df, cur_p_out_f)


def wt_indoor_set_scene_data(log_df):
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

    # 设置f_msisdn默认值
    f_msisdn = '533F8040D9351F4A9499FC7825805B14'
    # 检查数据统一输出目录是否存在，不存在则创建目录
    check_path(tmp_res_out_path)

    wifi_bluetooth_flag = False

    data_path = r'E:\work\demo_merge\demo_test_data\walktour\indoor'
    # 获取所有带有table数据的目录
    data_path_list = get_all_data_path(data_path, 'table')  # outdoor
    for i_path in data_path_list:
        # 根据目录中的设备的imei，动态设置f_msisdn
        if '2934' in i_path:
            f_msisdn = f_msisdn_dict['2934']
        elif '8539' in i_path:
            f_msisdn = f_msisdn_dict['8539']
        walk_tour(i_path, wt_indoor_set_scene_data, wifi_bluetooth_flag)
        # walk_tour_indoor(i_path, wt_indoor_set_scene_data, wifi_bluetooth_flag)
        # walk_tour_outdoor(i_path, wt_indoor_set_scene_data)
