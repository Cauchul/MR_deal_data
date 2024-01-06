# -*- coding: utf-8 -*-

import re

import numpy as np

from Common import *
from CommonData import set_scene_data, WalkTour_table_format_dict
from DataPreprocessing import DataPreprocessing


class DealData:
    @staticmethod
    def deal_wetest_indoor_4g(log_df_4g, in_f_msisdn, set_scene_data):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        log_df_4g['PCTime_'] = DataPreprocessing.convert_datetime_to_timestamp_v1(log_df_4g['pc_time'])
        deal_df_object.delete_second_level_duplicate_data(log_df_4g, log_df_4g['PCTime_'])

        # 获取指定列
        log_df_4g = log_df_4g.loc[:, ['imei',
                                      'imsi',
                                      'lte_eci',
                                      'ts',
                                      'lon',
                                      'lat',
                                      'lte_serving_cell_pci',
                                      'lte_serving_cell_freq',
                                      'lte_serving_cell_rsrp',
                                      'lte_serving_cell_rsrq',
                                      'lte_neighbor_cell_1_pci',
                                      'lte_neighbor_cell_1_freq',
                                      'lte_neighbor_cell_1_rsrp',
                                      'lte_neighbor_cell_1_rsrq',
                                      'lte_neighbor_cell_2_pci',
                                      'lte_neighbor_cell_2_freq',
                                      'lte_neighbor_cell_2_rsrp',
                                      'lte_neighbor_cell_2_rsrq',
                                      'lte_neighbor_cell_3_pci',
                                      'lte_neighbor_cell_3_freq',
                                      'lte_neighbor_cell_3_rsrp',
                                      'lte_neighbor_cell_3_rsrq',
                                      'lte_neighbor_cell_4_pci',
                                      'lte_neighbor_cell_4_freq',
                                      'lte_neighbor_cell_4_rsrp',
                                      'lte_neighbor_cell_4_rsrq',
                                      'lte_neighbor_cell_5_pci',
                                      'lte_neighbor_cell_5_freq',
                                      'lte_neighbor_cell_5_rsrp',
                                      'lte_neighbor_cell_5_rsrq',
                                      'lte_neighbor_cell_6_pci',
                                      'lte_neighbor_cell_6_freq',
                                      'lte_neighbor_cell_6_rsrp',
                                      'lte_neighbor_cell_6_rsrq',
                                      'lte_neighbor_cell_7_pci',
                                      'lte_neighbor_cell_7_freq',
                                      'lte_neighbor_cell_7_rsrp',
                                      'lte_neighbor_cell_7_rsrq',
                                      'lte_neighbor_cell_8_pci',
                                      'lte_neighbor_cell_8_freq',
                                      'lte_neighbor_cell_8_rsrp',
                                      'lte_neighbor_cell_8_rsrq',
                                      'pc_time',
                                      'x_new',
                                      'y_new']]

        # 列重命名
        log_df_4g = log_df_4g.rename(
            columns={
                'imsi': 'f_imsi',
                'imei': 'f_imei',
                'lte_eci': 'f_cell_id',
                'ts': 'f_time',
                'lon': 'f_longitude',
                'lat': 'f_latitude',
                'lte_serving_cell_pci': 'f_pci',
                'lte_serving_cell_freq': 'f_freq',
                'lte_serving_cell_rsrp': 'f_rsrp',
                'lte_serving_cell_rsrq': 'f_rsrq',
                'lte_neighbor_cell_1_freq': 'f_freq_n1',
                'lte_neighbor_cell_1_pci': 'f_pci_n1',
                'lte_neighbor_cell_1_rsrp': 'f_rsrp_n1',
                'lte_neighbor_cell_1_rsrq': 'f_rsrq_n1',
                'lte_neighbor_cell_2_freq': 'f_freq_n2',
                'lte_neighbor_cell_2_pci': 'f_pci_n2',
                'lte_neighbor_cell_2_rsrp': 'f_rsrp_n2',
                'lte_neighbor_cell_2_rsrq': 'f_rsrq_n2',
                'lte_neighbor_cell_3_freq': 'f_freq_n3',
                'lte_neighbor_cell_3_pci': 'f_pci_n3',
                'lte_neighbor_cell_3_rsrp': 'f_rsrp_n3',
                'lte_neighbor_cell_3_rsrq': 'f_rsrq_n3',
                'lte_neighbor_cell_4_freq': 'f_freq_n4',
                'lte_neighbor_cell_4_pci': 'f_pci_n4',
                'lte_neighbor_cell_4_rsrp': 'f_rsrp_n4',
                'lte_neighbor_cell_4_rsrq': 'f_rsrq_n4',
                'lte_neighbor_cell_5_freq': 'f_freq_n5',
                'lte_neighbor_cell_5_pci': 'f_pci_n5',
                'lte_neighbor_cell_5_rsrp': 'f_rsrp_n5',
                'lte_neighbor_cell_5_rsrq': 'f_rsrq_n5',
                'lte_neighbor_cell_6_freq': 'f_freq_n6',
                'lte_neighbor_cell_6_pci': 'f_pci_n6',
                'lte_neighbor_cell_6_rsrp': 'f_rsrp_n6',
                'lte_neighbor_cell_6_rsrq': 'f_rsrq_n6',
                'lte_neighbor_cell_7_freq': 'f_freq_n7',
                'lte_neighbor_cell_7_pci': 'f_pci_n7',
                'lte_neighbor_cell_7_rsrp': 'f_rsrp_n7',
                'lte_neighbor_cell_7_rsrq': 'f_rsrq_n7',
                'lte_neighbor_cell_8_freq': 'f_freq_n8',
                'lte_neighbor_cell_8_pci': 'f_pci_n8',
                'lte_neighbor_cell_8_rsrp': 'f_rsrp_n8',
                'lte_neighbor_cell_8_rsrq': 'f_rsrq_n8',
                'PC Time': 'pc_time',
                'x_new': 'f_x',
                'y_new': 'f_y',

            })
        # 删除重复列
        log_df_4g = deal_df_object.delete_duplicate_columns(log_df_4g)

        log_df_4g['f_msisdn'] = in_f_msisdn
        # 设置场景信息
        log_df_4g = set_scene_data(log_df_4g)
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
        # 计算行进方向。
        log_df_4g = deal_df_object.calculate_directions(log_df_4g)

        log_df_4g['f_enb_id'] = log_df_4g['f_cell_id'] // 256
        log_df_4g[['f_year', 'f_month', 'f_day']] = log_df_4g['f_time'].apply(convert_timestamp_to_date).to_list()
        # log_df_4g['f_cst_time'] = log_df_4g['pc_time']
        log_df_4g['f_eci'] = log_df_4g['f_cell_id']

        # SID暂时都赋值1
        log_df_4g['f_sid'] = ''
        log_df_4g['f_pid'] = (log_df_4g.index + 1).astype(str)
        new_columns = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
                       'f_building', 'f_floor', 'f_area', 'f_prru_id', 'f_scenario',
                       'f_roaming_type', 'f_imsi', 'f_imei', 'f_msisdn', 'f_cell_id',
                       'f_enb_id', 'f_time', 'f_longitude', 'f_latitude', 'f_altitude',
                       'f_phr', 'f_enb_received_power', 'f_ta', 'f_aoa', 'f_pci',
                       'f_freq', 'f_rsrp', 'f_rsrq', 'f_neighbor_cell_number', 'f_freq_n1',
                       'f_pci_n1', 'f_freq_n2', 'f_pci_n2', 'f_freq_n3', 'f_pci_n3',
                       'f_freq_n4', 'f_pci_n4', 'f_freq_n5', 'f_pci_n5', 'f_freq_n6',
                       'f_pci_n6', 'f_freq_n7', 'f_pci_n7', 'f_freq_n8', 'f_pci_n8',
                       'f_rsrp_n1', 'f_rsrq_n1', 'f_rsrp_n2', 'f_rsrq_n2', 'f_rsrp_n3',
                       'f_rsrq_n3', 'f_rsrp_n4', 'f_rsrq_n4', 'f_rsrp_n5', 'f_rsrq_n5',
                       'f_rsrp_n6', 'f_rsrq_n6', 'f_rsrp_n7', 'f_rsrq_n7', 'f_rsrp_n8',
                       'f_rsrq_n8', 'f_year', 'f_month', 'f_day', 'pc_time', 'f_x', 'f_y',
                       'f_sid', 'f_pid', 'f_direction', 'f_source', 'f_device_brand',
                       'f_device_model']
        log_df_4g = log_df_4g.reindex(columns=new_columns)
        DataPreprocessing.data_filling(log_df_4g, 'f_cell_id')

        # # 标题统一小写
        log_df_4g = log_df_4g.rename(str.lower, axis='columns')
        return log_df_4g

    @staticmethod
    def wetest_indoor_merge_ue_zcy(in_ue_data, in_zcy_data):
        in_ue_data['ts'] = DataPreprocessing.convert_datetime_to_timestamp_v1(in_ue_data['pc_time'])
        in_zcy_data['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_zcy_data['test_time'])
        in_merge_df = pd.merge(in_ue_data, in_zcy_data[['ts', 'x_new', 'y_new', 'lon', 'lat']], on='ts')
        return in_merge_df

    # @staticmethod
    # def walktour_indoor_merge_ue_zcy(in_ue_data, in_zcy_data):
    #     # in_ue_data['ts'] = DataPreprocessing.convert_datetime_to_timestamp_v1(in_ue_data['PC Time'])
    #     in_ue_data['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_ue_data['PC Time'])
    #     # in_zcy_data['ts'] = in_zcy_data['created_by_ue_time'].map(lambda x: int(str(x)[0:10]))
    #     in_zcy_data['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_zcy_data['test_time'])
    #     in_merge_df = pd.merge(in_ue_data, in_zcy_data[['ts', 'x_new', 'y_new', 'lon', 'lat', 'direction', 'altitude']])
    #     return in_merge_df

    # @staticmethod
    # def walktour_indoor_merge_ue_zcy(in_ue_data, in_zcy_data):
    #     # in_ue_data['ts'] = DataPreprocessing.convert_datetime_to_timestamp_v1(in_ue_data['PC Time'])
    #     in_ue_data['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_ue_data['PC Time'])
    #     # in_zcy_data['ts'] = in_zcy_data['created_by_ue_time'].map(lambda x: int(str(x)[0:10]))
    #     in_zcy_data['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_zcy_data['test_time'])
    #     in_merge_df = pd.merge(in_ue_data, in_zcy_data[['ts', 'x_new', 'y_new', 'lon', 'lat', 'direction', 'altitude']])
    #     print('in_merge_df: ', in_merge_df)
    #     return in_merge_df
    @staticmethod
    def walktour_indoor_merge_ue_zcy(in_ue_data, in_zcy_data):
        in_ue_data['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_ue_data['PC Time'])
        in_zcy_data['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_zcy_data['test_time'])
        in_merge_df = pd.merge(in_ue_data,
                               in_zcy_data[['ts', 'f_x', 'f_y', 'f_longitude', 'f_latitude', 'direction', 'altitude']])
        print('in_merge_df: ', in_merge_df)
        return in_merge_df

    @staticmethod
    def walktour_merge_ue_imei(in_ue_df, in_imei_df):
        out_merge_df = pd.merge(in_ue_df, in_imei_df, left_on="PC Time", right_on="PCTime", how='left')
        return out_merge_df

    @staticmethod
    def deal_wetest_indoor_5g(log_df_5g, in_f_msisdn, set_scene_data):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        log_df_5g['PCTime_'] = DataPreprocessing.convert_datetime_to_timestamp_v1(log_df_5g['pc_time'])
        deal_df_object.delete_second_level_duplicate_data(log_df_5g, log_df_5g['PCTime_'])

        # 定义要进行替换的正则表达式模式
        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_5g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_5g.columns]

        log_df_5g = log_df_5g.loc[:, ['imei',
                                      'imsi',
                                      'nci',
                                      'ts',
                                      'lon',
                                      'lat',
                                      'nr_serving_cell_pci',
                                      'nr_serving_cell_freq',
                                      'nr_serving_cell_ssb_rsrp',
                                      'nr_serving_cell_ssb_rsrq',
                                      'nr_serving_cell_ssb_sinr',
                                      'nr_neighbor_cell_1_pci',
                                      'nr_neighbor_cell_1_freq',
                                      'nr_neighbor_cell_1_ssb_rsrp',
                                      'nr_neighbor_cell_1_ssb_rsrq',
                                      'nr_neighbor_cell_1_ssb_sinr',
                                      'nr_neighbor_cell_2_pci',
                                      'nr_neighbor_cell_2_freq',
                                      'nr_neighbor_cell_2_ssb_rsrp',
                                      'nr_neighbor_cell_2_ssb_rsrq',
                                      'nr_neighbor_cell_2_ssb_sinr',
                                      'nr_neighbor_cell_3_pci',
                                      'nr_neighbor_cell_3_freq',
                                      'nr_neighbor_cell_3_ssb_rsrp',
                                      'nr_neighbor_cell_3_ssb_rsrq',
                                      'nr_neighbor_cell_3_ssb_sinr',
                                      'nr_neighbor_cell_4_pci',
                                      'nr_neighbor_cell_4_freq',
                                      'nr_neighbor_cell_4_ssb_rsrp',
                                      'nr_neighbor_cell_4_ssb_rsrq',
                                      'nr_neighbor_cell_4_ssb_sinr',
                                      'nr_neighbor_cell_5_pci',
                                      'nr_neighbor_cell_5_freq',
                                      'nr_neighbor_cell_5_ssb_rsrp',
                                      'nr_neighbor_cell_5_ssb_rsrq',
                                      'nr_neighbor_cell_5_ssb_sinr',
                                      'nr_neighbor_cell_6_pci',
                                      'nr_neighbor_cell_6_freq',
                                      'nr_neighbor_cell_6_ssb_rsrp',
                                      'nr_neighbor_cell_6_ssb_rsrq',
                                      'nr_neighbor_cell_6_ssb_sinr',
                                      'nr_neighbor_cell_7_pci',
                                      'nr_neighbor_cell_7_freq',
                                      'nr_neighbor_cell_7_ssb_rsrp',
                                      'nr_neighbor_cell_7_ssb_rsrq',
                                      'nr_neighbor_cell_7_ssb_sinr',
                                      'nr_neighbor_cell_8_pci',
                                      'nr_neighbor_cell_8_freq',
                                      'nr_neighbor_cell_8_ssb_rsrp',
                                      'nr_neighbor_cell_8_ssb_rsrq',
                                      'nr_neighbor_cell_8_ssb_sinr',
                                      'pc_time',
                                      'x_new',
                                      'y_new']]

        log_df_5g = log_df_5g.rename(
            columns={
                'imsi': 'f_imsi',
                'imei': 'f_imei',
                'nci': 'f_cell_id',
                'ts': 'f_time',
                'lon': 'f_longitude',
                'lat': 'f_latitude',
                'nr_serving_cell_pci': 'f_pci',
                'nr_serving_cell_freq': 'f_freq',
                'nr_serving_cell_ssb_rsrp': 'f_rsrp',
                'nr_serving_cell_ssb_rsrq': 'f_rsrq',
                'nr_serving_cell_ssb_sinr': 'f_sinr',
                'nr_neighbor_cell_1_freq': 'f_freq_n1',
                'nr_neighbor_cell_1_pci': 'f_pci_n1',
                'nr_neighbor_cell_1_ssb_rsrp': 'f_rsrp_n1',
                'nr_neighbor_cell_1_ssb_rsrq': 'f_rsrq_n1',
                'nr_neighbor_cell_1_ssb_sinr': 'f_sinr_n1',
                'nr_neighbor_cell_2_freq': 'f_freq_n2',
                'nr_neighbor_cell_2_pci': 'f_pci_n2',
                'nr_neighbor_cell_2_ssb_rsrp': 'f_rsrp_n2',
                'nr_neighbor_cell_2_ssb_rsrq': 'f_rsrq_n2',
                'nr_neighbor_cell_2_ssb_sinr': 'f_sinr_n2',
                'nr_neighbor_cell_3_freq': 'f_freq_n3',
                'nr_neighbor_cell_3_pci': 'f_pci_n3',
                'nr_neighbor_cell_3_ssb_rsrp': 'f_rsrp_n3',
                'nr_neighbor_cell_3_ssb_rsrq': 'f_rsrq_n3',
                'nr_neighbor_cell_3_ssb_sinr': 'f_sinr_n3',
                'nr_neighbor_cell_4_freq': 'f_freq_n4',
                'nr_neighbor_cell_4_pci': 'f_pci_n4',
                'nr_neighbor_cell_4_ssb_rsrp': 'f_rsrp_n4',
                'nr_neighbor_cell_4_ssb_rsrq': 'f_rsrq_n4',
                'nr_neighbor_cell_4_ssb_sinr': 'f_sinr_n4',
                'nr_neighbor_cell_5_freq': 'f_freq_n5',
                'nr_neighbor_cell_5_pci': 'f_pci_n5',
                'nr_neighbor_cell_5_ssb_rsrp': 'f_rsrp_n5',
                'nr_neighbor_cell_5_ssb_rsrq': 'f_rsrq_n5',
                'nr_neighbor_cell_5_ssb_sinr': 'f_sinr_n5',
                'nr_neighbor_cell_6_freq': 'f_freq_n6',
                'nr_neighbor_cell_6_pci': 'f_pci_n6',
                'nr_neighbor_cell_6_ssb_rsrp': 'f_rsrp_n6',
                'nr_neighbor_cell_6_ssb_rsrq': 'f_rsrq_n6',
                'nr_neighbor_cell_6_ssb_sinr': 'f_sinr_n6',
                'nr_neighbor_cell_7_freq': 'f_freq_n7',
                'nr_neighbor_cell_7_pci': 'f_pci_n7',
                'nr_neighbor_cell_7_ssb_rsrp': 'f_rsrp_n7',
                'nr_neighbor_cell_7_ssb_rsrq': 'f_rsrq_n7',
                'nr_neighbor_cell_7_ssb_sinr': 'f_sinr_n7',
                'nr_neighbor_cell_8_freq': 'f_freq_n8',
                'nr_neighbor_cell_8_pci': 'f_pci_n8',
                'nr_neighbor_cell_8_ssb_rsrp': 'f_rsrp_n8',
                'nr_neighbor_cell_8_ssb_rsrq': 'f_rsrq_n8',
                'nr_neighbor_cell_8_ssb_sinr': 'f_sinr_n8',
                'pc_time': 'pc_time',
                'x_new': 'f_x',
                'y_new': 'f_y',
            })
        log_df_5g['f_msisdn'] = in_f_msisdn
        # 删除重复列
        log_df_5g = deal_df_object.delete_duplicate_columns(log_df_5g)
        # 设置场景信息
        log_df_5g = set_scene_data(log_df_5g)
        # 时间转上海时区
        sh_timez = deal_df_object.change_to_Shanghai_time_zone(log_df_5g['f_time'])
        log_df_5g['f_time_1'] = sh_timez
        # 生成finger_id
        finger_id = deal_df_object.generate_finger_id(log_df_5g['f_time_1'], log_df_5g['f_msisdn'])
        log_df_5g['finger_id'] = finger_id
        # 获取领区数
        num_list = deal_df_object.get_cell_number(log_df_5g)
        log_df_5g['f_neighbor_cell_number'] = num_list
        # 置空 UEMR 数据
        log_df_5g = deal_df_object.add_and_empty_UEMR_data(log_df_5g)

        log_df_5g['f_gnb_id'] = log_df_5g['f_cell_id'] // 4096
        log_df_5g['f_imsi'] = np.array(log_df_5g['f_imsi'])
        log_df_5g[['f_year', 'f_month', 'f_day']] = log_df_5g['f_time'].apply(convert_timestamp_to_date).to_list()
        # log_df_5g['f_cst_time'] = log_df_5g['pc_time']
        log_df_5g['f_eci'] = log_df_5g['f_cell_id']

        log_df_5g = deal_df_object.calculate_directions(log_df_5g)  # 计算行进方向。

        # SID暂时都赋值1
        log_df_5g['f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)
        new_columns = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
                       'f_building', 'f_floor', 'f_area', 'f_prru_id', 'f_scenario',
                       'f_roaming_type', 'f_imsi', 'f_imei', 'f_msisdn', 'f_cell_id',
                       'f_gnb_id', 'f_time', 'f_longitude', 'f_latitude', 'f_altitude',
                       'f_phr', 'f_enb_received_power', 'f_ta', 'f_aoa', 'f_pci',
                       'f_freq', 'f_rsrp', 'f_rsrq', 'f_sinr', 'f_neighbor_cell_number', 'f_freq_n1',
                       'f_pci_n1', 'f_freq_n2', 'f_pci_n2', 'f_freq_n3', 'f_pci_n3',
                       'f_freq_n4', 'f_pci_n4', 'f_freq_n5', 'f_pci_n5', 'f_freq_n6',
                       'f_pci_n6', 'f_freq_n7', 'f_pci_n7', 'f_freq_n8', 'f_pci_n8',
                       'f_rsrp_n1', 'f_rsrq_n1', 'f_sinr_n1', 'f_rsrp_n2', 'f_rsrq_n2', 'f_sinr_n2', 'f_rsrp_n3',
                       'f_rsrq_n3', 'f_sinr_n3', 'f_rsrp_n4', 'f_rsrq_n4', 'f_sinr_n4', 'f_rsrp_n5', 'f_rsrq_n5',
                       'f_sinr_n5',
                       'f_rsrp_n6', 'f_rsrq_n6', 'f_sinr_n6', 'f_rsrp_n7', 'f_rsrq_n7', 'f_sinr_n7', 'f_rsrp_n8',
                       'f_rsrq_n8', 'f_sinr_n8', 'f_year', 'f_month', 'f_day', 'pc_time', 'f_x', 'f_y',
                       'f_sid', 'f_pid', 'f_direction', 'f_source', 'f_device_brand',
                       'f_device_model']
        log_df_5g = log_df_5g.reindex(columns=new_columns)
        DataPreprocessing.data_filling(log_df_5g, 'f_cell_id')

        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        return log_df_5g

    @staticmethod
    def deal_wetest_outdoor_4g(log_df_4g, in_f_msisdn, set_scene_data):

        # 删除测试log中 秒级重复数据，同秒取第一条。
        log_df_4g['PCTime_'] = DataPreprocessing.convert_datetime_to_timestamp_v1(log_df_4g['pc_time'])
        log_df_4g = deal_df_object.delete_second_level_duplicate_data(log_df_4g, log_df_4g['PCTime_'])

        log_df_4g['ts'] = DataPreprocessing.convert_datetime_to_timestamp_v1(log_df_4g['pc_time'])

        log_df_4g = log_df_4g.loc[:, ['imei',
                                      'imsi',
                                      'startlocation_longitude',
                                      'startlocation_latitude',
                                      'lte_eci',
                                      'ts',
                                      'lte_serving_cell_pci',
                                      'lte_serving_cell_freq',
                                      'lte_serving_cell_rsrp',
                                      'lte_serving_cell_rsrq',
                                      'lte_neighbor_cell_1_pci',
                                      'lte_neighbor_cell_1_freq',
                                      'lte_neighbor_cell_1_rsrp',
                                      'lte_neighbor_cell_1_rsrq',
                                      'lte_neighbor_cell_2_pci',
                                      'lte_neighbor_cell_2_freq',
                                      'lte_neighbor_cell_2_rsrp',
                                      'lte_neighbor_cell_2_rsrq',
                                      'lte_neighbor_cell_3_pci',
                                      'lte_neighbor_cell_3_freq',
                                      'lte_neighbor_cell_3_rsrp',
                                      'lte_neighbor_cell_3_rsrq',
                                      'lte_neighbor_cell_4_pci',
                                      'lte_neighbor_cell_4_freq',
                                      'lte_neighbor_cell_4_rsrp',
                                      'lte_neighbor_cell_4_rsrq',
                                      'lte_neighbor_cell_5_pci',
                                      'lte_neighbor_cell_5_freq',
                                      'lte_neighbor_cell_5_rsrp',
                                      'lte_neighbor_cell_5_rsrq',
                                      'lte_neighbor_cell_6_pci',
                                      'lte_neighbor_cell_6_freq',
                                      'lte_neighbor_cell_6_rsrp',
                                      'lte_neighbor_cell_6_rsrq',
                                      'lte_neighbor_cell_7_pci',
                                      'lte_neighbor_cell_7_freq',
                                      'lte_neighbor_cell_7_rsrp',
                                      'lte_neighbor_cell_7_rsrq',
                                      'lte_neighbor_cell_8_pci',
                                      'lte_neighbor_cell_8_freq',
                                      'lte_neighbor_cell_8_rsrp',
                                      'lte_neighbor_cell_8_rsrq',
                                      'pc_time']]

        log_df_4g = log_df_4g.rename(
            columns={
                'imsi': 'f_imsi',
                'imei': 'f_imei',
                'lte_eci': 'f_cell_id',
                'ts': 'f_time',
                "startlocation_longitude": 'f_longitude',
                "startlocation_latitude": 'f_latitude',
                'lte_serving_cell_pci': 'f_pci',
                'lte_serving_cell_freq': 'f_freq',
                'lte_serving_cell_rsrp': 'f_rsrp',
                'lte_serving_cell_rsrq': 'f_rsrq',
                'lte_neighbor_cell_1_freq': 'f_freq_n1',
                'lte_neighbor_cell_1_pci': 'f_pci_n1',
                'lte_neighbor_cell_1_rsrp': 'f_rsrp_n1',
                'lte_neighbor_cell_1_rsrq': 'f_rsrq_n1',
                'lte_neighbor_cell_2_freq': 'f_freq_n2',
                'lte_neighbor_cell_2_pci': 'f_pci_n2',
                'lte_neighbor_cell_2_rsrp': 'f_rsrp_n2',
                'lte_neighbor_cell_2_rsrq': 'f_rsrq_n2',
                'lte_neighbor_cell_3_freq': 'f_freq_n3',
                'lte_neighbor_cell_3_pci': 'f_pci_n3',
                'lte_neighbor_cell_3_rsrp': 'f_rsrp_n3',
                'lte_neighbor_cell_3_rsrq': 'f_rsrq_n3',
                'lte_neighbor_cell_4_freq': 'f_freq_n4',
                'lte_neighbor_cell_4_pci': 'f_pci_n4',
                'lte_neighbor_cell_4_rsrp': 'f_rsrp_n4',
                'lte_neighbor_cell_4_rsrq': 'f_rsrq_n4',
                'lte_neighbor_cell_5_freq': 'f_freq_n5',
                'lte_neighbor_cell_5_pci': 'f_pci_n5',
                'lte_neighbor_cell_5_rsrp': 'f_rsrp_n5',
                'lte_neighbor_cell_5_rsrq': 'f_rsrq_n5',
                'lte_neighbor_cell_6_freq': 'f_freq_n6',
                'lte_neighbor_cell_6_pci': 'f_pci_n6',
                'lte_neighbor_cell_6_rsrp': 'f_rsrp_n6',
                'lte_neighbor_cell_6_rsrq': 'f_rsrq_n6',
                'lte_neighbor_cell_7_freq': 'f_freq_n7',
                'lte_neighbor_cell_7_pci': 'f_pci_n7',
                'lte_neighbor_cell_7_rsrp': 'f_rsrp_n7',
                'lte_neighbor_cell_7_rsrq': 'f_rsrq_n7',
                'lte_neighbor_cell_8_freq': 'f_freq_n8',
                'lte_neighbor_cell_8_pci': 'f_pci_n8',
                'lte_neighbor_cell_8_rsrp': 'f_rsrp_n8',
                'lte_neighbor_cell_8_rsrq': 'f_rsrq_n8',
                'pc_time': 'pc_time',
            })
        log_df_4g['f_msisdn'] = in_f_msisdn
        # 删除重复列
        log_df_4g = deal_df_object.delete_duplicate_columns(log_df_4g)
        # 设置场景信息
        log_df_4g = set_scene_data(log_df_4g)
        # 时间转上海时区
        sh_timez = deal_df_object.change_to_Shanghai_time_zone(log_df_4g['f_time'])
        log_df_4g['f_time_1'] = sh_timez
        # 生成finger_id
        finger_id = deal_df_object.generate_finger_id(log_df_4g['f_time_1'], log_df_4g['f_msisdn'])
        log_df_4g['finger_id'] = finger_id
        # 置空 UEMR 数据
        log_df_4g = deal_df_object.add_and_empty_UEMR_data(log_df_4g)

        log_df_4g['f_enb_id'] = log_df_4g['f_cell_id'] // 256
        # 获取领区数
        num_list = deal_df_object.get_cell_number(log_df_4g)
        log_df_4g['f_neighbor_cell_number'] = num_list
        log_df_4g[['f_year', 'f_month', 'f_day']] = log_df_4g['f_time'].apply(convert_timestamp_to_date).to_list()
        # log_df_4g['f_cst_time'] = log_df_4g['pc_time']
        log_df_4g['f_eci'] = log_df_4g['f_cell_id']
        log_df_4g = deal_df_object.calculate_directions(log_df_4g)  # 计算行进方向。

        # SID暂时都赋值1
        log_df_4g['f_sid'] = ''
        log_df_4g['f_pid'] = (log_df_4g.index + 1).astype(str)
        new_columns = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
                       'f_building', 'f_floor', 'f_area', 'f_prru_id', 'f_scenario',
                       'f_roaming_type', 'f_imsi', 'f_imei', 'f_msisdn', 'f_cell_id',
                       'f_enb_id', 'f_time', 'f_longitude', 'f_latitude', 'f_altitude',
                       'f_phr', 'f_enb_received_power', 'f_ta', 'f_aoa', 'f_pci',
                       'f_freq', 'f_rsrp', 'f_rsrq', 'f_neighbor_cell_number', 'f_freq_n1',
                       'f_pci_n1', 'f_freq_n2', 'f_pci_n2', 'f_freq_n3', 'f_pci_n3',
                       'f_freq_n4', 'f_pci_n4', 'f_freq_n5', 'f_pci_n5', 'f_freq_n6',
                       'f_pci_n6', 'f_freq_n7', 'f_pci_n7', 'f_freq_n8', 'f_pci_n8',
                       'f_rsrp_n1', 'f_rsrq_n1', 'f_rsrp_n2', 'f_rsrq_n2', 'f_rsrp_n3',
                       'f_rsrq_n3', 'f_rsrp_n4', 'f_rsrq_n4', 'f_rsrp_n5', 'f_rsrq_n5',
                       'f_rsrp_n6', 'f_rsrq_n6', 'f_rsrp_n7', 'f_rsrq_n7', 'f_rsrp_n8',
                       'f_rsrq_n8', 'f_year', 'f_month', 'f_day', 'pc_time', 'f_x', 'f_y',
                       'f_sid', 'f_pid', 'f_direction', 'f_source', 'f_device_brand',
                       'f_device_model']
        log_df_4g = log_df_4g.reindex(columns=new_columns)
        DataPreprocessing.data_filling(log_df_4g, 'f_cell_id')

        log_df_4g = log_df_4g.rename(str.lower, axis='columns')
        return log_df_4g

    @staticmethod
    def deal_wetest_outdoor_5g(log_df_5g, in_f_msisdn, set_scene_data):
        log_df_5g = deal_df_object.delete_duplicate_columns(log_df_5g)
        # 删除测试log中 秒级重复数据，同秒取第一条。
        log_df_5g['PCTime_'] = DataPreprocessing.convert_datetime_to_timestamp_v1(log_df_5g['pc_time'])
        log_df_5g = deal_df_object.delete_second_level_duplicate_data(log_df_5g, log_df_5g['PCTime_'])

        # 定义要进行替换的正则表达式模式
        log_df_5g['ts'] = DataPreprocessing.convert_datetime_to_timestamp_v1(log_df_5g['pc_time'])
        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_5g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_5g.columns]

        log_df_5g = log_df_5g.loc[:, ['imei',
                                      'imsi',
                                      'nci',
                                      'ts',
                                      'startlocation_longitude',
                                      'startlocation_latitude',
                                      'nr_serving_cell_pci',
                                      'nr_serving_cell_freq',
                                      'nr_serving_cell_ssb_rsrp',
                                      'nr_serving_cell_ssb_rsrq',
                                      'nr_serving_cell_ssb_sinr',
                                      'nr_neighbor_cell_1_pci',
                                      'nr_neighbor_cell_1_freq',
                                      'nr_neighbor_cell_1_ssb_rsrp',
                                      'nr_neighbor_cell_1_ssb_rsrq',
                                      'nr_neighbor_cell_1_ssb_sinr',
                                      'nr_neighbor_cell_2_pci',
                                      'nr_neighbor_cell_2_freq',
                                      'nr_neighbor_cell_2_ssb_rsrp',
                                      'nr_neighbor_cell_2_ssb_rsrq',
                                      'nr_neighbor_cell_2_ssb_sinr',
                                      'nr_neighbor_cell_3_pci',
                                      'nr_neighbor_cell_3_freq',
                                      'nr_neighbor_cell_3_ssb_rsrp',
                                      'nr_neighbor_cell_3_ssb_rsrq',
                                      'nr_neighbor_cell_3_ssb_sinr',
                                      'nr_neighbor_cell_4_pci',
                                      'nr_neighbor_cell_4_freq',
                                      'nr_neighbor_cell_4_ssb_rsrp',
                                      'nr_neighbor_cell_4_ssb_rsrq',
                                      'nr_neighbor_cell_4_ssb_sinr',
                                      'nr_neighbor_cell_5_pci',
                                      'nr_neighbor_cell_5_freq',
                                      'nr_neighbor_cell_5_ssb_rsrp',
                                      'nr_neighbor_cell_5_ssb_rsrq',
                                      'nr_neighbor_cell_5_ssb_sinr',
                                      'nr_neighbor_cell_6_pci',
                                      'nr_neighbor_cell_6_freq',
                                      'nr_neighbor_cell_6_ssb_rsrp',
                                      'nr_neighbor_cell_6_ssb_rsrq',
                                      'nr_neighbor_cell_6_ssb_sinr',
                                      'nr_neighbor_cell_7_pci',
                                      'nr_neighbor_cell_7_freq',
                                      'nr_neighbor_cell_7_ssb_rsrp',
                                      'nr_neighbor_cell_7_ssb_rsrq',
                                      'nr_neighbor_cell_7_ssb_sinr',
                                      'nr_neighbor_cell_8_pci',
                                      'nr_neighbor_cell_8_freq',
                                      'nr_neighbor_cell_8_ssb_rsrp',
                                      'nr_neighbor_cell_8_ssb_rsrq',
                                      'nr_neighbor_cell_8_ssb_sinr',
                                      'pc_time']]

        log_df_5g = log_df_5g.rename(
            columns={
                'imsi': 'f_imsi',
                'imei': 'f_imei',
                'nci': 'f_cell_id',
                'ts': 'f_time',
                'lon': 'f_longitude',
                'lat': 'f_latitude',
                'nr_serving_cell_pci': 'f_pci',
                'nr_serving_cell_freq': 'f_freq',
                'nr_serving_cell_ssb_rsrp': 'f_rsrp',
                'nr_serving_cell_ssb_rsrq': 'f_rsrq',
                'nr_serving_cell_ssb_sinr': 'f_sinr',
                'nr_neighbor_cell_1_freq': 'f_freq_n1',
                'nr_neighbor_cell_1_pci': 'f_pci_n1',
                'nr_neighbor_cell_1_ssb_rsrp': 'f_rsrp_n1',
                'nr_neighbor_cell_1_ssb_rsrq': 'f_rsrq_n1',
                'nr_neighbor_cell_1_ssb_sinr': 'f_sinr_n1',
                'nr_neighbor_cell_2_freq': 'f_freq_n2',
                'nr_neighbor_cell_2_pci': 'f_pci_n2',
                'nr_neighbor_cell_2_ssb_rsrp': 'f_rsrp_n2',
                'nr_neighbor_cell_2_ssb_rsrq': 'f_rsrq_n2',
                'nr_neighbor_cell_2_ssb_sinr': 'f_sinr_n2',
                'nr_neighbor_cell_3_freq': 'f_freq_n3',
                'nr_neighbor_cell_3_pci': 'f_pci_n3',
                'nr_neighbor_cell_3_ssb_rsrp': 'f_rsrp_n3',
                'nr_neighbor_cell_3_ssb_rsrq': 'f_rsrq_n3',
                'nr_neighbor_cell_3_ssb_sinr': 'f_sinr_n3',
                'nr_neighbor_cell_4_freq': 'f_freq_n4',
                'nr_neighbor_cell_4_pci': 'f_pci_n4',
                'nr_neighbor_cell_4_ssb_rsrp': 'f_rsrp_n4',
                'nr_neighbor_cell_4_ssb_rsrq': 'f_rsrq_n4',
                'nr_neighbor_cell_4_ssb_sinr': 'f_sinr_n4',
                'nr_neighbor_cell_5_freq': 'f_freq_n5',
                'nr_neighbor_cell_5_pci': 'f_pci_n5',
                'nr_neighbor_cell_5_ssb_rsrp': 'f_rsrp_n5',
                'nr_neighbor_cell_5_ssb_rsrq': 'f_rsrq_n5',
                'nr_neighbor_cell_5_ssb_sinr': 'f_sinr_n5',
                'nr_neighbor_cell_6_freq': 'f_freq_n6',
                'nr_neighbor_cell_6_pci': 'f_pci_n6',
                'nr_neighbor_cell_6_ssb_rsrp': 'f_rsrp_n6',
                'nr_neighbor_cell_6_ssb_rsrq': 'f_rsrq_n6',
                'nr_neighbor_cell_6_ssb_sinr': 'f_sinr_n6',
                'nr_neighbor_cell_7_freq': 'f_freq_n7',
                'nr_neighbor_cell_7_pci': 'f_pci_n7',
                'nr_neighbor_cell_7_ssb_rsrp': 'f_rsrp_n7',
                'nr_neighbor_cell_7_ssb_rsrq': 'f_rsrq_n7',
                'nr_neighbor_cell_7_ssb_sinr': 'f_sinr_n7',
                'nr_neighbor_cell_8_freq': 'f_freq_n8',
                'nr_neighbor_cell_8_pci': 'f_pci_n8',
                'nr_neighbor_cell_8_ssb_rsrp': 'f_rsrp_n8',
                'nr_neighbor_cell_8_ssb_rsrq': 'f_rsrq_n8',
                'nr_neighbor_cell_8_ssb_sinr': 'f_sinr_n8',
                'pc_time': 'pc_time',
                "startlocation_longitude": 'f_longitude',
                "startlocation_latitude": 'f_latitude',
            })
        log_df_5g['f_msisdn'] = in_f_msisdn
        # 设置场景信息
        log_df_5g = set_scene_data(log_df_5g)
        # 时间转上海时区
        sh_timez = deal_df_object.change_to_Shanghai_time_zone(log_df_5g['f_time'])
        log_df_5g['f_time_1'] = sh_timez
        # 生成finger_id
        finger_id = deal_df_object.generate_finger_id(log_df_5g['f_time_1'], log_df_5g['f_msisdn'])
        log_df_5g['finger_id'] = finger_id
        # 置空 UEMR 数据
        log_df_5g = deal_df_object.add_and_empty_UEMR_data(log_df_5g)
        # 获取领区数
        num_list = deal_df_object.get_cell_number(log_df_5g)
        log_df_5g['f_neighbor_cell_number'] = num_list

        log_df_5g['f_imsi'] = np.array(log_df_5g['f_imsi'])
        log_df_5g['f_gnb_id'] = log_df_5g['f_cell_id'] // 4096
        log_df_5g[['f_year', 'f_month', 'f_day']] = log_df_5g['f_time'].apply(convert_timestamp_to_date).to_list()
        # log_df_5g['f_cst_time'] = log_df_5g['pc_time']
        log_df_5g['f_eci'] = log_df_5g['f_cell_id']

        log_df_5g = deal_df_object.calculate_directions(log_df_5g)  # 计算行进方向。

        # SID暂时都赋值1
        log_df_5g['f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)
        new_columns = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
                       'f_building', 'f_floor', 'f_area', 'f_prru_id', 'f_scenario',
                       'f_roaming_type', 'f_imsi', 'f_imei', 'f_msisdn', 'f_cell_id',
                       'f_gnb_id', 'f_time', 'f_longitude', 'f_latitude', 'f_altitude',
                       'f_phr', 'f_enb_received_power', 'f_ta', 'f_aoa', 'f_pci',
                       'f_freq', 'f_rsrp', 'f_rsrq', 'f_sinr', 'f_neighbor_cell_number', 'f_freq_n1',
                       'f_pci_n1', 'f_freq_n2', 'f_pci_n2', 'f_freq_n3', 'f_pci_n3',
                       'f_freq_n4', 'f_pci_n4', 'f_freq_n5', 'f_pci_n5', 'f_freq_n6',
                       'f_pci_n6', 'f_freq_n7', 'f_pci_n7', 'f_freq_n8', 'f_pci_n8',
                       'f_rsrp_n1', 'f_rsrq_n1', 'f_sinr_n1', 'f_rsrp_n2', 'f_rsrq_n2', 'f_sinr_n2', 'f_rsrp_n3',
                       'f_rsrq_n3', 'f_sinr_n3', 'f_rsrp_n4', 'f_rsrq_n4', 'f_sinr_n4', 'f_rsrp_n5', 'f_rsrq_n5',
                       'f_sinr_n5',
                       'f_rsrp_n6', 'f_rsrq_n6', 'f_sinr_n6', 'f_rsrp_n7', 'f_rsrq_n7', 'f_sinr_n7', 'f_rsrp_n8',
                       'f_rsrq_n8', 'f_sinr_n8', 'f_year', 'f_month', 'f_day', 'pc_time', 'f_x', 'f_y',
                       'f_sid', 'f_pid', 'f_direction', 'f_source', 'f_device_brand',
                       'f_device_model']
        log_df_5g = log_df_5g.reindex(columns=new_columns)
        DataPreprocessing.data_filling(log_df_5g, 'f_cell_id')

        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        return log_df_5g

    @staticmethod
    def deal_WalkTour_indoor_4g(log_df_4g, in_net_type):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        log_df_4g = deal_df_object.delete_second_level_duplicate_data(log_df_4g, log_df_4g['ts'])

        cell_cnt = 0
        while True:
            cell_cnt += 1
            if f'NCell{cell_cnt} EARFCN' in log_df_4g.columns:
                prefix = f'NCell{cell_cnt}'
                suffixes = ['NARFCN', 'PCI', 'SS-RSRP', 'SS-RSRQ', 'SS-SINR']
                change_dict = {'NARFCN': 'f_freq_n', 'PCI': 'f_pci_n', 'SS-RSRP': 'f_rsrp_n', 'SS-RSRQ': 'f_rsrq_n',
                               'SS-SINR': 'f_sinr_n'}

                for suffix in suffixes:
                    old_column_name = f'{prefix} {suffix}'
                    new_column_name = f'{change_dict[suffix]}{cell_cnt}'

                    log_df_4g.rename(columns={old_column_name: new_column_name}, inplace=True)
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
        log_df_4g = set_scene_data(log_df_4g)
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

        log_df_4g = log_df_4g.reindex(columns=WalkTour_table_format_dict[in_net_type])
        # 计算领区数
        cell_number = deal_df_object.get_cell_number(log_df_4g)
        log_df_4g['f_neighbor_cell_number'] = cell_number
        DataPreprocessing.data_filling(log_df_4g, 'f_cell_id')

        log_df_4g = log_df_4g.rename(str.lower, axis='columns')
        return log_df_4g

    @staticmethod
    def deal_WalkTour_indoor_5g(log_df_5g, in_net_type):
        # 删除测试log中 秒级重复数据，同秒取第一条
        log_df_5g = log_df_5g.groupby(log_df_5g['ts']).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。

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
        log_df_5g = set_scene_data(log_df_5g)

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

        log_df_5g = log_df_5g.reindex(columns=WalkTour_table_format_dict[in_net_type])
        # 获取领区数
        num_list = deal_df_object.get_cell_number(log_df_5g)
        log_df_5g['f_neighbor_cell_number'] = num_list
        DataPreprocessing.data_filling(log_df_5g, 'f_cell_id')

        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        return log_df_5g


    @staticmethod
    def deal_WalkTour_outdoor_4g(uemr_data_df, in_f_msisdn, set_scene_data):
        uemr_data_df = uemr_data_df.loc[:, [
                                               'PCell ECI',
                                               'PCell EARFCN',
                                               'PCell PCI',
                                               'PCell RSRP',
                                               'PCell RSRQ',
                                               'NCell1 EARFCN',
                                               'NCell1 PCI',
                                               'NCell1 RSRP',
                                               'NCell1 RSRQ',
                                               'NCell2 EARFCN',
                                               'NCell2 PCI',
                                               'NCell2 RSRP',
                                               'NCell2 RSRQ',
                                               'NCell3 EARFCN',
                                               'NCell3 PCI',
                                               'NCell3 RSRP',
                                               'NCell3 RSRQ',
                                               'NCell4 EARFCN',
                                               'NCell4 PCI',
                                               'NCell4 RSRP',
                                               'NCell4 RSRQ',
                                               'NCell5 EARFCN',
                                               'NCell5 PCI',
                                               'NCell5 RSRP',
                                               'NCell5 RSRQ',
                                               'NCell6 EARFCN',
                                               'NCell6 PCI',
                                               'NCell6 RSRP',
                                               'NCell6 RSRQ',
                                               'NCell7 EARFCN',
                                               'NCell7 PCI',
                                               'NCell7 RSRP',
                                               'NCell7 RSRQ',
                                               'NCell8 EARFCN',
                                               'NCell8 PCI',
                                               'NCell8 RSRP',
                                               'NCell8 RSRQ',
                                               'PC Time',
                                               'Longitude',
                                               'Latitude',
                                               'IMEI',
                                               'IMSI'
                                           ]]
        uemr_data_df = uemr_data_df.rename(
            columns={
                'PCell ECI': 'f_cell_id',
                'PCell EARFCN': 'f_freq',
                'PCell PCI': 'f_pci',
                'PCell RSRP': 'f_rsrp',
                'PCell RSRQ': 'f_rsrq',
                'NCell1 EARFCN': 'f_freq_n1',
                'NCell1 PCI': 'f_pci_n1',
                'NCell1 RSRP': 'f_rsrp_n1',
                'NCell1 RSRQ': 'f_rsrq_n1',
                'NCell2 EARFCN': 'f_freq_n2',
                'NCell2 PCI': 'f_pci_n2',
                'NCell2 RSRP': 'f_rsrp_n2',
                'NCell2 RSRQ': 'f_rsrq_n2',
                'NCell3 EARFCN': 'f_freq_n3',
                'NCell3 PCI': 'f_pci_n3',
                'NCell3 RSRP': 'f_rsrp_n3',
                'NCell3 RSRQ': 'f_rsrq_n3',
                'NCell4 EARFCN': 'f_freq_n4',
                'NCell4 PCI': 'f_pci_n4',
                'NCell4 RSRP': 'f_rsrp_n4',
                'NCell4 RSRQ': 'f_rsrq_n4',
                'NCell5 EARFCN': 'f_freq_n5',
                'NCell5 PCI': 'f_pci_n5',
                'NCell5 RSRP': 'f_rsrp_n5',
                'NCell5 RSRQ': 'f_rsrq_n5',
                'NCell6 EARFCN': 'f_freq_n6',
                'NCell6 PCI': 'f_pci_n6',
                'NCell6 RSRP': 'f_rsrp_n6',
                'NCell6 RSRQ': 'f_rsrq_n6',
                'NCell7 EARFCN': 'f_freq_n7',
                'NCell7 PCI': 'f_pci_n7',
                'NCell7 RSRP': 'f_rsrp_n7',
                'NCell7 RSRQ': 'f_rsrq_n7',
                'NCell8 EARFCN': 'f_freq_n8',
                'NCell8 PCI': 'f_pci_n8',
                'NCell8 RSRP': 'f_rsrp_n8',
                'NCell8 RSRQ': 'f_rsrq_n8',
                'PC Time': 'pc_time',
                'Longitude': 'f_longitude',
                'Latitude': 'f_latitude',
                'IMSI': 'f_imsi',
                'IMEI': 'f_imei',
            })
        uemr_data_df['f_time'] = DataPreprocessing.convert_datetime_to_timestamp(uemr_data_df['pc_time'])
        uemr_data_df['f_msisdn'] = in_f_msisdn
        # 删除重复列
        uemr_data_df = deal_df_object.delete_duplicate_columns(uemr_data_df)
        # 设置场景信息
        uemr_data_df = set_scene_data(uemr_data_df)
        # 置空 UEMR 数据
        uemr_data_df = deal_df_object.add_and_empty_UEMR_data(uemr_data_df)
        # 计算行进方向。
        uemr_data_df = deal_df_object.calculate_directions(uemr_data_df)
        # 获取领区数
        num_list = deal_df_object.get_cell_number(uemr_data_df)
        uemr_data_df['f_neighbor_cell_number'] = num_list
        # 时间转上海时区
        sh_timez = deal_df_object.change_to_Shanghai_time_zone(uemr_data_df['f_time'])
        uemr_data_df['f_time_1'] = sh_timez
        # 生成finger_id
        finger_id = deal_df_object.generate_finger_id(uemr_data_df['f_time_1'], uemr_data_df['f_msisdn'])
        uemr_data_df['finger_id'] = finger_id

        uemr_data_df['f_enb_id'] = uemr_data_df['f_cell_id'] // 256
        uemr_data_df['f_cst_time'] = uemr_data_df['pc_time']

        # SID暂时都赋值1
        uemr_data_df['f_sid'] = 1
        uemr_data_df['f_pid'] = (uemr_data_df.index + 1).astype(str)
        uemr_data_df[['f_year', 'f_month', 'f_day']] = uemr_data_df['f_time'].apply(
            convert_timestamp_to_date).to_list()

        new_columns = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
                       'f_building', 'f_floor', 'f_area', 'f_prru_id', 'f_scenario',
                       'f_roaming_type', 'f_imsi', 'f_imei', 'f_msisdn', 'f_cell_id',
                       'f_enb_id', 'f_time', 'f_longitude', 'f_latitude', 'f_altitude',
                       'f_phr', 'f_enb_received_power', 'f_ta', 'f_aoa', 'f_pci', 'f_freq',
                       'f_rsrp', 'f_rsrq', 'f_sinr', 'f_pci_n1', 'f_pci_n2', 'f_pci_n3',
                       'f_pci_n4', 'f_pci_n5', 'f_pci_n6', 'f_pci_n7', 'f_pci_n8',
                       'f_freq_n1', 'f_freq_n2', 'f_freq_n3', 'f_freq_n4', 'f_freq_n5',
                       'f_freq_n6', 'f_freq_n7', 'f_freq_n8', 'f_rsrp_n1', 'f_rsrq_n1',
                       'f_sinr_n1', 'f_rsrp_n2', 'f_rsrq_n2', 'f_sinr_n2', 'f_rsrp_n3',
                       'f_rsrq_n3', 'f_sinr_n3', 'f_rsrp_n4', 'f_rsrq_n4', 'f_sinr_n4',
                       'f_rsrp_n5', 'f_rsrq_n5', 'f_sinr_n5', 'f_rsrp_n6', 'f_rsrq_n6',
                       'f_sinr_n6', 'f_rsrp_n7', 'f_rsrq_n7', 'f_sinr_n7', 'f_rsrp_n8',
                       'f_rsrq_n8', 'f_sinr_n8', 'f_neighbor_cell_number', 'f_year',
                       'f_month', 'f_day', 'pc_time', 'f_x', 'f_y', 'f_source',
                       'f_sid', 'f_pid', 'f_direction', 'f_device_brand', 'f_device_model']
        uemr_data_df = uemr_data_df.reindex(columns=new_columns)
        # 获取领区数
        uemr_data_df = get_cell_number(uemr_data_df)
        # 填充缺失的cell_id
        DataPreprocessing.data_filling(uemr_data_df, 'f_cell_id')
        DataPreprocessing.data_filling(uemr_data_df, 'f_enb_id')
        # DataPreprocessing.determine_duplicate_data(uemr_data_df, 'f_longitude', 'f_latitude')
        uemr_data_df = uemr_data_df.drop_duplicates()
        # 删除x,y有空值的行，删除拥有缺失值的行，any表示任何一行缺失都删除
        uemr_data_df = uemr_data_df.dropna(subset={'f_longitude', 'f_latitude'}, how='any')

        # uemr_data_df['time_cst_u'] = uemr_data_df['pc_time']
        # 时间转时间戳类型

        # uemr_data_df = uemr_data_df.groupby('IMSI').get_group(f_msisdn)
        # 校验cell_id是否有交集
        # DataPreprocessing.check_cellId_intersection(uemr_data_df, 'uemr', 'IMSI')
        # uemr_data_df = DataPreprocessing.group_by_IMSI(uemr_data_df, 'uemr', 'IMSI')

        print('分组后的4GLog数据：\n' + str(uemr_data_df))

        uemr_data_df = uemr_data_df.rename(str.lower, axis='columns')
        return uemr_data_df

    @staticmethod
    def deal_WalkTour_outdoor_5g(uemr_data_df, in_f_msisdn, set_scene_data):
        # # 定义要进行替换的正则表达式模式
        pattern = re.compile(r'NCell(\d)(\d)')
        # # 使用正则表达式匹配并替换列名
        uemr_data_df.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in
                                uemr_data_df.columns]

        uemr_data_df = uemr_data_df.loc[:, [
                                               'PC Time',
                                               'Longitude',
                                               'Latitude',
                                               'PCell1 -Beam PCI',
                                               'PCell1 -Beam NARFCN',
                                               'PCell1 -Beam SS-RSRP',
                                               'PCell1 -Beam SS-RSRQ',
                                               'PCell1 -Beam SS-SINR',
                                               'NCell1 -Beam PCI',
                                               'NCell1 -Beam NARFCN',
                                               'NCell1 -Beam SS-RSRP',
                                               'NCell1 -Beam SS-RSRQ',
                                               'NCell1 -Beam SS-SINR',
                                               'NCell2 -Beam PCI',
                                               'NCell2 -Beam NARFCN',
                                               'NCell2 -Beam SS-RSRP',
                                               'NCell2 -Beam SS-RSRQ',
                                               'NCell2 -Beam SS-SINR',
                                               'NCell3 -Beam PCI',
                                               'NCell3 -Beam NARFCN',
                                               'NCell3 -Beam SS-RSRP',
                                               'NCell3 -Beam SS-RSRQ',
                                               'NCell3 -Beam SS-SINR',
                                               'NCell4 -Beam PCI',
                                               'NCell4 -Beam NARFCN',
                                               'NCell4 -Beam SS-RSRP',
                                               'NCell4 -Beam SS-RSRQ',
                                               'NCell4 -Beam SS-SINR',
                                               'NCell5 -Beam PCI',
                                               'NCell5 -Beam NARFCN',
                                               'NCell5 -Beam SS-RSRP',
                                               'NCell5 -Beam SS-RSRQ',
                                               'NCell5 -Beam SS-SINR',
                                               'NCell6 -Beam PCI',
                                               'NCell6 -Beam NARFCN',
                                               'NCell6 -Beam SS-RSRP',
                                               'NCell6 -Beam SS-RSRQ',
                                               'NCell6 -Beam SS-SINR',
                                               'NR gNodeB ID(24bit)',
                                               'NCI',
                                               'IMSI',
                                               'IMEI'
                                           ]]
        uemr_data_df = uemr_data_df.rename(
            columns={
                'PCell1 -Beam PCI': 'f_pci',
                'PCell1 -Beam NARFCN': 'f_freq',
                'PCell1 -Beam SS-RSRP': 'f_rsrp',
                'PCell1 -Beam SS-RSRQ': 'f_rsrq',
                'PCell1 -Beam SS-SINR': 'f_sinr',
                'NCell1 -Beam NARFCN': 'f_freq_n1',
                'NCell1 -Beam PCI': 'f_pci_n1',
                'NCell1 -Beam SS-RSRP': 'f_rsrp_n1',
                'NCell1 -Beam SS-RSRQ': 'f_rsrq_n1',
                'NCell1 -Beam SS-SINR': 'f_sinr_n1',
                'NCell2 -Beam NARFCN': 'f_freq_n2',
                'NCell2 -Beam PCI': 'f_pci_n2',
                'NCell2 -Beam SS-RSRP': 'f_rsrp_n2',
                'NCell2 -Beam SS-RSRQ': 'f_rsrq_n2',
                'NCell2 -Beam SS-SINR': 'f_sinr_n2',
                'NCell3 -Beam NARFCN': 'f_freq_n3',
                'NCell3 -Beam PCI': 'f_pci_n3',
                'NCell3 -Beam SS-RSRP': 'f_rsrp_n3',
                'NCell3 -Beam SS-RSRQ': 'f_rsrq_n3',
                'NCell3 -Beam SS-SINR': 'f_sinr_n3',
                'NCell4 -Beam NARFCN': 'f_freq_n4',
                'NCell4 -Beam PCI': 'f_pci_n4',
                'NCell4 -Beam SS-RSRP': 'f_rsrp_n4',
                'NCell4 -Beam SS-RSRQ': 'f_rsrq_n4',
                'NCell4 -Beam SS-SINR': 'f_sinr_n4',
                'NCell5 -Beam NARFCN': 'f_freq_n5',
                'NCell5 -Beam PCI': 'f_pci_n5',
                'NCell5 -Beam SS-RSRP': 'f_rsrp_n5',
                'NCell5 -Beam SS-RSRQ': 'f_rsrq_n5',
                'NCell5 -Beam SS-SINR': 'f_sinr_n5',
                'NCell6 -Beam NARFCN': 'f_freq_n6',
                'NCell6 -Beam PCI': 'f_pci_n6',
                'NCell6 -Beam SS-RSRP': 'f_rsrp_n6',
                'NCell6 -Beam SS-RSRQ': 'f_rsrq_n6',
                'NCell6 -Beam SS-SINR': 'f_sinr_n6',
                'PC Time': 'pc_time',
                'IMSI': 'f_imsi',
                'IMEI': 'f_imei',
                'NCI': 'f_cell_id',
                'NR gNodeB ID(24bit)': 'f_gnb_id',
                "Longitude": 'f_longitude',
                "Latitude": 'f_latitude',
            })
        uemr_data_df['f_msisdn'] = in_f_msisdn
        # 删除重复列
        uemr_data_df = deal_df_object.delete_duplicate_columns(uemr_data_df)
        # 设置场景信息
        uemr_data_df = set_scene_data(uemr_data_df)
        uemr_data_df['f_NCI'] = uemr_data_df['f_cell_id']
        uemr_data_df['f_time'] = DataPreprocessing.convert_datetime_to_timestamp(uemr_data_df['pc_time'])
        # 计算行进方向。
        uemr_data_df = deal_df_object.calculate_directions(uemr_data_df)
        # 时间转上海时区
        sh_timez = deal_df_object.change_to_Shanghai_time_zone(uemr_data_df['f_time'])
        uemr_data_df['f_time_1'] = sh_timez

        # 生成finger_id
        finger_id = deal_df_object.generate_finger_id(uemr_data_df['f_time_1'], uemr_data_df['f_msisdn'])
        uemr_data_df['finger_id'] = finger_id
        # 获取领区数
        num_list = deal_df_object.get_cell_number(uemr_data_df)
        uemr_data_df['f_neighbor_cell_number'] = num_list

        # SID暂时都赋值1
        uemr_data_df['f_sid'] = 1
        uemr_data_df['f_pid'] = (uemr_data_df.index + 1).astype(str)
        uemr_data_df[['f_year', 'f_month', 'f_day']] = uemr_data_df['f_time'].apply(
            convert_timestamp_to_date).to_list()

        new_columns = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
                       'f_building', 'f_floor', 'f_area', 'f_prru_id', 'f_scenario',
                       'f_roaming_type', 'f_imsi', 'f_imei', 'f_msisdn', 'f_cell_id',
                       'f_gnb_id', 'f_time', 'f_longitude', 'f_latitude', 'f_altitude',
                       'f_phr', 'f_enb_received_power', 'f_ta', 'f_aoa', 'f_pci', 'f_freq',
                       'f_rsrp', 'f_rsrq', 'f_sinr', 'f_pci_n1', 'f_pci_n2', 'f_pci_n3',
                       'f_pci_n4', 'f_pci_n5', 'f_pci_n6', 'f_pci_n7', 'f_pci_n8',
                       'f_freq_n1', 'f_freq_n2', 'f_freq_n3', 'f_freq_n4', 'f_freq_n5',
                       'f_freq_n6', 'f_freq_n7', 'f_freq_n8', 'f_rsrp_n1', 'f_rsrq_n1',
                       'f_sinr_n1', 'f_rsrp_n2', 'f_rsrq_n2', 'f_sinr_n2', 'f_rsrp_n3',
                       'f_rsrq_n3', 'f_sinr_n3', 'f_rsrp_n4', 'f_rsrq_n4', 'f_sinr_n4',
                       'f_rsrp_n5', 'f_rsrq_n5', 'f_sinr_n5', 'f_rsrp_n6', 'f_rsrq_n6',
                       'f_sinr_n6', 'f_rsrp_n7', 'f_rsrq_n7', 'f_sinr_n7', 'f_rsrp_n8',
                       'f_rsrq_n8', 'f_sinr_n8', 'f_neighbor_cell_number', 'f_year',
                       'f_month', 'f_day', 'pc_time', 'f_NCI', 'f_x', 'f_y', 'f_source',
                       'f_sid', 'f_pid', 'f_direction', 'f_device_brand', 'f_device_model']
        uemr_data_df = uemr_data_df.reindex(columns=new_columns)

        # 填充缺失的cell_id
        # DataPreprocessing.data_filling(uemr_data_df, 'u_cell_id')
        # 删除重复行数据
        DataPreprocessing.determine_duplicate_data(uemr_data_df, 'f_longitude', 'f_latitude')
        uemr_data_df = uemr_data_df.drop_duplicates()
        # 删除x,y有空值的行
        uemr_data_df = uemr_data_df.dropna(subset={'f_longitude', 'f_latitude'}, how='any')
        # uemr_data_df['time_cst_u'] = uemr_data_df['pc_time']
        # 时间转时间戳类型
        # uemr_data_df = uemr_data_df.groupby('IMSI').get_group(f_msisdn)
        # 校验cell_id是否有交集
        # DataPreprocessing.check_cellId_intersection(uemr_data_df, 'uemr', 'IMSI')
        # uemr_data_df = DataPreprocessing.group_by_IMSI(uemr_data_df, 'uemr', 'IMSI')
        print('分组后的uemr5GLog数据：\n' + str(uemr_data_df))
        uemr_data_df = uemr_data_df.rename(str.lower, axis='columns')
        return uemr_data_df
