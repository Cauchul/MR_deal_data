import logging
import math
import time
import datetime
from datetime import datetime
import pandas as pd
import numpy as np
import os
import re
import hashlib
import pytz


class DataPreprocessing(object):

    def __init__(self, src_file_5g, src_file_4g):
        self.src_file_5g = src_file_5g
        self.src_file_4g = src_file_4g
        # self.file_name = file_name

    def data_preprocessing_wtlog_4g_outdoor(self):
        """
        预处理WeTest测试log原始数据文件_室外;
        :return: 返回清洗完成的测试数据文件，文件后缀格式csv;
        """
        import datetime
        current_date = datetime.date.today()
        formatted_date = current_date.strftime("%Y_%m_%d")
        file_name = '4G_WT_LOG_DT_{}_UE2_VIVO_2'.format(formatted_date)
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：VIVO_Y3_1_865624049070411
        f_msisdn = '154D4095974592369EC605DD5E8C0D24'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：VIVO_Y3_2_863120050305137
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：oppo_Reno8_1_863926068853912
        # f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：oppo_Reno8_2_863926066772577

        # log_df_4g, log_df_5g = self.merge_xy_log()
        log_df_4g = pd.read_csv(os.path.join(path_4g, ue1_4g))
        log_df_4g['PCTime_'] = DataPreprocessing.convert_datetime_to_timestamp_v1(log_df_4g['pc_time'])
        log_df_4g = log_df_4g.groupby(log_df_4g['PCTime_']).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。
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

        duplicate_columns = log_df_4g.columns[log_df_4g.columns.duplicated()]
        log_df_4g = log_df_4g.loc[:, ~log_df_4g.columns.duplicated()]
        log_df_4g['f_msisdn'] = f_msisdn

        log_df_4g['f_time_1'] = pd.to_datetime(log_df_4g['f_time'], unit='s').dt.tz_localize(pytz.utc).dt.tz_convert(
            pytz.timezone('Asia/Shanghai'))
        log_df_4g['finger_id'] = 'F' + log_df_4g['f_time_1'].dt.strftime('%Y%m%d%H') + '_' + log_df_4g['f_msisdn'].str[
                                                                                             -4:]
        log_df_4g['f_province'] = f_province
        log_df_4g['f_city'] = f_city
        log_df_4g['f_area'] = f_area
        log_df_4g['f_floor'] = f_floor
        log_df_4g['f_altitude'] = f_altitude
        log_df_4g['f_scenario'] = f_scenario
        log_df_4g['f_district'] = f_district
        log_df_4g['f_street'] = f_street
        log_df_4g['f_building'] = f_building
        log_df_4g['f_prru_id'] = 0
        log_df_4g['f_source'] = f_source
        log_df_4g['f_roaming_type'] = ''
        log_df_4g['f_phr'] = ''
        log_df_4g['f_enb_received_power'] = ''
        log_df_4g['f_ta'] = ''
        log_df_4g['f_aoa'] = ''
        log_df_4g['f_enb_id'] = log_df_4g['f_cell_id'] // 256
        num_list = []
        for i in range(len(log_df_4g)):
            num_list.append(np.count_nonzero(
                log_df_4g[['f_rsrp_n1', 'f_rsrp_n2', 'f_rsrp_n3', 'f_rsrp_n4', 'f_rsrp_n5',
                           'f_rsrp_n6', 'f_rsrp_n7', 'f_rsrp_n8']].isnull().values[i] == False))
        log_df_4g['f_neighbor_cell_number'] = num_list
        log_df_4g[['f_year', 'f_month', 'f_day']] = log_df_4g['f_time'].apply(convert_timestamp_to_date).to_list()
        # log_df_4g['f_cst_time'] = log_df_4g['pc_time']
        log_df_4g['f_eci'] = log_df_4g['f_cell_id']
        calculate_directions(log_df_4g)  # 计算行进方向。

        # SID暂时都赋值1
        log_df_4g.loc[:, 'f_sid'] = ''
        log_df_4g['f_pid'] = (log_df_4g.index + 1).astype(str)
        log_df_4g['f_device_brand'] = f_device_brand
        log_df_4g['f_device_model'] = f_device_model
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
        log_df_4g.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)

    def data_preprocessing_wtlog_4g(self):
        """
        预处理WeTest 测试 log原始数据文件
        :return: 返回清洗完成的测试数据文件，文件后缀格式csv
        """
        import datetime
        # 获取当前日期
        current_date = datetime.date.today()
        # 格式化为年_月_日形式
        formatted_date = current_date.strftime("%Y_%m_%d")
        file_name = '4G_WT_LOG_DT_{}_UE2_OPPO_2'.format(formatted_date)
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：VIVO_Y3_1
        # f_msisdn = '154D4095974592369EC605DD5E8C0D24'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：VIVO_Y3_2
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：oppo_Reno8_1
        f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：oppo_Reno8_2

        log_df_4g, log_df_5g = self.merge_xy_log()

        # print(log_df_4g.columns.values)
        log_df_4g['PCTime_'] = DataPreprocessing.convert_datetime_to_timestamp_v1(log_df_4g['pc_time'])
        log_df_4g = log_df_4g.groupby(log_df_4g['PCTime_']).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。

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

        duplicate_columns = log_df_4g.columns[log_df_4g.columns.duplicated()]
        log_df_4g = log_df_4g.loc[:, ~log_df_4g.columns.duplicated()]
        log_df_4g['f_msisdn'] = f_msisdn

        log_df_4g['f_time_1'] = pd.to_datetime(log_df_4g['f_time'], unit='s').dt.tz_localize(pytz.utc).dt.tz_convert(
            pytz.timezone('Asia/Shanghai'))
        log_df_4g['finger_id'] = 'F' + log_df_4g['f_time_1'].dt.strftime('%Y%m%d%H') + '_' + log_df_4g['f_msisdn'].str[
                                                                                             -4:]
        log_df_4g['f_province'] = f_province
        log_df_4g['f_city'] = f_city
        log_df_4g['f_area'] = f_area
        log_df_4g['f_floor'] = f_floor
        log_df_4g['f_altitude'] = f_altitude
        log_df_4g['f_scenario'] = f_scenario
        log_df_4g['f_district'] = f_district
        log_df_4g['f_street'] = f_street
        log_df_4g['f_building'] = f_building
        log_df_4g['f_prru_id'] = 0
        log_df_4g['f_source'] = f_source
        log_df_4g['f_roaming_type'] = ''
        log_df_4g['f_phr'] = ''
        log_df_4g['f_enb_received_power'] = ''
        log_df_4g['f_ta'] = ''
        log_df_4g['f_aoa'] = ''
        log_df_4g['f_enb_id'] = log_df_4g['f_cell_id'] // 256
        num_list = []
        for i in range(len(log_df_4g)):
            num_list.append(np.count_nonzero(
                log_df_4g[['f_rsrp_n1', 'f_rsrp_n2', 'f_rsrp_n3', 'f_rsrp_n4', 'f_rsrp_n5',
                           'f_rsrp_n6', 'f_rsrp_n7', 'f_rsrp_n8']].isnull().values[i] == False))
        log_df_4g['f_neighbor_cell_number'] = num_list
        log_df_4g[['f_year', 'f_month', 'f_day']] = log_df_4g['f_time'].apply(convert_timestamp_to_date).to_list()
        # log_df_4g['f_cst_time'] = log_df_4g['pc_time']
        log_df_4g['f_eci'] = log_df_4g['f_cell_id']
        calculate_directions(log_df_4g)  # 计算行进方向。

        # SID暂时都赋值1
        log_df_4g.loc[:, 'f_sid'] = ''
        log_df_4g['f_pid'] = (log_df_4g.index + 1).astype(str)
        log_df_4g['f_device_brand'] = f_device_brand
        log_df_4g['f_device_model'] = f_device_model
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
        log_df_4g.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)

    def data_preprocessing_wtlog_5g_outdoor(self):
        """
        预处理wt 测试 log原始数据文件
        :return: 返回清洗完成的测试数据文件，文件后缀格式csv
        """
        import datetime
        # 获取当前日期
        current_date = datetime.date.today()
        # 格式化为年_月_日形式
        formatted_date = current_date.strftime("%Y_%m_%d")

        # file_name = self.file_name
        # file_name = '5G_WT_UEMR_Outdoor_DT_0825_UE2'
        file_name = '5G_WT_LOG_DT_{}_UE1_OPPO_1'.format(formatted_date)
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：VIVO_Y3_1_865624049070411
        # f_msisdn = '154D4095974592369EC605DD5E8C0D24'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：VIVO_Y3_2_863120050305137
        f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：oppo_Reno8_1_863926068853912
        # f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：oppo_Reno8_2_863926066772577

        # log_df_4g, log_df_5g = self.merge_xy_log()
        log_df_5g = pd.read_csv(os.path.join(path_5g, ue1_5g))
        # log_df_5g.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)
        log_df_5g['PCTime_'] = DataPreprocessing.convert_datetime_to_timestamp_v1(log_df_5g['pc_time'])
        log_df_5g = log_df_5g.groupby(log_df_5g['PCTime_']).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。
        # 定义要进行替换的正则表达式模式
        log_df_5g['ts'] = DataPreprocessing.convert_datetime_to_timestamp_v1(log_df_5g['pc_time'])
        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_5g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_5g.columns]
        # print(log_df_5g.columns)
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
        duplicate_columns = log_df_5g.columns[log_df_5g.columns.duplicated()]
        log_df_5g = log_df_5g.loc[:, ~log_df_5g.columns.duplicated()]
        log_df_5g['f_time_1'] = pd.to_datetime(log_df_5g['f_time'], unit='s').dt.tz_localize(
            pytz.utc).dt.tz_convert(pytz.timezone('Asia/Shanghai'))
        log_df_5g['f_msisdn'] = f_msisdn
        log_df_5g['finger_id'] = 'F' + log_df_5g['f_time_1'].dt.strftime('%Y%m%d%H') + '_' + log_df_5g['f_msisdn'].str[
                                                                                             -4:]
        log_df_5g['f_imsi'] = np.array(log_df_5g['f_imsi'])
        log_df_5g['f_province'] = f_province
        log_df_5g['f_city'] = f_city
        log_df_5g['f_area'] = f_area
        log_df_5g['f_floor'] = f_floor
        log_df_5g['f_altitude'] = f_altitude
        log_df_5g['f_scenario'] = f_scenario
        log_df_5g['f_district'] = f_district
        log_df_5g['f_street'] = f_street
        log_df_5g['f_building'] = f_building
        log_df_5g['f_prru_id'] = 0
        log_df_5g['f_source'] = f_source
        log_df_5g['f_roaming_type'] = ''
        log_df_5g['f_phr'] = ''
        log_df_5g['f_enb_received_power'] = ''
        log_df_5g['f_ta'] = ''
        log_df_5g['f_aoa'] = ''
        log_df_5g['f_gnb_id'] = log_df_5g['f_cell_id'] // 4096
        num_list = []
        for i in range(len(log_df_5g)):
            num_list.append(np.count_nonzero(
                log_df_5g[['f_rsrp_n1', 'f_rsrp_n2', 'f_rsrp_n3', 'f_rsrp_n4', 'f_rsrp_n5', 'f_rsrp_n6', 'f_rsrp_n7',
                           'f_rsrp_n8']].isnull().values[i] == False))
        log_df_5g['f_neighbor_cell_number'] = num_list
        log_df_5g[['f_year', 'f_month', 'f_day']] = log_df_5g['f_time'].apply(convert_timestamp_to_date).to_list()
        # log_df_5g['f_cst_time'] = log_df_5g['pc_time']
        log_df_5g['f_eci'] = log_df_5g['f_cell_id']

        calculate_directions(log_df_5g)  # 计算行进方向。

        # SID暂时都赋值1
        log_df_5g.loc[:, 'f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)
        log_df_5g['f_device_brand'] = f_device_brand
        log_df_5g['f_device_model'] = f_device_model
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

        #  标题统一小写
        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        log_df_5g.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)

    def data_preprocessing_log_wt_5g_outdoor(self):
        """
        预处理室外WT_log原始数据文件
        :return: 返回清洗完成的wt_5G数据文件，文件后缀格式csv
        """
        # file_name = self.file_name
        file_name = '5G_WT_LOG_DT_0911_UE1_Mi12_2'
        # f_msisdn = '533F8040D9351F4A9499FC7825805B14'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；P40_1
        # f_msisdn = '7314E1BE6DF72134E285D6AC1A99D8B7'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；P40_2
        # f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；三星 S22_1
        # f_msisdn = '154D4095974592369EC605DD5E8C0D24'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；三星 S22_2
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 13_1
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 13_2
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 12_1
        f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 12_2
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；IQOO7_1
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；IQOO7_2
        # f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Mate30
        if os.path.exists(self.src_file_5g):
            uemr_data_df = pd.read_csv(os.path.join(path_5g, ue1_5g), encoding='utf-8', header=0)
            col_imei = ['PCTime', 'Lon', 'Lat', 'NR gNodeB ID(24bit)', 'NCI', 'IMSI', 'IMEI']
            imei_data_df = pd.read_csv(os.path.join(path_5g, imei_5g_1), encoding='utf-8', header=0, usecols=col_imei)
            uemr_data_df = pd.merge(uemr_data_df, imei_data_df, left_on="PC Time", right_on="PCTime", how='left')
            if len(uemr_data_df) > 0:
                # uemr_data_df['PCTime_'] = DataPreprocessing.convert_datetime_to_timestamp(uemr_data_df['PC Time'])
                # log_df_5g = uemr_data_df.groupby(uemr_data_df['PCTime_']).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。
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
                                                       'NCell7 -Beam PCI',
                                                       'NCell7 -Beam NARFCN',
                                                       'NCell7 -Beam SS-RSRP',
                                                       'NCell7 -Beam SS-RSRQ',
                                                       'NCell7 -Beam SS-SINR',
                                                       'NCell8 -Beam PCI',
                                                       'NCell8 -Beam NARFCN',
                                                       'NCell8 -Beam SS-RSRP',
                                                       'NCell8 -Beam SS-RSRQ',
                                                       'NCell8 -Beam SS-SINR',
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
                        'NCell7 -Beam NARFCN': 'f_freq_n7',
                        'NCell7 -Beam PCI': 'f_pci_n7',
                        'NCell7 -Beam SS-RSRP': 'f_rsrp_n7',
                        'NCell7 -Beam SS-RSRQ': 'f_rsrq_n7',
                        'NCell7 -Beam SS-SINR': 'f_sinr_n7',
                        'NCell8 -Beam NARFCN': 'f_freq_n8',
                        'NCell8 -Beam PCI': 'f_pci_n8',
                        'NCell8 -Beam SS-RSRP': 'f_rsrp_n8',
                        'NCell8 -Beam SS-RSRQ': 'f_rsrq_n8',
                        'NCell8 -Beam SS-SINR': 'f_sinr_n8',
                        'PC Time': 'pc_time',
                        'IMSI': 'f_imsi',
                        'IMEI': 'f_imei',
                        'NCI': 'f_cell_id',
                        'NR gNodeB ID(24bit)': 'f_gnb_id',
                        "Longitude": 'f_longitude',
                        "Latitude": 'f_latitude',
                    })
                uemr_data_df['f_province'] = f_province
                uemr_data_df['f_city'] = f_city
                uemr_data_df['f_area'] = f_area
                uemr_data_df['f_floor'] = f_floor
                uemr_data_df['f_altitude'] = f_altitude
                uemr_data_df['f_scenario'] = f_scenario
                uemr_data_df['f_NCI'] = uemr_data_df['f_cell_id']
                uemr_data_df['f_time'] = DataPreprocessing.convert_datetime_to_timestamp(uemr_data_df['pc_time'])
                calculate_directions(uemr_data_df)  # 计算行进方向。
                uemr_data_df['PCTime_'] = DataPreprocessing.convert_timestamp_to_datetime_t(uemr_data_df['f_time'])
                uemr_data_df['f_msisdn'] = f_msisdn
                uemr_data_df['f_district'] = f_district
                uemr_data_df['f_street'] = f_street
                uemr_data_df['f_building'] = f_building
                uemr_data_df['f_prru_id'] = 0
                uemr_data_df['f_source'] = f_source
                # SID暂时都赋值1
                uemr_data_df.loc[:, 'f_sid'] = 1
                uemr_data_df['f_pid'] = (uemr_data_df.index + 1).astype(str)
                uemr_data_df['f_device_brand'] = f_device_brand
                uemr_data_df['f_device_model'] = f_device_model
                uemr_data_df['f_time_1'] = pd.to_datetime(uemr_data_df['f_time'], unit='s').dt.tz_localize(
                    pytz.utc).dt.tz_convert(pytz.timezone('Asia/Shanghai'))
                uemr_data_df['finger_id'] = 'F' + uemr_data_df['f_time_1'].dt.strftime('%Y%m%d%H') + '_' + uemr_data_df[
                                                                                                               'f_msisdn'].str[
                                                                                                           -4:]
                uemr_data_df[['f_year', 'f_month', 'f_day']] = uemr_data_df['f_time'].apply(
                    convert_timestamp_to_date).to_list()
                num_list = []
                for i in range(len(uemr_data_df)):
                    num_list.append(np.count_nonzero(
                        uemr_data_df[['f_rsrp_n1', 'f_rsrp_n2', 'f_rsrp_n3', 'f_rsrp_n4', 'f_rsrp_n5',
                                      'f_rsrp_n6', 'f_rsrp_n7', 'f_rsrp_n8']].isnull().values[i] == False))
                uemr_data_df['f_neighbor_cell_number'] = num_list

                duplicate_columns = uemr_data_df.columns[uemr_data_df.columns.duplicated()]
                uemr_data_df = uemr_data_df.loc[:, ~uemr_data_df.columns.duplicated()]
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
                logging.info('>>>>>>>>>> 开始删除重复行数据 ')
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
            # 标题统一小写
            uemr_data_df = uemr_data_df.rename(str.lower, axis='columns')
            uemr_data_df.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)

    def data_preprocessing_log_wt_4g_outdoor(self):
        """
        预处理室外WT_log原始数据文件
        :return: 返回清洗完成的wt_4G数据文件，文件后缀格式csv
        """
        import datetime
        # 获取当前日期
        current_date = datetime.date.today()
        # 格式化为年_月_日形式
        formatted_date = current_date.strftime("%Y_%m_%d")
        file_name = '4G_WT_LOG_DT_{}_UE1_P40_111'.format(formatted_date)
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：iPhone13_1
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：iPhone13_2
        f_msisdn = '533F8040D9351F4A9499FC7825805B14'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；P40_1
        # f_msisdn = '7314E1BE6DF72134E285D6AC1A99D8B7'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；P40_2
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；三星 S22_1
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；三星 S22_2
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 13_1
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 13_2
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；IQOO9
        # f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；IQOO7_1
        # f_msisdn = '154D4095974592369EC605DD5E8C0D24'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；IQOO7_2
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Mate40_1
        # f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Mate40_2
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Honor_90_1
        # f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Honor_90_2

        uemr_data_df = pd.read_csv(os.path.join(path_4g, ue1_4g), encoding='utf-8', header=0)
        # uemr_data_df = pd.read_excel(self.src_file_4g, header=0)
        # uemr_data_df = pd.read_csv(self.src_file_4g, encoding='utf-8', header=0)
        col_imei = ['PCTime', 'Lon', 'Lat', 'IMSI', 'IMEI']
        imei_data_df = pd.read_csv(os.path.join(path_4g, imei_4g_1), encoding='utf-8', header=0, usecols=col_imei)
        uemr_data_df = pd.merge(uemr_data_df, imei_data_df, left_on="PC Time", right_on="PCTime", how='left')
        # print(uemr_data_df.columns.values)
        # # 定义要进行替换的正则表达式模式
        # pattern = re.compile(r'NCell(\d)(\d)')
        # # # 使用正则表达式匹配并替换列名
        # uemr_data_df.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in uemr_data_df.columns]
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
        uemr_data_df['f_phr'] = ''
        uemr_data_df['f_enb_received_power'] = ''
        uemr_data_df['f_ta'] = ''
        uemr_data_df['f_aoa'] = ''
        uemr_data_df['f_enb_id'] = uemr_data_df['f_cell_id'] // 256
        uemr_data_df['f_cst_time'] = uemr_data_df['pc_time']
        uemr_data_df['f_province'] = f_province
        uemr_data_df['f_city'] = f_city
        uemr_data_df['f_area'] = f_area
        uemr_data_df['f_floor'] = f_floor
        uemr_data_df['f_altitude'] = f_altitude
        uemr_data_df['f_scenario'] = f_scenario
        # uemr_data_df['f_cell_id'] = uemr_data_df['f_cell_id']
        uemr_data_df['f_time'] = DataPreprocessing.convert_datetime_to_timestamp(uemr_data_df['pc_time'])
        calculate_directions(uemr_data_df)  # 计算行进方向。
        uemr_data_df['PCTime_'] = DataPreprocessing.convert_timestamp_to_datetime_t(uemr_data_df['f_time'])
        uemr_data_df['f_msisdn'] = f_msisdn
        uemr_data_df['f_district'] = f_district
        uemr_data_df['f_street'] = f_street
        uemr_data_df['f_building'] = f_building
        uemr_data_df['f_prru_id'] = 0
        uemr_data_df['f_source'] = f_source
        # SID暂时都赋值1
        uemr_data_df.loc[:, 'f_sid'] = 1
        uemr_data_df['f_pid'] = (uemr_data_df.index + 1).astype(str)
        uemr_data_df['f_device_brand'] = f_device_brand
        uemr_data_df['f_device_model'] = f_device_model
        uemr_data_df['f_time_1'] = pd.to_datetime(uemr_data_df['f_time'], unit='s').dt.tz_localize(
            pytz.utc).dt.tz_convert(pytz.timezone('Asia/Shanghai'))
        uemr_data_df['finger_id'] = 'F' + uemr_data_df['f_time_1'].dt.strftime('%Y%m%d%H') + '_' + uemr_data_df[
                                                                                                       'f_msisdn'].str[
                                                                                                   -4:]
        uemr_data_df[['f_year', 'f_month', 'f_day']] = uemr_data_df['f_time'].apply(convert_timestamp_to_date).to_list()
        num_list = []
        for i in range(len(uemr_data_df)):
            num_list.append(np.count_nonzero(
                uemr_data_df[['f_rsrp_n1', 'f_rsrp_n2', 'f_rsrp_n3', 'f_rsrp_n4', 'f_rsrp_n5', 'f_rsrp_n6', 'f_rsrp_n7',
                              'f_rsrp_n8']].isnull().values[i] == False))
        uemr_data_df['f_neighbor_cell_number'] = num_list
        duplicate_columns = uemr_data_df.columns[uemr_data_df.columns.duplicated()]
        uemr_data_df = uemr_data_df.loc[:, ~uemr_data_df.columns.duplicated()]
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
        # 填充缺失的cell_id
        DataPreprocessing.data_filling(uemr_data_df, 'f_cell_id')
        DataPreprocessing.data_filling(uemr_data_df, 'f_enb_id')
        # 删除重复行数据
        # DataPreprocessing.determine_duplicate_data(uemr_data_df, 'f_longitude', 'f_latitude')
        # logging.info('>>>>>>>>>> 开始删除重复行数据 ')
        uemr_data_df = uemr_data_df.drop_duplicates()
        # 删除x,y有空值的行
        uemr_data_df = uemr_data_df.dropna(subset={'f_longitude', 'f_latitude'}, how='any')

        # uemr_data_df['time_cst_u'] = uemr_data_df['pc_time']
        # 时间转时间戳类型

        # uemr_data_df = uemr_data_df.groupby('IMSI').get_group(f_msisdn)
        # 校验cell_id是否有交集
        # DataPreprocessing.check_cellId_intersection(uemr_data_df, 'uemr', 'IMSI')
        # uemr_data_df = DataPreprocessing.group_by_IMSI(uemr_data_df, 'uemr', 'IMSI')

        print('分组后的4GLog数据：\n' + str(uemr_data_df))
        # 标题统一小写
        uemr_data_df = uemr_data_df.rename(str.lower, axis='columns')
        uemr_data_df.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)

    def data_preprocessing_walktour_log_4g(self):
        """
        预处walktour 测试 log原始数据文件
        :return: 返回清洗完成的测试数据文件，文件后缀格式csv
        """
        import datetime
        # 获取当前日期
        current_date = datetime.date.today()
        # 格式化为年_月_日形式
        formatted_date = current_date.strftime("%Y_%m_%d")
        file_name = '4G_WT_LOG_DT_{}_UE2_P40_2'.format(formatted_date)
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：iPhone13_1
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：iPhone13_2
        # f_msisdn = '533F8040D9351F4A9499FC7825805B14'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；P40_1
        f_msisdn = '7314E1BE6DF72134E285D6AC1A99D8B7'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；P40_2
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；三星 S22_1
        # f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；三星 S22_1
        # f_msisdn = '154D4095974592369EC605DD5E8C0D24'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；三星 S22_2
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 13_1
        # f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 13_2
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；IQOO9
        # f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Mate30
        # f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Mate40_1
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Mate40_2
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 12_1
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 12_2
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：IQOO7_1
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；IQOO7_2
        # f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：荣耀90_1
        # f_msisdn = '154D4095974592369EC605DD5E8C0D24'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；荣耀90_2

        log_df_4g, log_df_5g = self.merge_xy_log()

        # print(log_df_4g.columns.values)
        log_df_4g['PCTime_'] = DataPreprocessing.convert_datetime_to_timestamp(log_df_4g['PC Time'])
        log_df_4g = log_df_4g.groupby(log_df_4g['PCTime_']).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。

        log_df_4g = log_df_4g.loc[:, ['IMEI',
                                      'IMSI',
                                      'PCell ECI',
                                      'ts',
                                      'lon',
                                      'lat',
                                      'PCell PCI',
                                      'PCell EARFCN',
                                      'PCell RSRP',
                                      'PCell RSRQ',
                                      'NCell1 PCI',
                                      'NCell1 EARFCN',
                                      'NCell1 RSRP',
                                      'NCell1 RSRQ',
                                      'NCell2 PCI',
                                      'NCell2 EARFCN',
                                      'NCell2 RSRP',
                                      'NCell2 RSRQ',
                                      'NCell3 PCI',
                                      'NCell3 EARFCN',
                                      'NCell3 RSRP',
                                      'NCell3 RSRQ',
                                      'NCell4 PCI',
                                      'NCell4 EARFCN',
                                      'NCell4 RSRP',
                                      'NCell4 RSRQ',
                                      'NCell5 PCI',
                                      'NCell5 EARFCN',
                                      'NCell5 RSRP',
                                      'NCell6 RSRQ',
                                      'NCell6 PCI',
                                      'NCell6 EARFCN',
                                      'NCell6 RSRP',
                                      'NCell6 RSRQ',
                                      'NCell7 PCI',
                                      'NCell7 EARFCN',
                                      'NCell7 RSRP',
                                      'NCell7 RSRQ',
                                      'NCell8 PCI',
                                      'NCell8 EARFCN',
                                      'NCell8 RSRP',
                                      'NCell8 RSRQ',
                                      'PC Time',
                                      'x_new',
                                      'y_new']]

        log_df_4g = log_df_4g.rename(
            columns={
                'IMSI': 'f_imsi',
                'IMEI': 'f_imei',
                'PCell ECI': 'f_cell_id',
                'ts': 'f_time',
                'lon': 'f_longitude',
                'lat': 'f_latitude',
                'PCell PCI': 'f_pci',
                'PCell EARFCN': 'f_freq',
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
                'x_new': 'f_x',
                'y_new': 'f_y',

            })

        duplicate_columns = log_df_4g.columns[log_df_4g.columns.duplicated()]
        log_df_4g = log_df_4g.loc[:, ~log_df_4g.columns.duplicated()]
        log_df_4g['f_msisdn'] = f_msisdn

        log_df_4g['f_time_1'] = pd.to_datetime(log_df_4g['f_time'], unit='s').dt.tz_localize(pytz.utc).dt.tz_convert(
            pytz.timezone('Asia/Shanghai'))
        log_df_4g['finger_id'] = 'F' + log_df_4g['f_time_1'].dt.strftime('%Y%m%d%H') + '_' + log_df_4g['f_msisdn'].str[
                                                                                             -4:]
        log_df_4g['f_province'] = f_province
        log_df_4g['f_city'] = f_city
        log_df_4g['f_area'] = f_area
        log_df_4g['f_floor'] = f_floor
        log_df_4g['f_altitude'] = f_altitude
        log_df_4g['f_scenario'] = f_scenario
        log_df_4g['f_district'] = f_district
        log_df_4g['f_street'] = f_street
        log_df_4g['f_building'] = f_building
        log_df_4g['f_prru_id'] = 0
        log_df_4g['f_source'] = f_source
        log_df_4g['f_roaming_type'] = ''
        log_df_4g['f_phr'] = ''
        log_df_4g['f_enb_received_power'] = ''
        log_df_4g['f_ta'] = ''
        log_df_4g['f_aoa'] = ''
        log_df_4g['f_enb_id'] = log_df_4g['f_cell_id'] // 256
        num_list = []
        for i in range(len(log_df_4g)):
            num_list.append(np.count_nonzero(
                log_df_4g[['f_rsrp_n1', 'f_rsrp_n2', 'f_rsrp_n3', 'f_rsrp_n4', 'f_rsrp_n5',
                           'f_rsrp_n6', 'f_rsrp_n7', 'f_rsrp_n8']].isnull().values[i] == False))
        log_df_4g['f_neighbor_cell_number'] = num_list
        log_df_4g[['f_year', 'f_month', 'f_day']] = log_df_4g['f_time'].apply(convert_timestamp_to_date).to_list()
        # log_df_4g['f_cst_time'] = log_df_4g['pc_time']
        log_df_4g['f_eci'] = log_df_4g['f_cell_id']
        calculate_directions(log_df_4g)  # 计算行进方向。

        # SID暂时都赋值1
        log_df_4g.loc[:, 'f_sid'] = ''
        log_df_4g['f_pid'] = (log_df_4g.index + 1).astype(str)
        log_df_4g['f_device_brand'] = f_device_brand
        log_df_4g['f_device_model'] = f_device_model
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
        log_df_4g.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)

    def data_preprocessing_walktour_log_5g(self):
        """
        预处理walktour 测试 log原始数据文件
        :return: 返回清洗完成的测试数据文件，文件后缀格式csv
        """
        import datetime
        # 获取当前日期
        current_date = datetime.date.today()
        # 格式化为年_月_日形式
        formatted_date = current_date.strftime("%Y_%m_%d")

        # file_name = self.file_name
        # file_name = '5G_WT_UEMR_Outdoor_DT_0825_UE2'
        file_name = '5G_WT_LOG_DT_{}_UE2_P40_2'.format(formatted_date)
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：iPhone13_1
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：iPhone13_2
        # f_msisdn = '533F8040D9351F4A9499FC7825805B14'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；P40_1
        f_msisdn = '7314E1BE6DF72134E285D6AC1A99D8B7'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；P40_2
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；三星 S22_1
        # f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；三星 S22_2
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 13_1
        # f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 13_2
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；IQOO9
        # f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Mate30
        # f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Mate40_1
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Mate40_2
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 12_1
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 12_2
        # f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：IQOO7_1
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；IQOO7_2
        # f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：荣耀90_1
        # f_msisdn = '154D4095974592369EC605DD5E8C0D24'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；荣耀90_2

        log_df_4g, log_df_5g = self.merge_xy_log()
        # log_df_5g.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)
        log_df_5g['PCTime_'] = DataPreprocessing.convert_datetime_to_timestamp(log_df_5g['PC Time'])
        log_df_5g = log_df_5g.groupby(log_df_5g['PCTime_']).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。
        # 定义要进行替换的正则表达式模式
        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_5g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_5g.columns]

        log_df_5g = log_df_5g.loc[:, ['IMEI',
                                      'IMSI',
                                      'NCI',
                                      'ts',
                                      'lon',
                                      'lat',
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
                                      'NCell7 -Beam PCI',
                                      'NCell7 -Beam NARFCN',
                                      'NCell7 -Beam SS-RSRP',
                                      'NCell7 -Beam SS-RSRQ',
                                      'NCell7 -Beam SS-SINR',
                                      'NCell8 -Beam PCI',
                                      'NCell8 -Beam NARFCN',
                                      'NCell8 -Beam SS-RSRP',
                                      'NCell8 -Beam SS-RSRQ',
                                      'NCell8 -Beam SS-SINR',
                                      'PCTime',
                                      'x_new',
                                      'y_new']]

        log_df_5g = log_df_5g.rename(
            columns={
                'IMSI': 'f_imsi',
                'IMEI': 'f_imei',
                'NCI': 'f_cell_id',
                'ts': 'f_time',
                'lon': 'f_longitude',
                'lat': 'f_latitude',
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
                'NCell7 -Beam NARFCN': 'f_freq_n7',
                'NCell7 -Beam PCI': 'f_pci_n7',
                'NCell7 -Beam SS-RSRP': 'f_rsrp_n7',
                'NCell7 -Beam SS-RSRQ': 'f_rsrq_n7',
                'NCell7 -Beam SS-SINR': 'f_sinr_n7',
                'NCell8 -Beam NARFCN': 'f_freq_n8',
                'NCell8 -Beam PCI': 'f_pci_n8',
                'NCell8 -Beam SS-RSRP': 'f_rsrp_n8',
                'NCell8 -Beam SS-RSRQ': 'f_rsrq_n8',
                'NCell8 -Beam SS-SINR': 'f_sinr_n8',
                'PCTime': 'pc_time',
                'x_new': 'f_x',
                'y_new': 'f_y',
            })
        duplicate_columns = log_df_5g.columns[log_df_5g.columns.duplicated()]
        log_df_5g = log_df_5g.loc[:, ~log_df_5g.columns.duplicated()]
        log_df_5g['f_time_1'] = pd.to_datetime(log_df_5g['f_time'], unit='s').dt.tz_localize(
            pytz.utc).dt.tz_convert(pytz.timezone('Asia/Shanghai'))
        log_df_5g['f_msisdn'] = f_msisdn
        log_df_5g['finger_id'] = 'F' + log_df_5g['f_time_1'].dt.strftime('%Y%m%d%H') + '_' + log_df_5g['f_msisdn'].str[
                                                                                             -4:]
        log_df_5g['f_imsi'] = np.array(log_df_5g['f_imsi'])
        log_df_5g['f_province'] = f_province
        log_df_5g['f_city'] = f_city
        log_df_5g['f_area'] = f_area
        log_df_5g['f_floor'] = f_floor
        log_df_5g['f_altitude'] = f_altitude
        log_df_5g['f_scenario'] = f_scenario
        log_df_5g['f_district'] = f_district
        log_df_5g['f_street'] = f_street
        log_df_5g['f_building'] = f_building
        log_df_5g['f_prru_id'] = 0
        log_df_5g['f_source'] = f_source
        log_df_5g['f_roaming_type'] = ''
        log_df_5g['f_phr'] = ''
        log_df_5g['f_enb_received_power'] = ''
        log_df_5g['f_ta'] = ''
        log_df_5g['f_aoa'] = ''
        log_df_5g['f_gnb_id'] = log_df_5g['f_cell_id'] // 4096
        num_list = []
        for i in range(len(log_df_5g)):
            num_list.append(np.count_nonzero(
                log_df_5g[['f_rsrp_n1', 'f_rsrp_n2', 'f_rsrp_n3', 'f_rsrp_n4', 'f_rsrp_n5', 'f_rsrp_n6', 'f_rsrp_n7',
                           'f_rsrp_n8']].isnull().values[i] == False))
        log_df_5g['f_neighbor_cell_number'] = num_list
        log_df_5g[['f_year', 'f_month', 'f_day']] = log_df_5g['f_time'].apply(convert_timestamp_to_date).to_list()
        # log_df_5g['f_cst_time'] = log_df_5g['pc_time']
        log_df_5g['f_eci'] = log_df_5g['f_cell_id']

        calculate_directions(log_df_5g)  # 计算行进方向。

        # SID暂时都赋值1
        log_df_5g.loc[:, 'f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)
        log_df_5g['f_device_brand'] = f_device_brand
        log_df_5g['f_device_model'] = f_device_model
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

        #  标题统一小写
        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        log_df_5g.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)

    def data_preprocessing_walktour_wtlog_5g(self):
        """
        预处理wt 测试 log原始数据文件
        :return: 返回清洗完成的测试数据文件，文件后缀格式csv
        """
        import datetime
        # 获取当前日期
        current_date = datetime.date.today()
        # 格式化为年_月_日形式
        formatted_date = current_date.strftime("%Y_%m_%d")

        # file_name = self.file_name
        # file_name = '5G_WT_UEMR_Outdoor_DT_0825_UE2'
        file_name = '5G_WT_LOG_DT_{}_UE2_P40_2'.format(formatted_date)
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：iPhone13_1
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：iPhone13_2
        # f_msisdn = '533F8040D9351F4A9499FC7825805B14'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；P40_1
        f_msisdn = '7314E1BE6DF72134E285D6AC1A99D8B7'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；P40_2
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；三星 S22_1
        # f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；三星 S22_2
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 13_1
        # f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 13_2
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；IQOO9
        # f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Mate30
        # f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Mate40_1
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Mate40_2
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 12_1
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 12_2
        # f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：IQOO7_1
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；IQOO7_2
        # f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：荣耀90_1
        # f_msisdn = '154D4095974592369EC605DD5E8C0D24'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；荣耀90_2

        log_df_4g, log_df_5g = self.merge_xy_log()
        # log_df_5g.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)
        log_df_5g['PCTime_'] = DataPreprocessing.convert_datetime_to_timestamp(log_df_5g['PC Time'])
        log_df_5g = log_df_5g.groupby(log_df_5g['PCTime_']).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。
        # 定义要进行替换的正则表达式模式
        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_5g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_5g.columns]

        log_df_5g = log_df_5g.loc[:, ['IMEI',
                                      'IMSI',
                                      'NCI',
                                      'ts',
                                      'lon',
                                      'lat',
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
                                      'NCell7 -Beam PCI',
                                      'NCell7 -Beam NARFCN',
                                      'NCell7 -Beam SS-RSRP',
                                      'NCell7 -Beam SS-RSRQ',
                                      'NCell7 -Beam SS-SINR',
                                      'NCell8 -Beam PCI',
                                      'NCell8 -Beam NARFCN',
                                      'NCell8 -Beam SS-RSRP',
                                      'NCell8 -Beam SS-RSRQ',
                                      'NCell8 -Beam SS-SINR',
                                      'PCTime',
                                      'x_new',
                                      'y_new']]

        log_df_5g = log_df_5g.rename(
            columns={
                'IMSI': 'f_imsi',
                'IMEI': 'f_imei',
                'NCI': 'f_cell_id',
                'ts': 'f_time',
                'lon': 'f_longitude',
                'lat': 'f_latitude',
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
                'NCell7 -Beam NARFCN': 'f_freq_n7',
                'NCell7 -Beam PCI': 'f_pci_n7',
                'NCell7 -Beam SS-RSRP': 'f_rsrp_n7',
                'NCell7 -Beam SS-RSRQ': 'f_rsrq_n7',
                'NCell7 -Beam SS-SINR': 'f_sinr_n7',
                'NCell8 -Beam NARFCN': 'f_freq_n8',
                'NCell8 -Beam PCI': 'f_pci_n8',
                'NCell8 -Beam SS-RSRP': 'f_rsrp_n8',
                'NCell8 -Beam SS-RSRQ': 'f_rsrq_n8',
                'NCell8 -Beam SS-SINR': 'f_sinr_n8',
                'PCTime': 'pc_time',
                'x_new': 'f_x',
                'y_new': 'f_y',
            })
        duplicate_columns = log_df_5g.columns[log_df_5g.columns.duplicated()]
        log_df_5g = log_df_5g.loc[:, ~log_df_5g.columns.duplicated()]
        log_df_5g['f_time_1'] = pd.to_datetime(log_df_5g['f_time'], unit='s').dt.tz_localize(
            pytz.utc).dt.tz_convert(pytz.timezone('Asia/Shanghai'))
        log_df_5g['f_msisdn'] = f_msisdn
        log_df_5g['finger_id'] = 'F' + log_df_5g['f_time_1'].dt.strftime('%Y%m%d%H') + '_' + log_df_5g['f_msisdn'].str[
                                                                                             -4:]
        log_df_5g['f_imsi'] = np.array(log_df_5g['f_imsi'])
        log_df_5g['f_province'] = f_province
        log_df_5g['f_city'] = f_city
        log_df_5g['f_area'] = f_area
        log_df_5g['f_floor'] = f_floor
        log_df_5g['f_altitude'] = f_altitude
        log_df_5g['f_scenario'] = f_scenario
        log_df_5g['f_district'] = f_district
        log_df_5g['f_street'] = f_street
        log_df_5g['f_building'] = f_building
        log_df_5g['f_prru_id'] = 0
        log_df_5g['f_source'] = f_source
        log_df_5g['f_roaming_type'] = ''
        log_df_5g['f_phr'] = ''
        log_df_5g['f_enb_received_power'] = ''
        log_df_5g['f_ta'] = ''
        log_df_5g['f_aoa'] = ''
        log_df_5g['f_gnb_id'] = log_df_5g['f_cell_id'] // 4096
        num_list = []
        for i in range(len(log_df_5g)):
            num_list.append(np.count_nonzero(
                log_df_5g[['f_rsrp_n1', 'f_rsrp_n2', 'f_rsrp_n3', 'f_rsrp_n4', 'f_rsrp_n5', 'f_rsrp_n6', 'f_rsrp_n7',
                           'f_rsrp_n8']].isnull().values[i] == False))
        log_df_5g['f_neighbor_cell_number'] = num_list
        log_df_5g[['f_year', 'f_month', 'f_day']] = log_df_5g['f_time'].apply(convert_timestamp_to_date).to_list()
        # log_df_5g['f_cst_time'] = log_df_5g['pc_time']
        log_df_5g['f_eci'] = log_df_5g['f_cell_id']

        calculate_directions(log_df_5g)  # 计算行进方向。

        # SID暂时都赋值1
        log_df_5g.loc[:, 'f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)
        log_df_5g['f_device_brand'] = f_device_brand
        log_df_5g['f_device_model'] = f_device_model
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

        #  标题统一小写
        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        log_df_5g.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)

    def data_preprocessing_uemr_4g(self):
        """
        预处理室内log原始数据文件
        :return: 返回清洗完成的uemr5G数据文件，文件后缀格式csv
        """
        # file_name = self.file_name
        file_name = '5G_WT_UEMR_Indoor_DT_0720_UE2'
        if os.path.exists(self.src_file_5g):
            uemr_data_df = pd.read_excel(self.src_file_5g, header=0, sheet_name='5G')
            # uemr_data_df = pd.read_csv(self.src_file_5g, encoding='gbk', header=0)
            if len(uemr_data_df) > 0:
                uemr_data_df = uemr_data_df.loc[:, [
                                                       'NCI',
                                                       'PCell1 -Beam NARFCN',
                                                       'PCell1 -Beam PCI',
                                                       'PCell1 -Beam SS-RSRP',
                                                       'PCell1 -Beam SS-RSRQ',
                                                       'PCell1 -Beam SS-SINR',
                                                       # 'PCell2 -Beam NARFCN',
                                                       # 'PCell2 -Beam PCI',
                                                       # 'PCell2 -Beam SS-RSRP',
                                                       # 'PCell2 -Beam SS-RSRQ',
                                                       # 'PCell2 -Beam SS-SINR',
                                                       # 'PCell3 -Beam NARFCN',
                                                       # 'PCell3 -Beam PCI',
                                                       # 'PCell3 -Beam SS-RSRP',
                                                       # 'PCell3 -Beam SS-RSRQ',
                                                       # 'PCell3 -Beam SS-SINR',
                                                       # 'PCell4 -Beam NARFCN',
                                                       # 'PCell4 -Beam PCI',
                                                       # 'PCell4 -Beam SS-RSRP',
                                                       # 'PCell4 -Beam SS-RSRQ',
                                                       # 'PCell4 -Beam SS-SINR',
                                                       # 'PCell5 -Beam NARFCN',
                                                       # 'PCell5 -Beam PCI',
                                                       # 'PCell5 -Beam SS-RSRP',
                                                       # 'PCell5 -Beam SS-RSRQ',
                                                       # 'PCell5 -Beam SS-SINR',
                                                       # 'PCell6 -Beam NARFCN',
                                                       # 'PCell6 -Beam PCI',
                                                       # 'PCell6 -Beam SS-RSRP',
                                                       # 'PCell6 -Beam SS-RSRQ',
                                                       # 'PCell6 -Beam SS-SINR',
                                                       # 'PCell7 -Beam NARFCN',
                                                       # 'PCell7 -Beam PCI',
                                                       # 'PCell7 -Beam SS-RSRP',
                                                       # 'PCell7 -Beam SS-RSRQ',
                                                       # 'PCell7 -Beam SS-SINR',
                                                       # 'PCell8 -Beam NARFCN',
                                                       # 'PCell8 -Beam PCI',
                                                       # 'PCell8 -Beam SS-RSRP',
                                                       # 'PCell8 -Beam SS-RSRQ',
                                                       # 'PCell8 -Beam SS-SINR',
                                                       'NCell1 -Beam PCI',
                                                       'NCell2 -Beam PCI',
                                                       'NCell3 -Beam PCI',
                                                       'NCell4 -Beam PCI',
                                                       'NCell5 -Beam PCI',
                                                       'NCell6 -Beam PCI',
                                                       'NCell7 -Beam PCI',
                                                       'NCell8 -Beam PCI',
                                                       'NCell1 -Beam NARFCN',
                                                       'NCell2 -Beam NARFCN',
                                                       'NCell3 -Beam NARFCN',
                                                       'NCell4 -Beam NARFCN',
                                                       'NCell5 -Beam NARFCN',
                                                       'NCell6 -Beam NARFCN',
                                                       'NCell7 -Beam NARFCN',
                                                       'NCell8 -Beam NARFCN',
                                                       'NCell1 -Beam SS-RSRP',
                                                       'NCell1 -Beam SS-RSRQ',
                                                       'NCell1 -Beam SS-SINR',
                                                       'NCell2 -Beam SS-RSRP',
                                                       'NCell2 -Beam SS-RSRQ',
                                                       'NCell2 -Beam SS-SINR',
                                                       'NCell3 -Beam SS-RSRP',
                                                       'NCell3 -Beam SS-RSRQ',
                                                       'NCell3 -Beam SS-SINR',
                                                       'NCell4 -Beam SS-RSRP',
                                                       'NCell4 -Beam SS-RSRQ',
                                                       'NCell4 -Beam SS-SINR',
                                                       'NCell5 -Beam SS-RSRP',
                                                       'NCell5 -Beam SS-RSRQ',
                                                       'NCell5 -Beam SS-SINR',
                                                       'NCell6 -Beam SS-RSRP',
                                                       'NCell6 -Beam SS-RSRQ',
                                                       'NCell6 -Beam SS-SINR',
                                                       'NCell7 -Beam SS-RSRP',
                                                       'NCell7 -Beam SS-RSRQ',
                                                       'NCell7 -Beam SS-SINR',
                                                       'NCell8 -Beam SS-RSRP',
                                                       'NCell8 -Beam SS-RSRQ',
                                                       'NCell8 -Beam SS-SINR',
                                                       'PC Time',
                                                       'x',
                                                       'y',
                                                       'IMEI',
                                                       'IMSI',
                                                       'SID',
                                                       'PID',
                                                       'NR gNodeB ID(24bit)',
                                                       # 'gNB_ID',
                                                       'zone',
                                                       'floor',
                                                       'scenario',
                                                       'direction',
                                                       'Longitude',
                                                       'Latitude'
                                                       # '区域',
                                                       # '楼层',
                                                       # '场景',
                                                       # '方向',
                                                       # 'data.nameValuePairs.CELLID',
                                                       # 'data.nameValuePairs.TAC',
                                                       # 'data.nameValuePairs.FREQ',
                                                       # 'data.nameValuePairs.RSRP',
                                                       # 'data.nameValuePairs.SINR',
                                                       # 'data.nameValuePairs.PCID',
                                                       # 'data.nameValuePairs.RSRQ',
                                                       # 'currentTime'
                                                   ]]
                uemr_data_df = uemr_data_df.rename(
                    columns={
                        'NCI': 'u_cell_id',

                        'PCell1 -Beam NARFCN': 'u_freq',
                        'PCell1 -Beam PCI': 'u_pci',
                        'PCell1 -Beam SS-RSRP': 'u_rsrp',
                        'PCell1 -Beam SS-RSRQ': 'u_rsrq',
                        'PCell1 -Beam SS-SINR': 'u_sinr',

                        # 'PCell1 -Beam NARFCN': 'u_freq_s1',
                        # 'PCell1 -Beam PCI': 'u_pci_s1',
                        # 'PCell1 -Beam SS-RSRP': 'u_rsrp_s1',
                        # 'PCell1 -Beam SS-RSRQ': 'u_rsrq_s1',
                        # 'PCell1 -Beam SS-SINR': 'u_sinr_s1',

                        # 'PCell2 -Beam NARFCN': 'u_freq_s2',
                        # 'PCell2 -Beam PCI': 'u_pci_s2',
                        # 'PCell2 -Beam SS-RSRP': 'u_rsrp_s2',
                        # 'PCell2 -Beam SS-RSRQ': 'u_rsrq_s2',
                        # 'PCell2 -Beam SS-SINR': 'u_sinr_s2',
                        #
                        # 'PCell3 -Beam NARFCN': 'u_freq_s3',
                        # 'PCell3 -Beam PCI': 'u_pci_s3',
                        # 'PCell3 -Beam SS-RSRP': 'u_rsrp_s3',
                        # 'PCell3 -Beam SS-RSRQ': 'u_rsrq_s3',
                        # 'PCell3 -Beam SS-SINR': 'u_sinr_s3',
                        #
                        # 'PCell4 -Beam NARFCN': 'u_freq_s4',
                        # 'PCell4 -Beam PCI': 'u_pci_s4',
                        # 'PCell4 -Beam SS-RSRP': 'u_rsrp_s4',
                        # 'PCell4 -Beam SS-RSRQ': 'u_rsrq_s4',
                        # 'PCell4 -Beam SS-SINR': 'u_sinr_s4',
                        #
                        # 'PCell5 -Beam NARFCN': 'u_freq_s5',
                        # 'PCell5 -Beam PCI': 'u_pci_s5',
                        # 'PCell5 -Beam SS-RSRP': 'u_rsrp_s5',
                        # 'PCell5 -Beam SS-RSRQ': 'u_rsrq_s5',
                        # 'PCell5 -Beam SS-SINR': 'u_sinr_s5',
                        #
                        # 'PCell6 -Beam NARFCN': 'u_freq_s6',
                        # 'PCell6 -Beam PCI': 'u_pci_s6',
                        # 'PCell6 -Beam SS-RSRP': 'u_rsrp_s6',
                        # 'PCell6 -Beam SS-RSRQ': 'u_rsrq_s6',
                        # 'PCell6 -Beam SS-SINR': 'u_sinr_s6',
                        #
                        # 'PCell7 -Beam NARFCN': 'u_freq_s7',
                        # 'PCell7 -Beam PCI': 'u_pci_s7',
                        # 'PCell7 -Beam SS-RSRP': 'u_rsrp_s7',
                        # 'PCell7 -Beam SS-RSRQ': 'u_rsrq_s7',
                        # 'PCell7 -Beam SS-SINR': 'u_sinr_s7',
                        #
                        # 'PCell8 -Beam NARFCN': 'u_freq_s8',
                        # 'PCell8 -Beam PCI': 'u_pci_s8',
                        # 'PCell8 -Beam SS-RSRP': 'u_rsrp_s8',
                        # 'PCell8 -Beam SS-RSRQ': 'u_rsrq_s8',
                        # 'PCell8 -Beam SS-SINR': 'u_sinr_s8',

                        'NCell1 -Beam PCI': 'u_pci_n1',
                        'NCell2 -Beam PCI': 'u_pci_n2',
                        'NCell3 -Beam PCI': 'u_pci_n3',
                        'NCell4 -Beam PCI': 'u_pci_n4',
                        'NCell5 -Beam PCI': 'u_pci_n5',
                        'NCell6 -Beam PCI': 'u_pci_n6',
                        'NCell7 -Beam PCI': 'u_pci_n7',
                        'NCell8 -Beam PCI': 'u_pci_n8',
                        'NCell1 -Beam NARFCN': 'u_freq_n1',
                        'NCell2 -Beam NARFCN': 'u_freq_n2',
                        'NCell3 -Beam NARFCN': 'u_freq_n3',
                        'NCell4 -Beam NARFCN': 'u_freq_n4',
                        'NCell5 -Beam NARFCN': 'u_freq_n5',
                        'NCell6 -Beam NARFCN': 'u_freq_n6',
                        'NCell7 -Beam NARFCN': 'u_freq_n7',
                        'NCell8 -Beam NARFCN': 'u_freq_n8',
                        'NCell1 -Beam SS-RSRP': 'u_rsrp_1',
                        'NCell1 -Beam SS-RSRQ': 'u_rsrq_1',
                        'NCell1 -Beam SS-SINR': 'u_sinr_1',
                        'NCell2 -Beam SS-RSRP': 'u_rsrp_2',
                        'NCell2 -Beam SS-RSRQ': 'u_rsrq_2',
                        'NCell2 -Beam SS-SINR': 'u_sinr_2',
                        'NCell3 -Beam SS-RSRP': 'u_rsrp_3',
                        'NCell3 -Beam SS-RSRQ': 'u_rsrq_3',
                        'NCell3 -Beam SS-SINR': 'u_sinr_3',
                        'NCell4 -Beam SS-RSRP': 'u_rsrp_4',
                        'NCell4 -Beam SS-RSRQ': 'u_rsrq_4',
                        'NCell4 -Beam SS-SINR': 'u_sinr_4',
                        'NCell5 -Beam SS-RSRP': 'u_rsrp_5',
                        'NCell5 -Beam SS-RSRQ': 'u_rsrq_5',
                        'NCell5 -Beam SS-SINR': 'u_sinr_5',
                        'NCell6 -Beam SS-RSRP': 'u_rsrp_6',
                        'NCell6 -Beam SS-RSRQ': 'u_rsrq_6',
                        'NCell6 -Beam SS-SINR': 'u_sinr_6',
                        'NCell7 -Beam SS-RSRP': 'u_rsrp_7',
                        'NCell7 -Beam SS-RSRQ': 'u_rsrq_7',
                        'NCell7 -Beam SS-SINR': 'u_sinr_7',
                        'NCell8 -Beam SS-RSRP': 'u_rsrp_8',
                        'NCell8 -Beam SS-RSRQ': 'u_rsrq_8',
                        'NCell8 -Beam SS-SINR': 'u_sinr_8',
                        'PC Time': 'time_u',
                        'x': 'u_x',
                        'y': 'u_y',
                        # '区域': 'zone',
                        # '楼层': 'floor',
                        # '场景': 'scenario',
                        # '方向': 'direction',
                        'NR gNodeB ID(24bit)': 'gNB_ID',
                        'Longitude': 'u_Longitude',
                        'Latitude': 'u_Latitude',

                    })

                # uemr_data_df['u_pci_n4'] = np.nan
                # uemr_data_df['u_pci_n5'] = np.nan
                # uemr_data_df['u_pci_n6'] = np.nan
                # uemr_data_df['u_pci_n7'] = np.nan
                # uemr_data_df['u_pci_n8'] = np.nan
                #
                # # uemr_data_df['u_freq_n4'] = np.nan
                # uemr_data_df['u_freq_n5'] = np.nan
                # uemr_data_df['u_freq_n6'] = np.nan
                # uemr_data_df['u_freq_n7'] = np.nan
                # uemr_data_df['u_freq_n8'] = np.nan
                #
                # # uemr_data_df['u_rsrp_4'] = np.nan
                # # uemr_data_df['u_rsrq_4'] = np.nan
                # # uemr_data_df['u_sinr_4'] = np.nan
                #
                # uemr_data_df['u_rsrp_5'] = np.nan
                # uemr_data_df['u_rsrq_5'] = np.nan
                # uemr_data_df['u_sinr_5'] = np.nan
                #
                # uemr_data_df['u_rsrp_6'] = np.nan
                # uemr_data_df['u_rsrq_6'] = np.nan
                # uemr_data_df['u_sinr_6'] = np.nan
                #
                # uemr_data_df['u_rsrp_7'] = np.nan
                # uemr_data_df['u_rsrq_7'] = np.nan
                # uemr_data_df['u_sinr_7'] = np.nan
                #
                # uemr_data_df['u_rsrp_8'] = np.nan
                # uemr_data_df['u_rsrq_8'] = np.nan
                # uemr_data_df['u_sinr_8'] = np.nan

                # uemr_data_df['SID'] = 1

                uemr_data_df['time_cst_u'] = uemr_data_df['time_u']
                # 时间转时间戳类型
                uemr_data_df['time_u'] = uemr_data_df['time_u'].apply(lambda x: time.mktime(x.timetuple()))
                # uemr_data_df['time_u'] = DataPreprocessing.convert_datetime_to_timestamp(uemr_data_df['time_cst_u'])

                # 填充缺失的cell_id
                DataPreprocessing.data_filling(uemr_data_df, 'u_cell_id')
                # 加自增序列
                # n = len(uemr_data_df) + 1
                # nlist = range(1, n)
                # uemr_data_df['PID'] = nlist
                # 删除x,y有空值的行
                uemr_data_df = uemr_data_df.dropna(subset={'u_x', 'u_y'}, how='any')
                # SID暂时都赋值1 有值就不赋值
                # uemr_data_df.loc[:, 'SID'] = 1
                num_list = []
                for i in range(len(uemr_data_df)):
                    num_list.append(np.count_nonzero(
                        uemr_data_df[['u_rsrp_1', 'u_rsrp_2', 'u_rsrp_3', 'u_rsrp_4', 'u_rsrp_5', 'u_rsrp_6',
                                      'u_rsrp_7', 'u_rsrp_8']].isnull().values[i] == False))
                uemr_data_df['neighbor_cell_number'] = num_list

            # 校验cell_id是否有交集
            # DataPreprocessing.check_cellId_intersection(uemr_data_df, 'uemr', 'IMSI')
            # uemr_data_df = uemr_data_df.groupby('IMSI').get_group(460001953133670)
            uemr_data_df = DataPreprocessing.group_by_IMSI(uemr_data_df, 'uemr', 'IMSI')
            print('分组后的uemr5GLog数据：\n' + str(uemr_data_df))
            # 删除重复行数据
            # uemr_data_df.to_csv("/Users/rainrain/Desktop/LAC/大兴机场/0719/0719_Indoor/ouput/" + file_name + "_重复数据.csv", encoding='utf-8', index=True)
            DataPreprocessing.determine_duplicate_data(uemr_data_df, 'uemr', '5G')
            uemr_data_df = uemr_data_df.drop_duplicates()
            # 标题统一小写
            uemr_data_df = uemr_data_df.rename(str.lower, axis='columns')
            uemr_data_df.to_csv("/Users/rainrain/Desktop/LAC/大兴机场/0719/0719_Indoor/ouput/" + file_name + ".csv",
                                encoding='utf-8', index=False)
            # 数据校验
            DataPreprocessing.data_quality_detection(
                '/Users/rainrain/Desktop/LAC/大兴机场/0719/0719_Indoor/ouput/' + file_name + '.csv', 'U', '5G',
                file_name)

    def data_preprocessing_uemr_5g(self):
        """
        预处理uemr服务器原始数据文件
        :return: 返回清洗完成的uemr5G数据文件，文件后缀格式csv
        """
        # file_name = self.file_name
        file_name = '5G_WT_UEMR_DT_0824_UE2'

        log_df_4g, log_df_5g = self.merge_xy_log()

        log_df_5g = log_df_5g.loc[:, ['PCTime', 'ts', 'IMEI', 'x_new', 'y_new', 'NCI']]
        log_df_5g['ts'] = log_df_5g['ts'] + 18
        log_df_5g = log_df_5g.rename(columns={
            'IMEI': 'f_imei',
            'x_new': 'f_x',
            'y_new': 'f_y',
            'NCI': 'f_NCI'
        })
        if os.path.exists(self.src_file_5g):
            uemr_data_df = pd.read_excel(self.src_file_5g, header=0, sheet_name='Sheet1')
            if len(uemr_data_df) > 0:
                uemr_data_df = uemr_data_df.loc[:, [
                                                       'uemr5g.local_province',
                                                       'uemr5g.local_city',
                                                       'uemr5g.roaming_type',
                                                       'uemr5g.imsi',
                                                       'uemr5g.msisdn',
                                                       'uemr5g.cell_id',
                                                       'uemr5g.gnb_id',
                                                       'uemr5g.create_time',
                                                       'uemr5g.startlocation_longitude',
                                                       'uemr5g.startlocation_latitude',
                                                       'uemr5g.startlocation_altitude',
                                                       'uemr5g.phr',
                                                       'uemr5g.enb_received_power',
                                                       'uemr5g.ta',
                                                       'uemr5g.aoa',
                                                       'uemr5g.servingcell_1_pci',
                                                       'uemr5g.servingcell_1_freq',
                                                       'uemr5g.servingcell_1_ssb_rsrp',
                                                       'uemr5g.servingcell_1_ssb_rsrq',
                                                       'uemr5g.servingcell_1_ssb_sinr',
                                                       'uemr5g.neighbor_nr_cell_1_pci',
                                                       'uemr5g.neighbor_nr_cell_2_pci',
                                                       'uemr5g.neighbor_nr_cell_3_pci',
                                                       'uemr5g.neighbor_nr_cell_4_pci',
                                                       'uemr5g.neighbor_nr_cell_5_pci',
                                                       'uemr5g.neighbor_nr_cell_6_pci',
                                                       'uemr5g.neighbor_nr_cell_7_pci',
                                                       'uemr5g.neighbor_nr_cell_8_pci',
                                                       'uemr5g.neighbor_nr_cell_1_freq',
                                                       'uemr5g.neighbor_nr_cell_2_freq',
                                                       'uemr5g.neighbor_nr_cell_3_freq',
                                                       'uemr5g.neighbor_nr_cell_4_freq',
                                                       'uemr5g.neighbor_nr_cell_5_freq',
                                                       'uemr5g.neighbor_nr_cell_6_freq',
                                                       'uemr5g.neighbor_nr_cell_7_freq',
                                                       'uemr5g.neighbor_nr_cell_8_freq',
                                                       'uemr5g.neighbor_nr_cell_1_ssb_rsrp',
                                                       'uemr5g.neighbor_nr_cell_1_ssb_rsrq',
                                                       'uemr5g.neighbor_nr_cell_1_ssb_sinr',
                                                       'uemr5g.neighbor_nr_cell_2_ssb_rsrp',
                                                       'uemr5g.neighbor_nr_cell_2_ssb_rsrq',
                                                       'uemr5g.neighbor_nr_cell_2_ssb_sinr',
                                                       'uemr5g.neighbor_nr_cell_3_ssb_rsrp',
                                                       'uemr5g.neighbor_nr_cell_3_ssb_rsrq',
                                                       'uemr5g.neighbor_nr_cell_3_ssb_sinr',
                                                       'uemr5g.neighbor_nr_cell_4_ssb_rsrp',
                                                       'uemr5g.neighbor_nr_cell_4_ssb_rsrq',
                                                       'uemr5g.neighbor_nr_cell_4_ssb_sinr',
                                                       'uemr5g.neighbor_nr_cell_5_ssb_rsrp',
                                                       'uemr5g.neighbor_nr_cell_5_ssb_rsrq',
                                                       'uemr5g.neighbor_nr_cell_5_ssb_sinr',
                                                       'uemr5g.neighbor_nr_cell_6_ssb_rsrp',
                                                       'uemr5g.neighbor_nr_cell_6_ssb_rsrq',
                                                       'uemr5g.neighbor_nr_cell_6_ssb_sinr',
                                                       'uemr5g.neighbor_nr_cell_7_ssb_rsrp',
                                                       'uemr5g.neighbor_nr_cell_7_ssb_rsrq',
                                                       'uemr5g.neighbor_nr_cell_7_ssb_sinr',
                                                       'uemr5g.neighbor_nr_cell_8_ssb_rsrp',
                                                       'uemr5g.neighbor_nr_cell_8_ssb_rsrq',
                                                       'uemr5g.neighbor_nr_cell_8_ssb_sinr',
                                                       'uemr5g.neighbor_nr_cell_number',
                                                       'uemr5g.year',
                                                       'uemr5g.month',
                                                       'uemr5g.day'
                                                       # 'x_new',
                                                       # 'y_new'
                                                       # 'uemr5g.f_cst_time'
                                                       # 'zone',
                                                       # 'floor',
                                                       # 'scenario',
                                                       # 'direction',
                                                   ]]
                uemr_data_df = uemr_data_df.rename(
                    columns={
                        'uemr5g.local_province': 'f_province',
                        'uemr5g.local_city': 'f_city',
                        'uemr5g.roaming_type': 'f_roaming_type',
                        'uemr5g.imsi': 'f_imsi',
                        'uemr5g.msisdn': 'f_msisdn',
                        'uemr5g.cell_id': 'f_cell_id',
                        'uemr5g.gnb_id': 'f_gnb_id',
                        'uemr5g.create_time': 'f_time',
                        'uemr5g.startlocation_longitude': 'f_longitude',
                        'uemr5g.startlocation_latitude': 'f_latitude',
                        'uemr5g.startlocation_altitude': 'f_altitude',
                        'uemr5g.phr': 'f_phr',
                        'uemr5g.enb_received_power': 'f_enb_received_power',
                        'uemr5g.ta': 'f_ta',
                        'uemr5g.aoa': 'f_aoa',
                        'uemr5g.neighbor_nr_cell_number': 'f_neighbor_cell_number',
                        'uemr5g.servingcell_1_freq': 'f_freq',
                        'uemr5g.servingcell_1_pci': 'f_pci',
                        'uemr5g.servingcell_1_ssb_rsrp': 'f_rsrp',
                        'uemr5g.servingcell_1_ssb_rsrq': 'f_rsrq',
                        'uemr5g.servingcell_1_ssb_sinr': 'f_sinr',
                        'uemr5g.neighbor_nr_cell_1_pci': 'f_pci_n1',
                        'uemr5g.neighbor_nr_cell_2_pci': 'f_pci_n2',
                        'uemr5g.neighbor_nr_cell_3_pci': 'f_pci_n3',
                        'uemr5g.neighbor_nr_cell_4_pci': 'f_pci_n4',
                        'uemr5g.neighbor_nr_cell_5_pci': 'f_pci_n5',
                        'uemr5g.neighbor_nr_cell_6_pci': 'f_pci_n6',
                        'uemr5g.neighbor_nr_cell_7_pci': 'f_pci_n7',
                        'uemr5g.neighbor_nr_cell_8_pci': 'f_pci_n8',
                        'uemr5g.neighbor_nr_cell_1_freq': 'f_freq_n1',
                        'uemr5g.neighbor_nr_cell_2_freq': 'f_freq_n2',
                        'uemr5g.neighbor_nr_cell_3_freq': 'f_freq_n3',
                        'uemr5g.neighbor_nr_cell_4_freq': 'f_freq_n4',
                        'uemr5g.neighbor_nr_cell_5_freq': 'f_freq_n5',
                        'uemr5g.neighbor_nr_cell_6_freq': 'f_freq_n6',
                        'uemr5g.neighbor_nr_cell_7_freq': 'f_freq_n7',
                        'uemr5g.neighbor_nr_cell_8_freq': 'f_freq_n8',
                        'uemr5g.neighbor_nr_cell_1_ssb_rsrp': 'f_rsrp_n1',
                        'uemr5g.neighbor_nr_cell_1_ssb_rsrq': 'f_rsrq_n1',
                        'uemr5g.neighbor_nr_cell_1_ssb_sinr': 'f_sinr_n1',
                        'uemr5g.neighbor_nr_cell_2_ssb_rsrp': 'f_rsrp_n2',
                        'uemr5g.neighbor_nr_cell_2_ssb_rsrq': 'f_rsrq_n2',
                        'uemr5g.neighbor_nr_cell_2_ssb_sinr': 'f_sinr_n2',
                        'uemr5g.neighbor_nr_cell_3_ssb_rsrp': 'f_rsrp_n3',
                        'uemr5g.neighbor_nr_cell_3_ssb_rsrq': 'f_rsrq_n3',
                        'uemr5g.neighbor_nr_cell_3_ssb_sinr': 'f_sinr_n3',
                        'uemr5g.neighbor_nr_cell_4_ssb_rsrp': 'f_rsrp_n4',
                        'uemr5g.neighbor_nr_cell_4_ssb_rsrq': 'f_rsrq_n4',
                        'uemr5g.neighbor_nr_cell_4_ssb_sinr': 'f_sinr_n4',
                        'uemr5g.neighbor_nr_cell_5_ssb_rsrp': 'f_rsrp_n5',
                        'uemr5g.neighbor_nr_cell_5_ssb_rsrq': 'f_rsrq_n5',
                        'uemr5g.neighbor_nr_cell_5_ssb_sinr': 'f_sinr_n5',
                        'uemr5g.neighbor_nr_cell_6_ssb_rsrp': 'f_rsrp_n6',
                        'uemr5g.neighbor_nr_cell_6_ssb_rsrq': 'f_rsrq_n6',
                        'uemr5g.neighbor_nr_cell_6_ssb_sinr': 'f_sinr_n6',
                        'uemr5g.neighbor_nr_cell_7_ssb_rsrp': 'f_rsrp_n7',
                        'uemr5g.neighbor_nr_cell_7_ssb_rsrq': 'f_rsrq_n7',
                        'uemr5g.neighbor_nr_cell_7_ssb_sinr': 'f_sinr_n7',
                        'uemr5g.neighbor_nr_cell_8_ssb_rsrp': 'f_rsrp_n8',
                        'uemr5g.neighbor_nr_cell_8_ssb_rsrq': 'f_rsrq_n8',
                        'uemr5g.neighbor_nr_cell_8_ssb_sinr': 'f_sinr_n8',
                        'uemr5g.year': 'f_year',
                        'uemr5g.month': 'f_month',
                        'uemr5g.day': 'f_day',
                    })
                uemr_data_df['f_province'] = f_province
                uemr_data_df['f_city'] = f_city
                uemr_data_df['f_area'] = f_area
                uemr_data_df['f_floor'] = f_floor
                uemr_data_df['f_altitude'] = f_altitude
                uemr_data_df['f_scenario'] = f_scenario

                # uemr_data_df = calculate_directions(uemr_data_df)
                # uemr_data_df['f_direction'] = uemr_data_df['f_direction'].apply(calculate_directions)
                # 截取 f_time 的前 10 位
                uemr_data_df['f_time_ts'] = uemr_data_df['f_time'].map(lambda x: int(str(x)[0:10]))

                calculate_directions(log_df_5g)  # 计算行进方向。
                log_df_5g['ts'] = log_df_5g['ts'].map(lambda x: int(str(x)[0:10]))
                uemr_data_df = pd.merge(uemr_data_df, log_df_5g, left_on="f_time_ts", right_on="ts", how='left')
                uemr_data_df = uemr_data_df.drop(['ts', 'f_time_ts'], axis=1)

                uemr_data_df['f_district'] = f_district
                uemr_data_df['f_street'] = f_street
                uemr_data_df['f_building'] = f_building
                uemr_data_df['f_prru_id'] = 0
                uemr_data_df['f_source'] = f_source
                # SID暂时都赋值1
                uemr_data_df.loc[:, 'f_sid'] = 1
                uemr_data_df['f_pid'] = (uemr_data_df.index + 1).astype(str)
                uemr_data_df['f_device_brand'] = f_device_brand
                uemr_data_df['f_device_model'] = f_device_model

                #
                uemr_data_df['f_time_1'] = pd.to_datetime(uemr_data_df['f_time'], unit='ms').dt.tz_localize(
                    pytz.utc).dt.tz_convert(pytz.timezone('Asia/Shanghai'))
                uemr_data_df['finger_id'] = 'F' + uemr_data_df['f_time_1'].dt.strftime('%Y%m%d%H') + '_' + uemr_data_df[
                                                                                                               'f_msisdn'].str[
                                                                                                           -4:]
                # uemr_data_df['f_time_1'] = pd.to_datetime(uemr_data_df['f_time'], unit='ms')
                # uemr_data_df['f_time_1'] = uemr_data_df['f_time_1'].dt.tz_localize(pytz.utc)
                # uemr_data_df['f_time_1'] = uemr_data_df['f_time_1'].dt.tz_convert(pytz.timezone('Asia/Shanghai'))
                # uemr_data_df['finger_id'] = 'F' + uemr_data_df['f_time_1'].dt.strftime('%Y%m%d') + '_' + uemr_data_df['f_msisdn'].str[-4:]

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
                               'f_month', 'f_day', 'PCTime', 'f_NCI', 'f_x', 'f_y', 'f_source',
                               'f_sid', 'f_pid', 'f_direction', 'f_device_brand', 'f_device_model']
                uemr_data_df = uemr_data_df.reindex(columns=new_columns)

                # 判断 5G UEMR 中的f_freq字段是否存在 513000 的值，若存在，将值修改为 504990
                uemr_data_df.loc[uemr_data_df['f_freq'] == 513000, 'f_freq'] = 504990

                # 5g范围值
                uemr_data_df['f_rsrp'] = uemr_data_df['f_rsrp'] - 156
                uemr_data_df['f_rsrq'] = (uemr_data_df['f_rsrq'] - 87) / 2
                uemr_data_df['f_sinr'] = (uemr_data_df['f_sinr'] - 46) / 2
                # 邻区数处理
                for i in range(1, 9):
                    # 5g范围值 指纹
                    uemr_data_df[f'f_rsrp_n{i}'] = uemr_data_df[f'f_rsrp_n{i}'] - 156
                    uemr_data_df[f'f_rsrq_n{i}'] = (uemr_data_df[f'f_rsrq_n{i}'] - 87) / 2
                    uemr_data_df[f'f_sinr_n{i}'] = (uemr_data_df[f'f_sinr_n{i}'] - 46) / 2

                # uemr_data_df['time_cst_u'] = pd.to_datetime(uemr_data_df['time_u'])
                # 时间戳转时间类型
                # uemr_data_df['f_cst_time'] = pd.to_datetime(
                #     DataPreprocessing.convert_timestamp_to_datetime(uemr_data_df['f_time']))
                # 填充缺失的cell_id
                DataPreprocessing.data_filling(uemr_data_df, 'f_cell_id')
                # 填充缺失的neighbor_cell_number为0
                DataPreprocessing.neighbor_cell_number_filling(uemr_data_df, 'f_neighbor_cell_number')
                # 删除重复行数据
                DataPreprocessing.determine_duplicate_data(uemr_data_df, 'uemr', '5G')
                uemr_data_df = uemr_data_df.drop_duplicates()
                # 删除空值
                # uemr_data_df = uemr_data_df.dropna(subset={'u_x', 'u_y'}, how='any')
                # SID暂时都赋值1
                # uemr_data_df.loc[:, 'f_SID'] = 1
                uemr_data_df['f_pid'] = (uemr_data_df.index + 1).astype(str)
                # 校验cell_id是否有交集
                # DataPreprocessing.check_cellId_intersection(uemr_data_df, 'uemr', 'msisdn')
                # 根据号码进行分组
                # uemr_data_df = uemr_data_df.groupby('f_msisdn').get_group('603FC2DDB848AFF4651DDA25608B39B9')
                # uemr_data_df = DataPreprocessing.group_by_IMSI(uemr_data_df, 'uemr', 'msisdn')
                print('分组后的指纹5G数据：\n' + str(uemr_data_df))
                # 标题统一小写
                uemr_data_df = uemr_data_df.rename(str.lower, axis='columns')
                uemr_data_df.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)
                # uemr_data_df.to_csv(output_folder + file_name + ".csv",
                #                  encoding='utf-8', index=False)

                # 数据校验
                # DataPreprocessing.data_quality_detection('/Users/rainrain/Desktop/LAC/大兴机场/0719/0719_Indoor/ouput/' + file_name + '.csv', 'U', '5G', file_name)

    def data_preprocessing_wtlog_5g(self):
        """
        预处理wt 测试 log原始数据文件
        :return: 返回清洗完成的测试数据文件，文件后缀格式csv
        """
        import datetime
        # 获取当前日期
        current_date = datetime.date.today()
        # 格式化为年_月_日形式
        formatted_date = current_date.strftime("%Y_%m_%d")

        # file_name = self.file_name
        # file_name = '5G_WT_UEMR_Outdoor_DT_0825_UE2'
        file_name = '5G_WT_LOG_DT_{}_UE2_OPPO_2'.format(formatted_date)
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：oppo_Reno8_1
        f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：oppo_Reno8_2

        log_df_4g, log_df_5g = self.merge_xy_log()

        # log_df_5g.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)
        log_df_5g['PCTime_'] = DataPreprocessing.convert_datetime_to_timestamp_v1(log_df_5g['pc_time'])
        log_df_5g = log_df_5g.groupby(log_df_5g['PCTime_']).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。
        # 定义要进行替换的正则表达式模式
        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_5g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_5g.columns]
        # print(log_df_5g.columns)
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
        duplicate_columns = log_df_5g.columns[log_df_5g.columns.duplicated()]
        log_df_5g = log_df_5g.loc[:, ~log_df_5g.columns.duplicated()]
        log_df_5g['f_time_1'] = pd.to_datetime(log_df_5g['f_time'], unit='s').dt.tz_localize(
            pytz.utc).dt.tz_convert(pytz.timezone('Asia/Shanghai'))
        log_df_5g['f_msisdn'] = f_msisdn
        log_df_5g['finger_id'] = 'F' + log_df_5g['f_time_1'].dt.strftime('%Y%m%d%H') + '_' + log_df_5g['f_msisdn'].str[
                                                                                             -4:]
        log_df_5g['f_imsi'] = np.array(log_df_5g['f_imsi'])
        log_df_5g['f_province'] = f_province
        log_df_5g['f_city'] = f_city
        log_df_5g['f_area'] = f_area
        log_df_5g['f_floor'] = f_floor
        log_df_5g['f_altitude'] = f_altitude
        log_df_5g['f_scenario'] = f_scenario
        log_df_5g['f_district'] = f_district
        log_df_5g['f_street'] = f_street
        log_df_5g['f_building'] = f_building
        log_df_5g['f_prru_id'] = 0
        log_df_5g['f_source'] = f_source
        log_df_5g['f_roaming_type'] = ''
        log_df_5g['f_phr'] = ''
        log_df_5g['f_enb_received_power'] = ''
        log_df_5g['f_ta'] = ''
        log_df_5g['f_aoa'] = ''
        log_df_5g['f_gnb_id'] = log_df_5g['f_cell_id'] // 4096
        num_list = []
        for i in range(len(log_df_5g)):
            num_list.append(np.count_nonzero(
                log_df_5g[['f_rsrp_n1', 'f_rsrp_n2', 'f_rsrp_n3', 'f_rsrp_n4', 'f_rsrp_n5', 'f_rsrp_n6', 'f_rsrp_n7',
                           'f_rsrp_n8']].isnull().values[i] == False))
        log_df_5g['f_neighbor_cell_number'] = num_list
        log_df_5g[['f_year', 'f_month', 'f_day']] = log_df_5g['f_time'].apply(convert_timestamp_to_date).to_list()
        # log_df_5g['f_cst_time'] = log_df_5g['pc_time']
        log_df_5g['f_eci'] = log_df_5g['f_cell_id']

        calculate_directions(log_df_5g)  # 计算行进方向。

        # SID暂时都赋值1
        log_df_5g.loc[:, 'f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)
        log_df_5g['f_device_brand'] = f_device_brand
        log_df_5g['f_device_model'] = f_device_model
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

        #  标题统一小写
        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        log_df_5g.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)

    def data_preprocessing_log_wt_4g_outdoor(self):
        """
        预处理室外WT_log原始数据文件
        :return: 返回清洗完成的wt_4G数据文件，文件后缀格式csv
        """
        import datetime
        # 获取当前日期
        current_date = datetime.date.today()
        # 格式化为年_月_日形式
        formatted_date = current_date.strftime("%Y_%m_%d")
        file_name = '4G_WT_LOG_DT_{}_UE1_P40_111'.format(formatted_date)
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：iPhone13_1
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：iPhone13_2
        f_msisdn = '533F8040D9351F4A9499FC7825805B14'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；P40_1
        # f_msisdn = '7314E1BE6DF72134E285D6AC1A99D8B7'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；P40_2
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；三星 S22_1
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；三星 S22_2
        # f_msisdn = '2FDB5089F348E243531E0CC4BE639DF8'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 13_1
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；小米 13_2
        # f_msisdn = '60F0ECE168DCB4B646A39E5D65BD5CE3'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；IQOO9
        # f_msisdn = 'AEF80522944F71607EAF49D320AC846F'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；IQOO7_1
        # f_msisdn = '154D4095974592369EC605DD5E8C0D24'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；IQOO7_2
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Mate40_1
        # f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Mate40_2
        # f_msisdn = '1794CAD04390860A9B1FD91F321AACE2'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Honor_90_1
        # f_msisdn = 'EB299AA88F04E739BD22726690042716'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行；Honor_90_2

        uemr_data_df = pd.read_csv(os.path.join(path_4g, ue1_4g), encoding='utf-8', header=0)
        # uemr_data_df = pd.read_excel(self.src_file_4g, header=0)
        # uemr_data_df = pd.read_csv(self.src_file_4g, encoding='utf-8', header=0)
        col_imei = ['PCTime', 'Lon', 'Lat', 'IMSI', 'IMEI']
        imei_data_df = pd.read_csv(os.path.join(path_4g, imei_4g_1), encoding='utf-8', header=0, usecols=col_imei)
        uemr_data_df = pd.merge(uemr_data_df, imei_data_df, left_on="PC Time", right_on="PCTime", how='left')
        # print(uemr_data_df.columns.values)
        # # 定义要进行替换的正则表达式模式
        # pattern = re.compile(r'NCell(\d)(\d)')
        # # # 使用正则表达式匹配并替换列名
        # uemr_data_df.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in uemr_data_df.columns]
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
        uemr_data_df['f_phr'] = ''
        uemr_data_df['f_enb_received_power'] = ''
        uemr_data_df['f_ta'] = ''
        uemr_data_df['f_aoa'] = ''
        uemr_data_df['f_enb_id'] = uemr_data_df['f_cell_id'] // 256
        uemr_data_df['f_cst_time'] = uemr_data_df['pc_time']
        uemr_data_df['f_province'] = f_province
        uemr_data_df['f_city'] = f_city
        uemr_data_df['f_area'] = f_area
        uemr_data_df['f_floor'] = f_floor
        uemr_data_df['f_altitude'] = f_altitude
        uemr_data_df['f_scenario'] = f_scenario
        # uemr_data_df['f_cell_id'] = uemr_data_df['f_cell_id']
        uemr_data_df['f_time'] = DataPreprocessing.convert_datetime_to_timestamp(uemr_data_df['pc_time'])
        calculate_directions(uemr_data_df)  # 计算行进方向。
        uemr_data_df['PCTime_'] = DataPreprocessing.convert_timestamp_to_datetime_t(uemr_data_df['f_time'])
        uemr_data_df['f_msisdn'] = f_msisdn
        uemr_data_df['f_district'] = f_district
        uemr_data_df['f_street'] = f_street
        uemr_data_df['f_building'] = f_building
        uemr_data_df['f_prru_id'] = 0
        uemr_data_df['f_source'] = f_source
        # SID暂时都赋值1
        uemr_data_df.loc[:, 'f_sid'] = 1
        uemr_data_df['f_pid'] = (uemr_data_df.index + 1).astype(str)
        uemr_data_df['f_device_brand'] = f_device_brand
        uemr_data_df['f_device_model'] = f_device_model
        uemr_data_df['f_time_1'] = pd.to_datetime(uemr_data_df['f_time'], unit='s').dt.tz_localize(
            pytz.utc).dt.tz_convert(pytz.timezone('Asia/Shanghai'))
        uemr_data_df['finger_id'] = 'F' + uemr_data_df['f_time_1'].dt.strftime('%Y%m%d%H') + '_' + uemr_data_df[
                                                                                                       'f_msisdn'].str[
                                                                                                   -4:]
        uemr_data_df[['f_year', 'f_month', 'f_day']] = uemr_data_df['f_time'].apply(convert_timestamp_to_date).to_list()
        num_list = []
        for i in range(len(uemr_data_df)):
            num_list.append(np.count_nonzero(
                uemr_data_df[['f_rsrp_n1', 'f_rsrp_n2', 'f_rsrp_n3', 'f_rsrp_n4', 'f_rsrp_n5', 'f_rsrp_n6', 'f_rsrp_n7',
                              'f_rsrp_n8']].isnull().values[i] == False))
        uemr_data_df['f_neighbor_cell_number'] = num_list
        duplicate_columns = uemr_data_df.columns[uemr_data_df.columns.duplicated()]
        uemr_data_df = uemr_data_df.loc[:, ~uemr_data_df.columns.duplicated()]
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
        # 填充缺失的cell_id
        DataPreprocessing.data_filling(uemr_data_df, 'f_cell_id')
        DataPreprocessing.data_filling(uemr_data_df, 'f_enb_id')
        # 删除重复行数据
        # DataPreprocessing.determine_duplicate_data(uemr_data_df, 'f_longitude', 'f_latitude')
        # logging.info('>>>>>>>>>> 开始删除重复行数据 ')
        uemr_data_df = uemr_data_df.drop_duplicates()
        # 删除x,y有空值的行
        uemr_data_df = uemr_data_df.dropna(subset={'f_longitude', 'f_latitude'}, how='any')

        # uemr_data_df['time_cst_u'] = uemr_data_df['pc_time']
        # 时间转时间戳类型

        # uemr_data_df = uemr_data_df.groupby('IMSI').get_group(f_msisdn)
        # 校验cell_id是否有交集
        # DataPreprocessing.check_cellId_intersection(uemr_data_df, 'uemr', 'IMSI')
        # uemr_data_df = DataPreprocessing.group_by_IMSI(uemr_data_df, 'uemr', 'IMSI')

        print('分组后的4GLog数据：\n' + str(uemr_data_df))
        # 标题统一小写
        uemr_data_df = uemr_data_df.rename(str.lower, axis='columns')
        uemr_data_df.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)

    def data_quality_detection(excel_file, f_or_u, g_number, file_name):
        """
        数据质量检测
        :param excel_file:需检测的文件路径
        :param f_or_u:指纹或UMER
        :param g_number:4G或5G
        """
        data = pd.read_csv(str(excel_file), header=0)
        if f_or_u == 'F':
            prefix = 'f'
        elif f_or_u == 'U':
            prefix = 'u'
        final_result = True
        if len(data) > 0:
            logging.info('>>>>>>>>>> 开始对' + file_name + '.csv文件进行数据检验')
            # 规则1：f_rsrp,f_rsrp,f_sinr作为主列,其副列参数值不能与主列相同
            final_result = DataPreprocessing.check_chief_series(data, prefix, g_number, final_result)
            # 规则2：rsrp、rsrq的参数值不能为空
            final_result = DataPreprocessing.check_rsrp_and_rsrq_isNull(data, prefix, final_result)
            # 规则3：rsrp，rsrq，sinr的参数值取值范围是否为正常范围内
            final_result = DataPreprocessing.check_rsrp_and_rsrq_range(data, prefix, g_number, final_result)
            # 规则4：数据行重复性检测
            final_result = DataPreprocessing.check_data_duplicate(data, final_result)
        if final_result == True:
            logging.info('数据一切正常，校验通过！！！')

    def check_chief_series(data, prefix, g_number, final_result):
        """
        规则1：f_rsrp,f_rsrp,f_sinr作为主列,其副列参数值不能与主列相同
        :param data：需检测的数据集
        :param prefix：前缀f或u
        :param g_number：4G或5G
        :param final_result：最终结果
        :return：校验结果True通过，False不通过
        """
        logging.info('>>>>>>>>>> 开始校验规则1：是否有与主列相同的列')
        for x in range(len(data)):
            for y in range(1, 7):
                if (data[prefix + '_rsrp'][x] == data[prefix + '_rsrp_' + str(y)][x]) and (
                        data[prefix + '_rsrq'][x] == data[prefix + '_rsrq_' + str(y)][x]):
                    logging.info(prefix + '_rsrp_' + str(y) + '列和' + prefix + '_rsrq_' + str(y) + '列，在第' + str(
                        x + 2) + '行，两个值都分别与' + prefix + '_rsrp列和' + prefix + '_rsrq列相同，校验不通过')
                    final_result = False
        if g_number == '5G':
            for x in range(len(data)):
                for y in range(1, 7):
                    if data[prefix + '_sinr'][x] == data[prefix + '_sinr_' + str(y)][x]:
                        logging.info(prefix + '_sinr_' + str(y) + '列，第' + str(
                            x + 2) + '行，与' + prefix + '_sinr列相同，校验不通过')
                        final_result = False
        return final_result

    def check_rsrp_and_rsrq_isNull(data, prefix, final_result):
        """
        规则2：rsrp、rsrq的参数值不能为空
        :param data：需检测的数据集
        :param prefix：前缀f或u
        :param final_result：最终结果
        :return：校验结果True通过，False不通过
        """
        logging.info('>>>>>>>>>> 开始校验规则2：rsrp、rsrq的参数值不能为空 ')
        for i in range(len(data)):
            if str(data[prefix + '_rsrp'][i]) == 'nan':
                logging.info(prefix + '_rsrp列,在第' + str(i + 2) + '行有空值，校验不通过')
                final_result = False
            if str(data[prefix + '_rsrq'][i]) == 'nan':
                logging.info(prefix + '_rsrq列,在第' + str(i + 2) + '行有空值，校验不通过')
                final_result = False
        return final_result

    def check_rsrp_and_rsrq_range(data, prefix, g_number, final_result):
        """
        规则3：检查rsrp和rsrq的数值是否在正常范围之内
        :param data：需检测的数据集
        :param prefix：前缀f或u
        :param g_number：4G或5G
        :param final_result：最终结果
        :return：校验结果True通过，False不通过
        """
        logging.info('>>>>>>>>>> 开始校验规则3：rsrp和rsrq的数值是否在正常范围之内')
        if g_number == '5G':
            for i in range(len(data[prefix + '_rsrp'])):
                rsrp = data[prefix + '_rsrp'].between(-141, -40)
                if rsrp[i] == False:
                    logging.info(prefix + '_rsrp的数值超出正常范围(-141, -40),错误值在第：' + str(
                        i + 2) + '行' + '填空中实际数值：' + str(
                        data.loc[i, prefix + '_rsrp']))
                    final_result = False
            for i in range(len(data[prefix + '_rsrq'])):
                f_rsrq = data[prefix + '_rsrq'].between(-30, 0)
                if f_rsrq[i] == False:
                    logging.info(prefix + '_rsrq的数值超出正常范围(-30, 0),错误值在第：' + str(
                        i + 2) + '行' + '填空中实际数值：' + str(
                        data.loc[i, prefix + '_rsrq']))
                    final_result = False
            for i in range(len(data[prefix + '_sinr'])):
                f_sinr = data[prefix + '_sinr'].between(-20, 50)
                if f_sinr[i] == False:
                    logging.info(prefix + '_sinr的数值超出正常范围(-20, 50),错误值在第：' + str(
                        i + 2) + '行' + '填空中实际数值：' + str(
                        data.loc[i, prefix + '_sinr']))
                    final_result = False
        elif g_number == '4G':
            for i in range(len(data[prefix + '_rsrp'])):
                rsrp = data[prefix + '_rsrp'].between(-141, -40)
                if rsrp[i] == False:
                    logging.info(prefix + '_rsrp的数值超出正常范围(-141, -40),错误值在第：' + str(
                        i + 2) + '行' + '填空中实际数值：' + str(
                        data.loc[i, prefix + '_rsrp']))
                    final_result = False
            for i in range(len(data[prefix + '_rsrq'])):
                f_rsrq = data[prefix + '_rsrq'].between(-40, 0)
                if f_rsrq[i] == False:
                    logging.info(prefix + '_rsrq的数值超出正常范围(-40, 0),错误值在第：' + str(
                        i + 2) + '行' + '填空中实际数值：' + str(
                        data.loc[i, prefix + '_rsrq']))
                    final_result = False
        return final_result

    def check_data_duplicate(data, final_result):
        """
        规则4：检查某一行数据是否存在一样的行数据
        :param data：需检测的数据集
        :param final_result：最终结果
        :return：校验结果True通过，False不通过
        """
        logging.info('>>>>>>>>>> 开始校验规则4：检查某一行数据是否存在一样的行数据 ')
        result = data.duplicated()
        for i in range(len(result)):
            if result.loc[i,] == True:
                logging.info('有重复数据在第' + str(i + 2) + '行，校验不通过')
                final_result = False
        return final_result

    @staticmethod
    def data_filling(finger_or_umer_df, column_name):
        """
        缺失的cell_id数据填充：第一个不为空的值，记录下来，把下个空值填上即可，直到下一个空值不为空
        :param finger_or_umer_df：指纹或uemr数据集
        :param column_name：需要填充的列名
        :return：填充后的数据集
        """
        temp = np.nan
        index = []
        for i in range(len(finger_or_umer_df)):
            value = finger_or_umer_df.loc[i, column_name]  # 获取列每行的值
            if str(value) != 'nan':  # 不空，就保存
                # 临时存储用于填充
                temp = value
            elif str(temp) == 'nan':
                # 将下标存储用于向上填充
                index.append(i)
            else:
                # 将临时变量的值进行填充
                finger_or_umer_df.loc[i, column_name] = temp
            if str(value) != 'nan':
                for k in index:
                    # 避免第一个为空的情况，向上填充
                    finger_or_umer_df.loc[k, column_name] = value
                index.clear()
        return finger_or_umer_df.reset_index(drop=True)

    # def data_filling(finger_or_umer_df, column_name):
    #     """
    #     缺失的cell_id数据填充：第一个不为空的值，记录下来，把下个空值填上即可，直到下一个空值不为空
    #     :param finger_or_umer_df：指纹或uemr数据集
    #     :param column_name：需要填充的列名
    #     :return：填充后的数据集
    #     """
    #     temp = np.nan
    #     index = []
    #     for i in range(len(finger_or_umer_df)):
    #         value = finger_or_umer_df.loc[i, column_name]  # 获取列每行的值
    #         if str(value) != 'nan':
    #             # 临时存储用于填充
    #             temp = finger_or_umer_df.loc[i, column_name]
    #         elif str(temp) == 'nan':
    #             # 将下标存储用于向上填充
    #             index.append(i)
    #         else:
    #             # 将临时变量的值进行填充
    #             finger_or_umer_df.loc[i, column_name] = temp
    #         if str(value) != 'nan':
    #             for k in index:
    #                 # 避免第一个为空的情况，向上填充
    #                 finger_or_umer_df.loc[k, column_name] = value
    #             index.clear()
    #     return finger_or_umer_df.reset_index(drop=True)

    def neighbor_cell_number_filling(finger_or_umer_df, column_name):
        """
        填充neighbor_cell_number
        :param finger_or_umer_df：指纹或uemr数据集
        :param column_name：需要填充的列名
        :return：填充后的数据集
        """
        for i in range(len(finger_or_umer_df)):
            value = finger_or_umer_df.loc[i, column_name]
            if str(value) == 'nan':
                finger_or_umer_df.loc[i, column_name] = 0
            elif str(value) == '255':
                finger_or_umer_df.loc[i, column_name] = 0
        return finger_or_umer_df

    def group_by_IMSI(data, decision_value, group_field):
        """
        根据某列进行数据分组
        :param data：数据集
        :param decision_value：finger或uemr
        :param group_field：分组的列名
        :return：分组后的数据集
        """
        datas = data.groupby(group_field)
        names = []
        for name, group in datas:
            # print(name)
            # print(len(group['IMSI']))
            names.append(name)

        print(names)
        if len(names) == 0:
            # 如果只有一个号码就不区分指纹和uemr了都用同一份数据
            return data

        if len(names) == 1:
            # 如果只有一个号码就不区分指纹和uemr了都用同一份数据
            data = data.groupby(group_field).get_group(names[0])
            return data

        a = len(data.groupby(group_field).get_group(names[0]))
        b = len(data.groupby(group_field).get_group(names[1]))

        # 根据数据量决定返回的数据
        if a > b and decision_value == 'finger':
            print(data.groupby(group_field).get_group(names[0]))
            data = data.groupby(group_field).get_group(names[0])
            return data
        elif a < b and decision_value == 'finger':
            print(data.groupby(group_field).get_group(names[1]))
            data = data.groupby(group_field).get_group(names[1])
            return data
        elif a > b and decision_value == 'uemr':
            print(data.groupby(group_field).get_group(names[1]))
            data = data.groupby(group_field).get_group(names[1])
            return data
        elif a < b and decision_value == 'uemr':
            print(data.groupby(group_field).get_group(names[0]))
            data = data.groupby(group_field).get_group(names[0])
            return data
        elif a == b and decision_value == 'finger':
            print(data.groupby(group_field).get_group(names[0]))
            data = data.groupby(group_field).get_group(names[0])
            return data
        elif a == b and decision_value == 'uemr':
            print(data.groupby(group_field).get_group(names[1]))
            data = data.groupby(group_field).get_group(names[1])
            return data

    def check_cellId_intersection(data, f_or_u, group_field):
        """
        根据某列进行数据分组
        :param data：数据集
        :param f_or_u：finger或uemr
        :param group_field：分组的列名
        """
        logging.info('>>>>>>>>>> 开始校验cell_id列是否存在交集数据 ')
        datas = data.groupby(group_field)
        names = []
        for name, group in datas:
            names.append(name)
        # 判断两组数据集的cell_id是否有交集
        data1 = data.groupby(group_field).get_group(names[0])
        data2 = data.groupby(group_field).get_group(names[1])
        if f_or_u == 'finger':
            prefix = 'f'
        elif f_or_u == 'uemr':
            prefix = 'u'
        intersection_number = len(set(data1[prefix + '_cell_id']).intersection(data2[prefix + '_cell_id']))
        print('相交的数量：' + str(len(set(data1[prefix + '_cell_id']).intersection(data2[prefix + '_cell_id']))))
        if intersection_number <= 0:
            logging.info('cell_id列不存在交集数据，数据有误！！！')
        else:
            logging.info('cell_id列数据正常存在交集数据')

    def determine_duplicate_data(data, f_or_u, g_number):
        """
        判断有重复的行数据
        :param data：数据集
        """
        # if f_or_u == 'finger':
        #     prefix = 'f_'
        #     suffix = 'f'
        # elif f_or_u == 'uemr':
        #     prefix = 'u_'
        #     suffix = 'u'
        # if g_number == '5G':
        #     result = data.duplicated(
        #         [prefix + 'freq', prefix + 'pci', prefix + 'rsrp', prefix + 'rsrq', prefix + 'sinr', prefix + 'pci_n1',
        #          prefix + 'pci_n2', prefix + 'pci_n3', prefix + 'pci_n4', prefix + 'pci_n5',
        #          prefix + 'pci_n6', prefix + 'pci_n7', prefix + 'pci_n8', prefix + 'freq_n1', prefix + 'freq_n2',
        #          prefix + 'freq_n3', prefix + 'freq_n4', prefix + 'freq_n5', prefix + 'freq_n6', prefix + 'freq_n7'
        #             , prefix + 'freq_n8', prefix + 'rsrp_1', prefix + 'rsrp_2', prefix + 'rsrp_3', prefix + 'rsrp_4',
        #          prefix + 'rsrp_5', prefix + 'rsrp_6', prefix + 'rsrp_7', prefix + 'rsrp_8', prefix + 'rsrq_1'
        #             , prefix + 'rsrq_2', prefix + 'rsrq_3', prefix + 'rsrq_4', prefix + 'rsrq_5', prefix + 'rsrq_6',
        #          prefix + 'rsrq_7', prefix + 'rsrq_8', prefix + 'sinr_1', prefix + 'sinr_2', prefix + 'sinr_3'
        #             , prefix + 'sinr_4', prefix + 'sinr_5', prefix + 'sinr_6', prefix + 'sinr_7', prefix + 'sinr_8',
        #          prefix + 'cell_id', 'time_' + suffix, 'IMEI', 'IMSI', 'SID', 'gNB_ID', 'time_cst_' + suffix])
        # elif g_number == '4G':
        #     result = data.duplicated(
        #         [prefix + 'freq', prefix + 'pci', prefix + 'rsrp', prefix + 'rsrq', prefix + 'pci_n1',
        #          prefix + 'pci_n2', prefix + 'pci_n3', prefix + 'pci_n4', prefix + 'pci_n5',
        #          prefix + 'pci_n6', prefix + 'pci_n7', prefix + 'pci_n8', prefix + 'freq_n1', prefix + 'freq_n2',
        #          prefix + 'freq_n3', prefix + 'freq_n4', prefix + 'freq_n5', prefix + 'freq_n6', prefix + 'freq_n7'
        #             , prefix + 'freq_n8', prefix + 'rsrp_1', prefix + 'rsrp_2', prefix + 'rsrp_3', prefix + 'rsrp_4',
        #          prefix + 'rsrp_5', prefix + 'rsrp_6', prefix + 'rsrp_7', prefix + 'rsrp_8', prefix + 'rsrq_1'
        #             , prefix + 'rsrq_2', prefix + 'rsrq_3', prefix + 'rsrq_4', prefix + 'rsrq_5', prefix + 'rsrq_6',
        #          prefix + 'rsrq_7', prefix + 'rsrq_8', prefix + 'cell_id', 'time_' + suffix, 'IMEI', 'IMSI', 'SID',
        #          'time_cst_' + suffix])
        # else:
        #     logging.info('参数有误，请检查g_number是否为4G或5G')
        result = data.duplicated()
        for i in result.index:
            if result.loc[i,] == True:
                # logging.info('>>>>>>>>>> 有重复数据在第' + str(i + 2) + '行')
                logging.info('>>>>>>>>>> 有重复数据在第' + str(i) + '行')

    def convert_timestamp_to_datetime(timestamp):
        """
        转换时间戳
        :param timestamp:数字时间戳list
        :return: 指定时间戳格式的strftime字符串
        """
        return [datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S.%f') for x in timestamp]

    def convert_timestamp_to_date(timestamp):
        date_obj = datetime.fromtimestamp(timestamp / 1000)
        year = date_obj.strftime('%Y')
        month = date_obj.strftime('%m')
        day = date_obj.strftime('%d')
        return year, month, day

    def convert_timestamp_to_datetime_timestr(timestamp):
        """
        转换时间戳
        :param timestamp:数字时间戳list
        :return: 指定时间戳格式的strftime字符串
        """
        return [datetime.fromtimestamp(x / 1000).strftime('%Y%m%d%H%M%S%f') for x in timestamp]

    def convert_timestamp_to_datetime_t(timestamp):
        """
        转换时间戳
        :param timestamp:数字时间戳list
        :return: 指定时间戳格式的strftime字符串
        """
        return [datetime.fromtimestamp(x).strftime('%Y%m%d%H%M%S') for x in timestamp]

    def convert_datetime_to_timestamp(dtime):
        """
        转换日期
        :param dtime:日期类型数据
        :return: 指定时间戳格式的strftime字符串
        """
        return [time.mktime(time.strptime(x, '%Y-%m-%d %H:%M:%S.%f')) for x in dtime]

    def convert_datetime_to_timestamp_v1(dtime):
        """
        转换日期
        :param dtime:日期类型数据
        :return: 指定时间戳格式的strftime字符串
        """
        return [time.mktime(time.strptime(x, '%Y/%m/%d %H:%M:%S.%f')) for x in dtime]

    def data_log_merge_5g(self):
        file_names = [ue1_5g, ue2_5g, imei_5g_1, imei_5g_2]  # ue1&ue2
        file_names = [ue1_5g, imei_5g_1]  # ue1
        file_generator = (os.path.join(path_5g, file) for file in file_names)
        data = [pd.read_csv(next(file_generator)) for _ in file_names]

        df_ue1_imei1 = pd.merge(data[0], data[1], left_on="PC Time", right_on="PCTime", how='left')
        df_ue2_imei2 = pd.merge(data[1], data[3], left_on="PC Time", right_on="PCTime", how='left')

        # df_ue1_ue2 = pd.concat([df_ue1_imei1, df_ue2_imei2])
        df_ue1_ue2 = df_ue1_imei1

        return df_ue1_ue2

    def data_log_merge_4g(self):
        file_names = [ue1_4g, ue2_4g, imei_4g_1, imei_4g_2]
        file_names = [ue1_4g, imei_4g_1]
        file_generator = (os.path.join(path_4g, file) for file in file_names)
        data = [pd.read_csv(next(file_generator)) for _ in file_names]

        # df_ue1_imei1 = pd.merge(data[0], data[2], left_on="PC Time", right_on="PCTime", how='left')
        # df_ue2_imei2 = pd.merge(data[1], data[3], left_on="PC Time", right_on="PCTime", how='left')

        df_ue1_imei1 = pd.merge(data[0], data[1], left_on="PC Time", right_on="PCTime", how='left')
        # df_ue2_imei2 = pd.merge(data[1], data[3], left_on="PC Time", right_on="PCTime", how='left')
        # df_ue1_ue2 = pd.concat([df_ue1_imei1, df_ue2_imei2])
        df_ue1_ue2 = df_ue1_imei1

        return df_ue1_ue2

    def xy_log_read(self):
        file_names = [zcy_4g, zcy_5g]
        data_5g = pd.read_csv(os.path.join(path_zcy, zcy_5g), low_memory=False)
        data_4g = pd.read_csv(os.path.join(path_zcy, zcy_4g), low_memory=False)
        return data_5g, data_4g

    def merge_xy_log(self):
        data_5g, data_4g = ue.xy_log_read()
        data_lte = pd.read_csv(os.path.join(path_4g, ue1_4g))
        data_nr = pd.read_csv(os.path.join(path_5g, ue1_5g))

        data_lte['ts'] = data_lte['f_time'].map(lambda x: int(str(x)[0:10]))
        # data_nr['ts'] = data_lte['created_by_ue_time'].map(lambda x: int(str(x)[0:10]))##5G软件未输出时间戳，使用替代的方式解决。

        data_nr['ts'] = DataPreprocessing.convert_datetime_to_timestamp_v1(data_nr['pc_time'])
        data_5g['ts'] = data_5g['f_time'].map(lambda x: int(str(x)[0:10]))
        data_4g['ts'] = data_4g['f_time'].map(lambda x: int(str(x)[0:10]))

        data_4g['ts'] = DataPreprocessing.convert_datetime_to_timestamp(data_4g['f_time'])
        data_5g['ts'] = DataPreprocessing.convert_datetime_to_timestamp(data_5g['f_time'])
        merged_df_4g_xy = pd.merge(data_lte, data_4g[['ts', 'x_new', 'y_new', 'lon', 'lat']], on='ts')
        merged_df_5g_xy = pd.merge(data_nr, data_5g[['ts', 'x_new', 'y_new', 'lon', 'lat']], on='ts')
        # merged_df_5g_xy.to_csv(os.path.join(output_folder, 'merged_df_5g_xy.csv'), index=False)
        return merged_df_4g_xy, merged_df_5g_xy

    def data_process_wifi_bluetooth_data_4g(self):
        """
                预处理WeTest 测试 log原始数据文件
                :return: 返回清洗完成的测试数据文件，文件后缀格式csv
                """
        import datetime
        # 获取当前日期
        f_device_brand = "华为"
        f_device_model = "P40"
        f_area = 'CD值机岛'
        f_floor = '4F'
        f_scenario = 1
        f_province = "北京"
        f_city = "北京"
        f_district = '大兴区'
        f_street = '大兴国际机场'
        f_building = '航站楼'
        f_prru_id = 0
        f_source = 'BlueTooth & Wifi'  # 测试log；2：MR软采；3：扫频仪；4：WIFI；5：OTT；6：蓝牙
        f_altitude = 121
        current_date = datetime.date.today()
        # 格式化为年_月_日形式
        formatted_date = current_date.strftime("%Y_%m_%d")
        file_name = '4G_WalkingIndoor_LOG_CQT_{}_UE1_P40_1'.format(formatted_date)
        f_msisdn = '533F8040D9351F4A9499FC7825805B14'  # 前台 log 的 MSISDN 填充 UEMR 不需要执行：oppo_Reno8_2
        output_folder = r"D:\开发\mrposition\MRData\FingerprintData\BlueTooth_Wifi_Finger"

        log_df_4g = pd.read_csv(os.path.join(r"C:\Users\Administrator\Downloads\dyj_20231127\研发\4\20231127",
                                             "dyj_研发_4_4G_20231127采样点数据_WiFi_BlueTooth.csv"))

        # print(log_df_4g.columns.values)
        # log_df_4g['PCTime_'] = DataPreprocessing.convert_datetime_to_timestamp_v1(log_df_4g['pc_time'])
        log_df_4g = log_df_4g.groupby(log_df_4g['f_time']).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。

        # log_df_4g = log_df_4g.loc[:, ['imei',
        #                               'imsi',
        #                               'lte_eci',
        #                               'ts',
        #                               'lon',
        #                               'lat',
        #                               'lte_serving_cell_pci',
        #                               'lte_serving_cell_freq',
        #                               'lte_serving_cell_rsrp',
        #                               'lte_serving_cell_rsrq',
        #                               'lte_neighbor_cell_1_pci',
        #                               'lte_neighbor_cell_1_freq',
        #                               'lte_neighbor_cell_1_rsrp',
        #                               'lte_neighbor_cell_1_rsrq',
        #                               'lte_neighbor_cell_2_pci',
        #                               'lte_neighbor_cell_2_freq',
        #                               'lte_neighbor_cell_2_rsrp',
        #                               'lte_neighbor_cell_2_rsrq',
        #                               'lte_neighbor_cell_3_pci',
        #                               'lte_neighbor_cell_3_freq',
        #                               'lte_neighbor_cell_3_rsrp',
        #                               'lte_neighbor_cell_3_rsrq',
        #                               'lte_neighbor_cell_4_pci',
        #                               'lte_neighbor_cell_4_freq',
        #                               'lte_neighbor_cell_4_rsrp',
        #                               'lte_neighbor_cell_4_rsrq',
        #                               'lte_neighbor_cell_5_pci',
        #                               'lte_neighbor_cell_5_freq',
        #                               'lte_neighbor_cell_5_rsrp',
        #                               'lte_neighbor_cell_5_rsrq',
        #                               'lte_neighbor_cell_6_pci',
        #                               'lte_neighbor_cell_6_freq',
        #                               'lte_neighbor_cell_6_rsrp',
        #                               'lte_neighbor_cell_6_rsrq',
        #                               'lte_neighbor_cell_7_pci',
        #                               'lte_neighbor_cell_7_freq',
        #                               'lte_neighbor_cell_7_rsrp',
        #                               'lte_neighbor_cell_7_rsrq',
        #                               'lte_neighbor_cell_8_pci',
        #                               'lte_neighbor_cell_8_freq',
        #                               'lte_neighbor_cell_8_rsrp',
        #                               'lte_neighbor_cell_8_rsrq',
        #                               'pc_time',
        #                               'x_new',
        #                               'y_new']]

        log_df_4g = log_df_4g.rename(
            columns={
                'x_new': 'f_x',
                'y_new': 'f_y',
            })

        duplicate_columns = log_df_4g.columns[log_df_4g.columns.duplicated()]
        log_df_4g = log_df_4g.loc[:, ~log_df_4g.columns.duplicated()]
        log_df_4g['f_msisdn'] = f_msisdn

        log_df_4g['f_datetime'] = DataPreprocessing.convert_timestamp_to_datetime(log_df_4g['f_time'])
        # log_df_4g['finger_id'] = 'F' + log_df_4g['f_datetime'].dt.strftime('%Y%m%d%H') + '_' + log_df_4g['f_msisdn'].str[
        #                                                                                      -4:]
        log_df_4g['f_province'] = f_province
        log_df_4g['f_city'] = f_city
        log_df_4g['f_area'] = f_area
        log_df_4g['f_floor'] = f_floor
        log_df_4g['f_altitude'] = f_altitude
        log_df_4g['f_scenario'] = f_scenario
        log_df_4g['f_district'] = f_district
        log_df_4g['f_street'] = f_street
        log_df_4g['f_building'] = f_building
        log_df_4g['f_source'] = f_source

        log_df_4g['f_device_brand'] = f_device_brand
        log_df_4g['f_device_model'] = f_device_model
        log_df_4g = log_df_4g.reindex()
        # DataPreprocessing.data_filling(log_df_4g, 'f_cell_id')

        # # 标题统一小写
        log_df_4g = log_df_4g.rename(str.lower, axis='columns')
        log_df_4g.to_csv(os.path.join(output_folder, file_name + '.csv'), index=False)


def generate_unique_id(data):
    ##哈希函数
    # 将数据转换为字节串
    data_bytes = str(data).encode('utf-8')

    # 使用SHA-256哈希算法生成唯一ID
    hash_object = hashlib.sha256()
    hash_object.update(data_bytes)
    unique_id = hash_object.hexdigest()

    return unique_id


def convert_timestamp_to_date(timestamp):
    date_obj = pd.to_datetime(timestamp, unit='s')
    return date_obj.year, date_obj.month, date_obj.day


def convert_timestamp_to_datestr(timestamp):
    date_obj = pd.to_datetime(timestamp, unit='s')
    year = str(date_obj.year)
    month = str(date_obj.month).zfill(2)
    day = str(date_obj.day).zfill(2)
    date_str = year + month + day
    return date_str


def calculate_directions(df):
    """
    计算当前位置的方向
    :return: 返回当前数据相对于上一条数据的方向。
    """

    def get_direction(curr_x, curr_y, prev_x, prev_y):
        delta_x = curr_x - prev_x
        delta_y = curr_y - prev_y

        angle = math.atan2(delta_y, delta_x)
        angle_deg = math.degrees(angle)

        if -45 <= angle_deg <= 45:
            return 'N'
        elif 45 < angle_deg <= 135:
            return 'E'
        elif -135 <= angle_deg < -45:
            return 'S'
        else:
            return 'W'

    prev_x = None
    prev_y = None

    directions = []
    for index, row in df.iterrows():
        curr_x = float(row['f_longitude'])
        curr_y = float(row['f_latitude'])

        if prev_x is not None and prev_y is not None:
            direction = get_direction(curr_x, curr_y, prev_x, prev_y)
            directions.append(direction)
        else:
            directions.append('未发生位移')

        prev_x = curr_x
        prev_y = curr_y

    df['f_direction'] = directions
    return df


if __name__ == '__main__':
    src_file_5g = r"C:\Users\Administrator\Downloads\dyj_20231127\研发\4\20231127"
    src_file_4g = r"C:\Users\Administrator\Downloads\dyj_20231127\研发\4\20231127"
    # #
    # #测试 log 数据路径：
    # path_4g = "/Users/rainrain/Desktop/LAC/大兴机场/1123/华为室外/4G"
    # path_5g = "/Users/rainrain/Desktop/LAC/大兴机场/1123/华为室外/5G"
    # #
    # path_op = "/Users/rainrain/Desktop/LAC/大兴机场/1123/华为室外/"
    # #
    # # # 室内走测仪数据路径：
    # path_zcy = "/Users/rainrain/Desktop/LAC/大兴机场/1121/zcy"
    # #
    # # 创建输出文件路径
    # output_folder = os.path.join(path_op, 'output')
    # if not os.path.exists(output_folder):
    #     os.mkdir(output_folder)
    # else:
    #     print(f'文件夹{output_folder}已存在，不需要创建')
    #
    # 输入需要处理的文件名称
    ue1_4g = 'LTE_20231123_161101_5137.csv'
    ue1_5g = 'NR_20231123_160346_3912.csv'
    # 室内走测仪数据
    zcy_4g = "国际财经中心4g_c座_2_4G_20231121采样点数据_2023-11-22_xyToLonLat_ZCY.csv"
    zcy_5g = "国际财经中心5g_c座_2_4G_20231121采样点数据_2023-11-22_xyToLonLat_ZCY.csv"
    #
    # # 填充测试数据的场景等信息_室内：
    # f_device_brand = "OPPO"
    # f_device_model = "Reno8"
    # f_area = 'AB座走廊'
    # f_floor = '2F'
    # f_scenario = 1 ###室内为 1：室外为 2
    # f_province = "北京"
    # f_city = "北京"
    # f_district = '海淀区'
    # f_street = '西三环北路87号'
    # f_building = '国际财经中心A座'
    # f_prru_id = 0
    # f_source = '测试log'# 测试log；2：MR软采；3：扫频仪；4：WIFI；5：OTT；6：蓝牙
    # f_altitude = 125

    # 填充测试数据的场景等信息_中兴室外：
    # f_device_brand = 'OPPO'
    # f_device_model = "Reno8"
    # f_area = '国际财经中心室外道路'
    # f_floor = '1F'
    # f_scenario = 2
    # f_province = "北京"
    # f_city = "北京"
    # f_district = '海淀区'
    # f_street = '西三环北路玲珑路南蓝靛厂南路北洼西街'
    # f_building = '国际财经中心'
    # f_prru_id = 0
    # f_source = 'WeTest_Log'  # 测试log；2：MR软采；3：扫频仪；4：WIFI；5：OTT；6：蓝牙;7.WeTest_Log
    # f_altitude = 100

    # 填充测试数据的场景等信息_华为室外：
    # f_device_brand = 'VIVO'
    # f_device_model = "Y3"
    # f_area = '国家会议中心室外道路'
    # f_floor = '1F'
    # f_scenario = 2
    # f_province = "北京"
    # f_city = "北京"
    # f_district = '朝阳区'
    # f_street = '大屯路北辰东路国家体育场北路北辰西路'
    # f_building = '国家会议中心'
    # f_prru_id = 0
    # f_source = '测试Log'  # 测试log；2：MR软采；3：扫频仪；4：WIFI；5：OTT；6：蓝牙
    # f_altitude = 100

    # 填充测试数据的场景等信息_华为室内：
    # f_device_brand = "iPhone"
    # f_device_model = "13"
    # f_area = 'CD值机岛'
    # f_floor = '4F'
    # f_scenario = 1
    # f_province = "北京"
    # f_city = "北京"
    # f_district = '大兴区'
    # f_street = '大兴国际机场'
    # f_building = '航站楼'
    # f_prru_id = 0
    # f_source = '测试log'  # 测试log；2：MR软采；3：扫频仪；4：WIFI；5：OTT；6：蓝牙
    # f_altitude = 121

    #
    # # 标准化指纹处理程序__weTest_log_Indoor
    # ue.data_preprocessing_wtlog_4g()###4G室外
    # ue.data_preprocessing_wtlog_5g()###5G室外
    #
    # # 标准化指纹处理程序__weTest_log_Outdoor
    # ue.data_preprocessing_wtlog_4g_outdoor()###4G室外
    # ue.data_preprocessing_wtlog_5g_outdoor()###5G室外
    #
    # # 标准化指纹处理程序__walkTour_log_室外
    # ue.data_preprocessing_log_wt_5g_outdoor()  ###5G室外
    # ue.data_preprocessing_log_wt_4g_outdoor()  ###5G室外
    #
    # # 标准化指纹处理程序__walkTour_log_室内
    # ue.data_preprocessing_walktour_log_4g()  ###4G室内
    # ue.data_preprocessing_walktour_log_5g()  ###5G室内
    #
    # # 标准化指纹处理程序__uemr_室内
    # ue.data_preprocessing_uemr_4g()  ###4G室内
    # ue.data_preprocessing_uemr_5g()  ###5G室内

    ue = DataPreprocessing(src_file_5g, src_file_4g)
    ue.data_process_wifi_bluetooth_data_4g()
