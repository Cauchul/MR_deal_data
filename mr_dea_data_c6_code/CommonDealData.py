# -*- coding: utf-8 -*-
import os

import pandas as pd

from Common import get_file_by_str, get_file_by_string, read_csv_get_df
from DataPreprocessing import DataPreprocessing


class DealData:
    @staticmethod
    def get_data_file_path(in_path):
        ue_file = get_file_by_str('UE', in_path)
        table_file = get_file_by_string('table', in_path)
        zcy_file = get_file_by_string('ZCY', in_path)
        wifi_bluetooth_file = get_file_by_string('xyToLonLat_WIFI_BlueTooth', in_path)
        return ue_file, table_file, zcy_file, wifi_bluetooth_file

    @staticmethod
    def deal_ue_table_df(in_ue_file, in_table_file):
        in_ue_df = read_csv_get_df(in_ue_file)
        if in_table_file and os.path.exists(in_table_file):
            in_table_df = read_csv_get_df(in_table_file)
            res_tmp_merge_df = pd.merge(in_ue_df, in_table_df, left_on="PC Time", right_on="PCTime", how='left')
            return res_tmp_merge_df
        else:
            return in_ue_df

    @staticmethod
    def get_zcy_data(in_zcy_file):
        in_zcy_df = read_csv_get_df(in_zcy_file)
        tmp_zcy_df = in_zcy_df[
            ['test_time', 'created_by_ue_time', 'f_x', 'f_y', 'f_longitude', 'f_latitude', 'direction', 'altitude']]
        return tmp_zcy_df

    @staticmethod
    def get_wifi_bluetooth_data(in_wifi_bluetooth_file):
        in_wifi_df = read_csv_get_df(in_wifi_bluetooth_file)
        tmp_wifi_df = in_wifi_df.drop(['f_x', 'f_y', 'f_longitude', 'f_latitude', 'f_direction', 'f_altitude'], axis=1)
        return tmp_wifi_df

    @staticmethod
    def merge_zcy_wifi_data(in_zcy_file, in_wifi_bluetooth_file):
        # 合并走测仪和wifi数据
        zcy_df = DealData.get_zcy_data(in_zcy_file)
        wifi_bluetooth_df = DealData.get_wifi_bluetooth_data(in_wifi_bluetooth_file)
        # 获取zcy，wifi数据
        tmp_merger_df = pd.merge(wifi_bluetooth_df, zcy_df, left_on="f_time", right_on="created_by_ue_time",
                                 how='left')
        return tmp_merger_df

    @staticmethod
    def merge_ue_zcy_df_data(in_ue_df, in_zcy_df):
        in_ue_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_ue_df['PC Time'])
        if in_zcy_df:
            in_zcy_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_zcy_df['test_time'])
            tmp_df = pd.merge(in_ue_df, in_zcy_df)
            return tmp_df
        return in_ue_df
