# -*- coding: utf-8 -*-

import configparser
import math
import os

import numpy as np
import pandas as pd

from Common import clear_path, unzip, get_file_by_string, copy_file, read_csv_get_df, data_conversion, df_write_to_csv, \
    generate_images, deal_df_object, check_path, split_path_get_list
from DataPreprocessing import DataPreprocessing, convert_timestamp_to_date
from GlobalConfig import WalkTour_table_format_dict, tmp_res_out_path, f_msisdn_dict


class WalkTourIndoor:
    @staticmethod
    def unzip_zcy_zip_file(in_path):
        # 解压目录
        in_extraction_path = os.path.join(in_path, 'unzip')
        clear_path(in_extraction_path)
        # 获取压缩文件
        in_zip_file = get_file_by_string('zip', in_path)
        print('zip_file: ', in_zip_file)
        print('unzip_path: ', in_extraction_path)
        # 解压
        unzip(in_zip_file, in_extraction_path)
        return in_extraction_path

    @staticmethod
    def get_zcy_data_file_list(in_unzip_path):
        tmp_list = []
        for root, dirs, files in os.walk(in_unzip_path):
            for file in files:
                if '-chart' in file or '_pci_' in file or '_WiFi_BlueTooth' in file:
                    file_path = os.path.join(root, file)
                    print('file_path: ', file_path)
                    tmp_list.append(file_path)
        return tmp_list

    @staticmethod
    def copy_zcy_file_to_path(in_file_list, in_out_path):
        for in_i_f in in_file_list:
            print('in_i_f: ', in_i_f)
            copy_file(in_i_f, in_out_path)

    @staticmethod
    # 读取配置文件
    def read_config_file(in_config_path):
        # in_con_file = os.path.join(in_config_path, 'config.ini')
        in_con_file = os.path.join(r'D:\working\merge', 'config.ini')
        config = configparser.ConfigParser()
        # config.read(in_config_file, encoding='UTF-8')
        config.read(in_con_file, encoding='GBK')
        in_lon_O = config.get('Coordinates', 'lon_O')
        in_lat_O = config.get('Coordinates', 'lat_O')
        in_len_east_x = config.get('Coordinates', 'len_east_x')
        in_len_north_y = config.get('Coordinates', 'len_north_y')
        return float(in_lon_O), float(in_lat_O), float(in_len_east_x), float(in_len_north_y)

    @staticmethod
    def char_file_data_xyToLonLat(in_path):
        # 获取配置文件信息
        lon_O, lat_O, len_east_x, len_north_y = WalkTourIndoor.read_config_file(in_path)
        # 处理char数据
        char_file = get_file_by_string('-chart', in_path)
        print('char_file: ', char_file)
        char_df = read_csv_get_df(char_file)

        res_x1_values = data_conversion(len_east_x, char_df['x'])
        lon = res_x1_values / (111000 * math.cos(lon_O / 180 * math.pi)) + lon_O
        lon = 2 * max(lon) - lon

        res_y1_values = data_conversion(len_north_y, char_df['y'])
        lat = res_y1_values / 111000 + lat_O

        char_df['f_x'] = res_x1_values
        char_df['f_y'] = res_y1_values
        char_df['f_longitude'] = lon
        char_df['f_latitude'] = lat

        # 删除列
        columns_to_delete = ['map_width_pixel', 'map_height_pixel', 'map_width_cm', 'map_height_cm']
        char_data = char_df.drop(columns_to_delete, axis=1)

        out_f = char_file.split(".")[0] + f'_xyToLonLat_ZCY.csv'
        df_write_to_csv(char_data, os.path.join(in_path, out_f))

        generate_images(res_x1_values, res_y1_values, lon, lat, in_path, '_走侧仪')

    @staticmethod
    def wifi_bluetooth_data_xyToLonLat(in_path):
        # 获取配置文件信息
        lon_O, lat_O, len_east_x, len_north_y = WalkTourIndoor.read_config_file(in_path)
        # 处理char数据
        in_wifi_bluetooth_file = get_file_by_string('_WiFi_BlueTooth', in_path)
        print('in_wifi_bluetooth_file: ', in_wifi_bluetooth_file)
        char_df = read_csv_get_df(in_wifi_bluetooth_file)

        res_x1_values = data_conversion(len_east_x, char_df['f_x'])
        lon = res_x1_values / (111000 * math.cos(lon_O / 180 * math.pi)) + lon_O
        lon = 2 * max(lon) - lon

        res_y1_values = data_conversion(len_north_y, char_df['f_y'])
        lat = res_y1_values / 111000 + lat_O

        char_df['f_x'] = res_x1_values
        char_df['f_y'] = res_y1_values
        char_df['f_longitude'] = lon
        char_df['f_latitude'] = lat

        # out_f = in_wifi_bluetooth_file.split(".")[0] + f'_{formatted_date}_xyToLonLat_WIFI_BlueTooth.csv'
        out_f = in_wifi_bluetooth_file.split(".")[0] + f'_xyToLonLat_WIFI_BlueTooth.csv'
        df_write_to_csv(char_df, os.path.join(in_path, out_f))

        generate_images(res_x1_values, res_y1_values, lon, lat, in_path, '_wifi_蓝牙')

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
    def get_merge_zcy_data(in_zcy_file, in_wifi_bluetooth_file):
        # 合并走测仪和wifi数据
        tmp_zcy_df = WalkTourIndoor.get_zcy_data(in_zcy_file)
        tmp_wifi_bluetooth_df = WalkTourIndoor.get_wifi_bluetooth_data(in_wifi_bluetooth_file)
        # 获取zcy，wifi数据
        tmp_merger_df = pd.merge(tmp_wifi_bluetooth_df, tmp_zcy_df, left_on="f_time", right_on="created_by_ue_time",
                                 how='left')
        return tmp_merger_df

    @staticmethod
    def get_merge_df_with_zcy(in_data_path):
        ue_file, table_file, zcy_file, wifi_bluetooth_file = WalkTour.get_data_file_path(in_data_path)

        print('zcy_file: ', zcy_file)
        # 获取所有数据的dataframe
        ue_df = WalkTour.deal_ue_table_df(ue_file, table_file)
        zcy_df = WalkTourIndoor.get_zcy_data(zcy_file)
        tmp_merge_df_data = WalkTourIndoor.merge_ue_zcy_df(ue_df, zcy_df)
        return tmp_merge_df_data

    @staticmethod
    def get_merge_df_with_zcy_wifi(in_data_path):
        ue_file, table_file, zcy_file, wifi_bluetooth_file = WalkTour.get_data_file_path(in_data_path)

        # 获取所有数据的dataframe
        ue_df = WalkTour.deal_ue_table_df(ue_file, table_file)
        zcy_wifi_df = WalkTourIndoor.get_merge_zcy_data(zcy_file, wifi_bluetooth_file)
        tmp_merge_df_data = WalkTourIndoor.merge_ue_zcy_df(ue_df, zcy_wifi_df)
        return tmp_merge_df_data

    @staticmethod
    def merge_ue_zcy_df(in_ue_df, in_zcy_df):
        in_ue_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_ue_df['PC Time'])
        if not in_zcy_df.empty:
            in_zcy_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_zcy_df['test_time'])
            tmp_df = pd.merge(in_ue_df, in_zcy_df)
            return tmp_df
        else:
            return in_ue_df


class WalkTour:
    @staticmethod
    def get_ue_table_df(in_path):
        tmp_ue_file, tmp_table_file, tmp_zcy_file, tmp_wifi_bluetooth_file = WalkTour.get_data_file_path(in_path)
        tmp_df = WalkTour.deal_ue_table_df(tmp_ue_file, tmp_table_file)
        return tmp_df

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
    def get_data_file_path(in_path):
        tmp_ue_file = get_file_by_string('UE', in_path)
        tmp_table_file = get_file_by_string('table', in_path)
        tmp_zcy_file = get_file_by_string('ZCY', in_path)
        tmp_wifi_bluetooth_file = get_file_by_string('xyToLonLat_WIFI_BlueTooth', in_path)
        return tmp_ue_file, tmp_table_file, tmp_zcy_file, tmp_wifi_bluetooth_file

    @staticmethod
    def WalkTourIndoor_deal_zcy_data(in_data_path):
        res_unzip_path = WalkTourIndoor.unzip_zcy_zip_file(in_data_path)
        res_zcy_data_list = WalkTourIndoor.get_zcy_data_file_list(res_unzip_path)
        WalkTourIndoor.copy_zcy_file_to_path(res_zcy_data_list, in_data_path)

        # 预处理zcy数据
        WalkTourIndoor.char_file_data_xyToLonLat(in_data_path)
        WalkTourIndoor.wifi_bluetooth_data_xyToLonLat(in_data_path)

    @staticmethod
    def deal_4g_df_data(log_df_4g):
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
        log_df_4g = wt_set_scene_data(log_df_4g)
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

        log_df_4g = log_df_4g.reindex(columns=WalkTour_table_format_dict['LTE'])
        # 计算领区数
        cell_number = deal_df_object.get_cell_number(log_df_4g)
        log_df_4g['f_neighbor_cell_number'] = cell_number
        DataPreprocessing.data_filling(log_df_4g, 'f_cell_id')

        log_df_4g = log_df_4g.rename(str.lower, axis='columns')
        return log_df_4g

    @staticmethod
    def deal_5g_df_data(log_df_5g):
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
        log_df_5g = wt_set_scene_data(log_df_5g)
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

        log_df_5g = log_df_5g.reindex(columns=WalkTour_table_format_dict['NR'])
        # 获取领区数
        num_list = deal_df_object.get_cell_number(log_df_5g)
        log_df_5g['f_neighbor_cell_number'] = num_list
        DataPreprocessing.data_filling(log_df_5g, 'f_cell_id')

        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        return log_df_5g

    @staticmethod
    def standard_output_name(in_path, in_net_type, in_name_ue, in_name_d_time):
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
        else:
            n_area = 'DaXin'

        file_name = f'{in_net_type}_{n_area}_{n_scenario}_WT_{in_net_type}_{in_name_ue}_{in_name_d_time}_{p_list[-3]}_{p_list[-4]}_{p_list[-2]}_LOG_UE_{p_list[-1]}'
        tmp_out_file = os.path.join(tmp_res_out_path, file_name + '.csv')
        tmp_cur_p_out_file = os.path.join(tmp_cur_out_path, file_name + '.csv')
        print('tmp_out_file: ', tmp_out_file)
        return tmp_out_file, tmp_cur_p_out_file

    @staticmethod
    def data_df_preprocess(in_data_df, in_data_path):
        # 获取数据是4G还是5G
        net_type = in_data_df['Network Type'][0]
        print('net_type: ', net_type)

        # 获取测试时间
        name_d_time = in_data_df['PC Time'][0].split(' ')[0]
        name_d_time = name_d_time[name_d_time.find('-'):].replace('-', '')
        print('name_d_time: ', name_d_time)

        # 设置msisdn
        if '8539' in in_data_path:
            f_msisdn = f_msisdn_dict['8539']
            in_data_df['f_msisdn'] = f_msisdn
            print('8539 f_msisdn: ', f_msisdn)
            name_ue = 'UE1'
        elif '2934' in in_data_path:
            f_msisdn = f_msisdn_dict['2934']
            in_data_df['f_msisdn'] = f_msisdn
            print('2934 f_msisdn: ', f_msisdn)
            name_ue = 'UE2'
        else:
            in_data_df['f_msisdn'] = f_msisdn_dict['2934']
            name_ue = 'UE'

        # 生成数据文件名
        tmp_out_file, tmp_cur_p_out_f = WalkTour.standard_output_name(in_data_path, net_type, name_ue, name_d_time)

        return in_data_df, tmp_out_file, tmp_cur_p_out_f

    # @staticmethod
    # def get_merge_df_with_zcy(in_data_path):
    #     ue_file, table_file, zcy_file, wifi_bluetooth_file = WalkTour.get_data_file_path(in_data_path)
    #
    #     # 获取所有数据的dataframe
    #     ue_df = WalkTour.deal_ue_table_df(ue_file, table_file)
    #     zcy_df = WalkTourIndoor.get_zcy_data(zcy_file)
    #     tmp_merge_df_data = WalkTour.merge_ue_zcy_df(ue_df, zcy_df)
    #     return tmp_merge_df_data
    #
    # @staticmethod
    # def get_merge_df_with_zcy_wifi(in_data_path):
    #     ue_file, table_file, zcy_file, wifi_bluetooth_file = WalkTour.get_data_file_path(in_data_path)
    #
    #     # 获取所有数据的dataframe
    #     ue_df = WalkTour.deal_ue_table_df(ue_file, table_file)
    #     zcy_wifi_df = WalkTourIndoor.get_merge_zcy_data(zcy_file, wifi_bluetooth_file)
    #     tmp_merge_df_data = WalkTour.merge_ue_zcy_df(ue_df, zcy_wifi_df)
    #     return tmp_merge_df_data


def wt_set_scene_data(log_df):
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


def run_deal_wt_4G(in_data_path, data_df):
    untreated_file = os.path.join(in_data_path, '4g原始_merge_文件.csv')
    df_write_to_csv(data_df, untreated_file)

    # 数据预处理
    data_df, out_file, cur_p_out_f = WalkTour.data_df_preprocess(data_df, in_data_path)
    # 数据处理
    res_data_df = WalkTour.deal_4g_df_data(data_df)
    # 结果保存
    df_write_to_csv(res_data_df, out_file)
    df_write_to_csv(res_data_df, cur_p_out_f)


def get_wt_indoor_df(in_data_path):
    # 解压处理zcy数据
    WalkTour.WalkTourIndoor_deal_zcy_data(in_data_path)
    # 获取merge df
    data_df = WalkTourIndoor.get_merge_df_with_zcy(in_data_path)
    return data_df
    # run_deal_wt_4G(in_data_path, data_df)
    # 保存处理前数据据
    # untreated_file = os.path.join(in_data_path, '4g原始_merge_文件.csv')
    # df_write_to_csv(data_df, untreated_file)
    #
    # # 数据预处理
    # data_df, out_file, cur_p_out_f = WalkTour.data_df_preprocess(data_df, in_data_path)
    # # 数据处理
    # res_data_df = WalkTour.deal_4g_df_data(data_df)
    # # 结果保存
    # df_write_to_csv(res_data_df, out_file)
    # df_write_to_csv(res_data_df, cur_p_out_f)


def get_wt_outdoor_df(in_data_path):
    data_df = WalkTour.get_ue_table_df(in_data_path)
    data_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(data_df['PC Time'])
    return data_df
    # run_deal_wt_4G(in_data_path, data_df)
    # 保存处理前数据据
    # untreated_file = os.path.join(in_data_path, '原始_merge_文件.csv')
    # df_write_to_csv(data_df, untreated_file)
    #
    # # 数据预处理
    # data_df, out_file, cur_p_out_f = WalkTour.data_df_preprocess(data_df, in_data_path)
    #
    # # 数据处理
    # res_data_df = WalkTour.deal_4g_df_data(data_df)
    # # 结果保存
    # df_write_to_csv(res_data_df, out_file)
    # df_write_to_csv(res_data_df, cur_p_out_f)


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

    wt_indoor_data_path = r'D:\working\1110室外\walktour\outdoor\LTE'
    # 处理indoor 4G
    # run_deal_wt_indoor_4G(wt_indoor_data_path)
    # run_deal_wt_outdoor_4G(wt_indoor_data_path)
    # 处理indoor 5G

    # data_df = WalkTour.get_ue_table_df(wt_indoor_data_path)
    # data_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(data_df['PC Time'])
    # # 保存处理前数据据
    # untreated_file = os.path.join(wt_indoor_data_path, '原始_merge_文件.csv')
    # df_write_to_csv(data_df, untreated_file)
    #
    # # 数据预处理
    # data_df, out_file, cur_p_out_f = WalkTour.data_df_preprocess(data_df, wt_indoor_data_path)
    #
    # # 数据处理
    # res_data_df = WalkTour.deal_4g_df_data(data_df)
    # # 结果保存
    # df_write_to_csv(res_data_df, out_file)
    # df_write_to_csv(res_data_df, cur_p_out_f)
