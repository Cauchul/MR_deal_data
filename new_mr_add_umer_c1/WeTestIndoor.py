# -*- coding: utf-8 -*-
import configparser
import math
import os
import re

import numpy as np
import pandas as pd

from Common import clear_path, get_file_by_string, unzip, copy_file, read_csv_get_df, data_conversion, \
    df_write_to_csv, generate_images, deal_df_object, split_path_get_list, check_path, clear_merge_path
from DataPreprocessing import DataPreprocessing, convert_timestamp_to_date
from GlobalConfig import WalkTour_table_format_dict, f_msisdn_dict, tmp_res_out_path, TableFormat, \
    WeTest_table_format_dict
from standard_output_data_name import standard_out_file

f_source = '测试log'  # 测试log；2：MR软采；3：扫频仪；4：WIFI；5：OTT；6：蓝牙;7.WeTest_Log
f_province = "北京"
f_city = "北京"
f_prru_id = 0


class WeTestIndoor:
    def deal_wetest_4g(self, log_df_4g):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        deal_df_object.delete_second_level_duplicate_data(log_df_4g, log_df_4g['ts'])

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
                'ts': 'f_time',
                'lte_serving_cell_pci': 'f_pci',
                'lte_serving_cell_freq': 'f_freq',
                'lte_serving_cell_rsrp': 'f_rsrp',
                'lte_serving_cell_rsrq': 'f_rsrq',
            })
        # 删除重复列
        log_df_4g = deal_df_object.delete_duplicate_columns(log_df_4g)

        # 设置场景信息
        log_df_4g = wetest_indoor_set_scene_data(log_df_4g)
        # 时间转上海时区
        sh_timez = deal_df_object.change_to_Shanghai_time_zone(log_df_4g['f_time'])
        log_df_4g['f_time_1'] = sh_timez
        print('sh_timez: ', sh_timez.values)
        # 生成finger_id
        finger_id = deal_df_object.generate_finger_id(log_df_4g['f_time_1'], log_df_4g['f_msisdn'])
        log_df_4g['finger_id'] = finger_id
        # 获取领区数
        num_list = deal_df_object.get_cell_number(log_df_4g)
        log_df_4g['f_neighbor_cell_number'] = num_list
        # 置空 UEMR 数据
        log_df_4g = deal_df_object.add_and_empty_UEMR_data(log_df_4g)

        log_df_4g['f_enb_id'] = log_df_4g['f_cell_id'] // 256

        print('f_time: ', log_df_4g['f_time'].values)

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

    def deal_wetest_5g(self, log_df_5g):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        log_df_5g = log_df_5g.groupby(log_df_5g['ts']).first().reset_index()

        # 定义要进行替换的正则表达式模式
        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_5g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_5g.columns]

        cell_cnt = 0
        while True:
            cell_cnt += 1
            if f'nr_neighbor_cell_{cell_cnt}_freq' in log_df_4g.columns:
                log_df_4g = log_df_4g.rename(
                    columns={
                        f'nr_neighbor_cell_{cell_cnt}_freq': f'f_freq_n{cell_cnt}',
                        f'nr_neighbor_cell_{cell_cnt}_pci': f'f_pci_n{cell_cnt}',
                        f'nr_neighbor_cell_{cell_cnt}_ssb_rsrp': f'f_rsrp_n{cell_cnt}',
                        f'nr_neighbor_cell_{cell_cnt}_ssb_rsrq': f'f_rsrq_n{cell_cnt}',
                        f'nr_neighbor_cell_{cell_cnt}_ssb_sinr': f'f_sinr_n{cell_cnt}',
                    })
            else:
                break

        log_df_5g = log_df_5g.rename(
            columns={
                'imsi': 'f_imsi',
                'imei': 'f_imei',
                'nci': 'f_cell_id',
                'ts': 'f_time',
                'nr_serving_cell_pci': 'f_pci',
                'nr_serving_cell_freq': 'f_freq',
                'nr_serving_cell_ssb_rsrp': 'f_rsrp',
                'nr_serving_cell_ssb_rsrq': 'f_rsrq',
                'nr_serving_cell_ssb_sinr': 'f_sinr',
            })
        # 删除重复列
        log_df_5g = deal_df_object.delete_duplicate_columns(log_df_5g)
        # 设置场景信息
        log_df_5g = wetest_indoor_set_scene_data(log_df_5g)
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

        # SID暂时都赋值1
        log_df_5g['f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)

        log_df_5g = log_df_5g.reindex(columns=WeTest_table_format_dict['NR'])
        DataPreprocessing.data_filling(log_df_5g, 'f_cell_id')

        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        return log_df_5g

    def unzip_zcy_zip_file(self, in_path):
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

    def get_zcy_data_file_list(self, in_unzip_path):
        tmp_list = []
        for root, dirs, files in os.walk(in_unzip_path):
            for file in files:
                if '-chart' in file or '_pci_' in file or '_WiFi_BlueTooth' in file:
                    file_path = os.path.join(root, file)
                    print('file_path: ', file_path)
                    tmp_list.append(file_path)
        return tmp_list

    def copy_zcy_file_to_path(self, in_file_list, in_out_path):
        for in_i_f in in_file_list:
            print('in_i_f: ', in_i_f)
            copy_file(in_i_f, in_out_path)

    def get_data_file_path(self, in_path):
        tmp_ue_file = get_file_by_string('UE', in_path)
        tmp_table_file = get_file_by_string('table', in_path)
        tmp_zcy_file = get_file_by_string('ZCY', in_path)
        tmp_wifi_bluetooth_file = get_file_by_string('xyToLonLat_WIFI_BlueTooth', in_path)
        return tmp_ue_file, tmp_table_file, tmp_zcy_file, tmp_wifi_bluetooth_file

    # 读取配置文件
    def read_config_file(self, in_config_path):
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

    def char_file_data_xyToLonLat(self, in_path):
        # 获取配置文件信息
        lon_O, lat_O, len_east_x, len_north_y = self.read_config_file(in_path)
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

    def wifi_bluetooth_data_xyToLonLat(self, in_path):
        # 获取配置文件信息
        lon_O, lat_O, len_east_x, len_north_y = self.read_config_file(in_path)
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

    def deal_ue_table_df(self, in_ue_file, in_table_file):
        in_ue_df = read_csv_get_df(in_ue_file)
        if in_table_file and os.path.exists(in_table_file):
            in_table_df = read_csv_get_df(in_table_file)
            res_tmp_merge_df = pd.merge(in_ue_df, in_table_df, left_on="PC Time", right_on="PCTime", how='left')
            return res_tmp_merge_df
        else:
            return in_ue_df

    def get_zcy_data(self, in_zcy_file):
        in_zcy_df = read_csv_get_df(in_zcy_file)
        # tmp_zcy_df = in_zcy_df[
        #     ['test_time', 'created_by_ue_time', 'f_x', 'f_y', 'f_longitude', 'f_latitude', 'direction', 'altitude']]
        tmp_zcy_df = in_zcy_df[
            ['test_time', 'created_by_ue_time', 'f_x', 'f_y', 'f_longitude', 'f_latitude', 'altitude']]
        return tmp_zcy_df

    def get_wifi_bluetooth_data(self, in_wifi_bluetooth_file):
        in_wifi_df = read_csv_get_df(in_wifi_bluetooth_file)
        tmp_wifi_df = in_wifi_df.drop(['f_x', 'f_y', 'f_longitude', 'f_latitude', 'f_direction', 'f_altitude'], axis=1)
        return tmp_wifi_df

    def get_merge_zcy_data(self, in_zcy_file, in_wifi_bluetooth_file):
        # 合并走测仪和wifi数据
        tmp_zcy_df = self.get_zcy_data(in_zcy_file)
        tmp_wifi_bluetooth_df = self.get_wifi_bluetooth_data(in_wifi_bluetooth_file)
        # 获取zcy，wifi数据
        tmp_merger_df = pd.merge(tmp_wifi_bluetooth_df, tmp_zcy_df, left_on="f_time", right_on="created_by_ue_time",
                                 how='left')
        return tmp_merger_df

    def merge_ue_zcy_df_data(self, in_ue_df, in_zcy_df):
        in_ue_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp_v1(in_ue_df['pc_time'])
        if not in_zcy_df.empty:
            in_zcy_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_zcy_df['test_time'])
            tmp_df = pd.merge(in_ue_df, in_zcy_df)
            return tmp_df
        else:
            return in_ue_df


def wetest_indoor_set_scene_data(log_df):
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

    file_name = f'{in_net_type}_{n_area}_{n_scenario}_WT_{n_test_type}_{in_name_ue}_{in_name_d_time}_{p_list[-3]}_{p_list[-4]}_{p_list[-2]}_LOG_UE_{p_list[-1]}'
    tmp_out_file = os.path.join(tmp_res_out_path, file_name + '.csv')
    tmp_cur_p_out_file = os.path.join(tmp_cur_out_path, file_name + '.csv')
    print('tmp_out_file: ', tmp_out_file)
    return tmp_out_file, tmp_cur_p_out_file


# 解压zcy zip文件
def unzip_zcy_zip_file(in_data_path, wifi_flag=False):
    # 解压zip到当前目录下
    in_extraction_path = os.path.join(in_data_path, 'unzip')
    clear_path(in_extraction_path)
    in_zip_file = get_file_by_string('zip', in_data_path)
    unzip(in_zip_file, in_extraction_path)
    # 拷贝char和wifi数据
    for root, dirs, files in os.walk(in_extraction_path):
        for file in files:
            if '-chart' in file or '_pci_' in file or '_WiFi_BlueTooth' in file:
                file_path = os.path.join(root, file)
                print('file_path: ', file_path)
                copy_file(file_path, in_data_path)


# 处理char和wifi原始数据
def deal_zcy_wifi_data(in_data_path):
    # 获取配置文件信息
    WeTestIn = WeTestIndoor()
    lon_O, lat_O, len_east_x, len_north_y = WeTestIn.read_config_file(in_data_path)

    # 获取char文件
    char_file = get_file_by_string('-chart', in_data_path)
    print('char_file: ', char_file)
    char_df = read_csv_get_df(char_file)


def get_merge_log_data(in_data_path):
    WeTestIn = WeTestIndoor()
    res_unzip_path = WeTestIn.unzip_zcy_zip_file(in_data_path)
    res_zcy_data_list = WeTestIn.get_zcy_data_file_list(res_unzip_path)
    WeTestIn.copy_zcy_file_to_path(res_zcy_data_list, in_data_path)

    # 预处理zcy数据
    WeTestIn.char_file_data_xyToLonLat(in_data_path)
    # 获取ue、table等数据名称
    ue_file, table_file, zcy_file, wifi_bluetooth_file = WeTestIn.get_data_file_path(in_data_path)

    # 获取所有数据的dataframe
    ue_df = WeTestIn.deal_ue_table_df(ue_file, table_file)
    zcy_df = WeTestIn.get_zcy_data(zcy_file)
    df_data = WeTestIn.merge_ue_zcy_df_data(ue_df, zcy_df)
    return df_data


def process_log_data(in_df_data, in_data_path, net_type):
    # 解压zcy数据
    WeTestIn = WeTestIndoor()
    # in_df_data = get_merge_df(in_data_path)
    # 获取测试时间
    name_d_time = in_df_data['pc_time'][0].split(' ')[0]
    name_d_time = name_d_time[name_d_time.find('-'):].replace('-', '')
    print('name_d_time: ', name_d_time)

    # 设置msisdn
    if '8539' in in_data_path:
        f_msisdn = f_msisdn_dict['8539']
        in_df_data['f_msisdn'] = f_msisdn
        print('8539 f_msisdn: ', f_msisdn)
        name_ue = 'UE1'
    elif '2934' in in_data_path:
        f_msisdn = f_msisdn_dict['2934']
        in_df_data['f_msisdn'] = f_msisdn
        print('2934 f_msisdn: ', f_msisdn)
        name_ue = 'UE2'
    else:
        name_ue = 'UE'
        in_df_data['f_msisdn'] = f_msisdn_dict['2934']

    # 生成数据文件名
    out_file, cur_p_out_f = standard_output_name(in_data_path, net_type, name_ue, name_d_time)
    if 'LTE' == net_type:
        # 处理前的原始文件保存一份
        untreated_file = os.path.join(in_data_path, '4g原始_merge_文件.csv')
        df_write_to_csv(in_df_data, untreated_file)
        # 处理数据
        res_df_data = WeTestIn.deal_wetest_4g(in_df_data)
        df_write_to_csv(res_df_data, out_file)
        df_write_to_csv(res_df_data, cur_p_out_f)
    elif 'NR' == net_type:
        untreated_file = os.path.join(in_data_path, '5g原始_merge_文件.csv')
        df_write_to_csv(in_df_data, untreated_file)
        nr_res_df = WeTestIn.deal_wetest_5g(in_df_data)
        df_write_to_csv(nr_res_df, out_file)
        df_write_to_csv(nr_res_df, cur_p_out_f)


n_test_type = 'DT'
if __name__ == '__main__':
    f_device_brand = 'HUAWEI'
    f_device_model = "P40"
    f_area = '国际财经中心'
    f_floor = '1F'
    f_scenario = 1
    f_district = '朝阳区'
    f_street = '西三环北路玲珑路南蓝靛厂南路北洼西街'
    f_building = '国际财经中心'
    # 数据路径
    wetest_indoor_data_path = r'E:\work\demo_merge\demo_test_data\wetest\indoor\4G'
    # 设置输出路径
    out_data_path = r'E:\work\demo_merge\merged'
    clear_merge_path(out_data_path)
    check_path(out_data_path)
    # 获取数据
    df_data = get_merge_log_data(wetest_indoor_data_path)
    # 处理数据
    process_log_data(df_data, wetest_indoor_data_path, 'LTE')
    # 标准化输出文件的文件名
    standard_out_file(out_data_path, in_clear_flag=False)
