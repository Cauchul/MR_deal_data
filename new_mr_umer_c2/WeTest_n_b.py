# -*- coding: utf-8 -*-
import os
import re

import numpy as np
import pandas as pd

from Common import get_file_by_string, get_file_dict, get_zcy_data, read_csv_get_df, \
    deal_df_object, standard_output_name, df_write_to_csv, list_files_in_directory, get_dict_key_by_value, \
    generate_output_file_name, get_all_data_path, check_path, split_path_get_list, check_file_exists, DealTime
from DataPreprocessing import DataPreprocessing, convert_timestamp_to_date
from GlobalConfig import WeTest_table_format_dict, f_msisdn_dict, tmp_res_out_path
from unzip_file import unzip_zcy_data


class WeTest:
    @staticmethod
    def indoor_get_df(in_data_path):
        unzip_zcy_data(in_data_path)
        in_file_list = ['UE', 'xyToLonLat_ZCY']
        file_dict = get_file_dict(in_data_path, in_file_list)
        print(file_dict)
        zcy_df = get_zcy_data(file_dict['xyToLonLat_ZCY'])
        res_ue_df = read_csv_get_df(file_dict['UE'])
        # res_tmp_df = WeTest.merge_ue_zcy_df(res_ue_df, zcy_df)
        res_tmp_df = WeTest.merge_ue_zcy_df_convert_format(res_ue_df, zcy_df)
        return res_tmp_df

    # 输入ue文件而不是路径
    @staticmethod
    def outdoor_get_df(res_ue_file):
        res_ue_df = read_csv_get_df(res_ue_file)
        res_ue_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp_v1(res_ue_df['pc_time'])
        return res_ue_df

    @staticmethod
    def wetest_generate_output_file_name(in_data_path, in_df, in_net_type, in_n_scene, in_test_type):
        tmp_cur_out_path = os.path.join(in_data_path, 'output')
        check_path(tmp_cur_out_path)

        tmp_split_list = split_path_get_list(in_data_path)

        name_d_time = in_df['pc_time'][0].split(' ')[0]
        name_d_time = name_d_time[name_d_time.find('/'):].replace('/', '')
        print('name_d_time: ', name_d_time)

        name_d_time2 = in_df['pc_time'][0].split(' ')[0]
        name_d_time2 = name_d_time2[name_d_time2.find('-'):].replace('-', '')
        print('name_d_time2: ', name_d_time2)

        if len(name_d_time2) > len(name_d_time):
            name_d_time = name_d_time2

        in_msisdn = in_df['f_msisdn'][0]
        n_dev_id = get_dict_key_by_value(f_msisdn_dict, in_msisdn)
        district = in_df['f_district'][0]
        if '海淀' in district:
            n_are = 'HaiDian'
        elif '朝阳' in district:
            n_are = 'CaoYang'
        else:
            n_are = 'DaXin'
        print(district)
        print('type: ', type(district))
        if in_n_scene:
            tmp_out_file_name = f'{in_net_type}_{n_are}_{in_n_scene}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}_{tmp_split_list[-1]}'
            # tmp_out_file_name = f'{in_net_type}_{n_are}_{in_n_scene}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}'
        else:
            tmp_out_file_name = f'{in_net_type}_{n_are}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}_{tmp_split_list[-1]}'
            # tmp_out_file_name = f'{in_net_type}_{n_are}_{in_test_type}_LOG_DT_UE_{n_dev_id}_{name_d_time}'

        tmp_out_file = os.path.join(tmp_res_out_path, tmp_out_file_name)
        print('tmp_out_file: ', tmp_out_file)
        tmp_cur_p_out_file = os.path.join(tmp_cur_out_path, tmp_out_file_name + '.csv')
        return tmp_out_file, tmp_cur_p_out_file

    @staticmethod
    def merge_ue_zcy_df_by_file():
        pass

    @staticmethod
    def merge_ue_zcy_df_convert_format(in_ue_df, in_zcy_df):
        in_ue_df['pc_time'] = DealTime.time_str_format(in_ue_df['pc_time'])

        in_ue_df = pd.merge(in_ue_df, in_zcy_df, left_on="pc_time", right_on="test_time", how='left')
        in_ue_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_ue_df['pc_time'])
        return in_ue_df

    @staticmethod
    def merge_ue_zcy_df(in_ue_df, in_zcy_df):
        in_ue_df = pd.merge(in_ue_df, in_zcy_df, left_on="pc_time", right_on="test_time", how='left')
        in_ue_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp_v1(in_ue_df['pc_time'])
        return in_ue_df
        # in_ue_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp_v1(in_ue_df['pc_time'])
        # if not in_zcy_df.empty:
        #     in_zcy_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_zcy_df['test_time'])
        #     tmp_df = pd.merge(in_ue_df, in_zcy_df)
        #     return tmp_df
        # else:
        #     return in_ue_df

    @staticmethod
    def deal_wetest_4g(log_df_4g, in_set_scene_data):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        deal_df_object.delete_second_level_duplicate_data(log_df_4g, log_df_4g['ts'])

        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_4g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_4g.columns]

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
        log_df_4g = in_set_scene_data(log_df_4g)
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

    @staticmethod
    def deal_wetest_5g(log_df_5g, in_set_scene_data):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        # print('log_df_5g: ', log_df_5g.columns)
        log_df_5g = log_df_5g.groupby(log_df_5g['ts']).first().reset_index()
        # 定义要进行替换的正则表达式模式
        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_5g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_5g.columns]

        cell_cnt = 0
        while True:
            cell_cnt += 1
            if f'nr_neighbor_cell_{cell_cnt}_freq' in log_df_5g.columns:
                log_df_5g = log_df_5g.rename(
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
        log_df_5g = in_set_scene_data(log_df_5g)
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


def wetest_outdoor(in_data_path, in_set_scene_data):
    print('-------start wetest_outdoor---------')
    if get_file_by_string('zip', in_data_path):
        print(f'error, 路径:{in_data_path} 下存在ZCY zip文件，不是outdoor数据路径')
        return
    n_scene = 'Outdoor'
    print('室外')
    file_list = list_files_in_directory(in_data_path)
    for i_data_f in file_list:
        net_type = ''
        in_res_df = WeTest.outdoor_get_df(i_data_f)
        if 'lte_enb_id' in in_res_df.columns:
            net_type = '4G'
            in_res_df = WeTest.deal_wetest_4g(in_res_df, in_set_scene_data)
        elif 'nr_gnb_id' in in_res_df.columns:
            net_type = '5G'
            in_res_df = WeTest.deal_wetest_5g(in_res_df, in_set_scene_data)
        else:
            print('i_data_f: ', i_data_f)
            print(f'error, f_enb_id and f_gnb_id not find in res_df columns')

        out_file, cur_p_out_f = WeTest.wetest_generate_output_file_name(in_data_path, in_res_df, net_type, n_scene,
                                                                        'WeTest')

        if check_file_exists(out_file + '.csv'):
            i = 0
            while True:
                i += 1
                if check_file_exists(out_file + f'_v{i}' + '.csv'):
                    continue
                else:
                    out_file = out_file + f'_v{i}.csv'
                    break
        else:
            out_file = out_file + '.csv'

        df_write_to_csv(in_res_df, out_file)
        df_write_to_csv(in_res_df, cur_p_out_f)


def wetest_indoor(in_data_path, in_set_scene_data):
    print('-------start wetest_indoor---------')
    if not get_file_by_string('zip', in_data_path):
        print(f'error, 路径:{in_data_path} 下没有找到 ZCY zip文件，不是indoor数据路径')
        return
    net_type = ''
    n_scene = 'Indoor'
    print('室内')
    in_res_df = WeTest.indoor_get_df(in_data_path)
    if 'lte_enb_id' in in_res_df.columns:
        print('4G')
        net_type = '4G'
        in_res_df = WeTest.deal_wetest_4g(in_res_df, in_set_scene_data)
    elif 'nr_gnb_id' in in_res_df.columns:
        print('5G')
        net_type = '5G'
        in_res_df = WeTest.deal_wetest_5g(in_res_df, in_set_scene_data)
    else:
        print(f'error, f_enb_id and f_gnb_id not find in in_res_df columns')

    out_file, cur_p_out_f = WeTest.wetest_generate_output_file_name(in_data_path, in_res_df, net_type, n_scene,
                                                                    'WeTest')
    if check_file_exists(out_file + '.csv'):
        i = 0
        while True:
            i += 1
            if check_file_exists(out_file + f'_v{i}' + '.csv'):
                continue
            else:
                out_file = out_file + f'_v{i}.csv'
                break
    else:
        out_file = out_file + '.csv'

    df_write_to_csv(in_res_df, out_file)
    df_write_to_csv(in_res_df, cur_p_out_f)


def wetest(in_data_path, in_set_scene_data):
    if get_file_by_string('zip', in_data_path):
        wetest_indoor(in_data_path, in_set_scene_data)
    else:
        wetest_outdoor(in_data_path, in_set_scene_data)


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

    # 设置f_msisdn
    f_msisdn = '533F8040D9351F4A9499FC7825805B14'
    # 检查数据统一输出目录是否存在，不存在则创建目录
    check_path(tmp_res_out_path)

    data_path = r'D:\working\1214\1214国际财经中心(1)\国际财经中心\国际财经中心5G纵1_20231214\wetest'

    # wetest的ue标志是UE_flag
    data_path_list = get_all_data_path(data_path, 'UE')
    data_path_list = list(set(data_path_list))
    # 去重
    print('data_path_list: ', data_path_list)
    for i_path in data_path_list:
        print('i_path: ', i_path)
        if '2934' in i_path:
            f_msisdn = f_msisdn_dict['2934']
        elif '8539' in i_path:
            f_msisdn = f_msisdn_dict['8539']
        wetest(i_path, wetest_set_scene_data)
        # wetest_indoor(i_path, wetest_set_scene_data)
        # wetest_outdoor(i_path, wetest_set_scene_data)
