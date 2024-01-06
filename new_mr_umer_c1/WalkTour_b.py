# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

from Common import deal_df_object, get_file_dict, get_zcy_data, deal_ue_table_df, get_file_by_string, \
    standard_output_name, df_write_to_csv, get_all_data_path, get_dict_key_by_value, generate_output_file_name, \
    check_path, check_file_exists, delete_last_character
from DataPreprocessing import convert_timestamp_to_date, DataPreprocessing
from GlobalConfig import WalkTour_table_format_dict, f_msisdn_dict, tmp_res_out_path
from unzip_file import unzip_zcy_data


class WalkTour:
    # @staticmethod
    # def generate_output_file_name(in_df, in_net_type, in_n_scene):
    #     name_d_time = in_df['PC Time'][0].split(' ')[0]
    #     name_d_time = name_d_time[name_d_time.find('/'):].replace('/', '')
    #     print('name_d_time: ', name_d_time)
    #
    #     in_msisdn = in_df['f_msisdn'][0]
    #     n_dev_id = get_dict_key_by_value(f_msisdn_dict, in_msisdn)
    #     district = in_df['f_district'][0]
    #     if '海淀' in district:
    #         n_are = 'HaiDian'
    #     elif '朝阳' in district:
    #         n_are = 'CaoYang'
    #     else:
    #         n_are = 'DaXin'
    #     print(district)
    #     print('type: ', type(district))
    #     tmp_out_file_name = f'{in_net_type}_{n_are}_{in_n_scene}_WeTest_LOG_DT_UE_{n_dev_id}_{name_d_time}'
    #     print('tmp_out_file_name: ', tmp_out_file_name)
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
    def deal_4g_df_data(log_df_4g, in_set_scene_data):
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

        log_df_4g = log_df_4g.reindex(columns=WalkTour_table_format_dict['LTE'])
        # 计算领区数
        cell_number = deal_df_object.get_cell_number(log_df_4g)
        log_df_4g['f_neighbor_cell_number'] = cell_number
        DataPreprocessing.data_filling(log_df_4g, 'f_cell_id')

        log_df_4g = log_df_4g.rename(str.lower, axis='columns')
        return log_df_4g

    @staticmethod
    def deal_5g_df_data(log_df_5g, in_set_scene_data):
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

        log_df_5g = log_df_5g.reindex(columns=WalkTour_table_format_dict['NR'])
        # 获取领区数
        num_list = deal_df_object.get_cell_number(log_df_5g)
        log_df_5g['f_neighbor_cell_number'] = num_list
        DataPreprocessing.data_filling(log_df_5g, 'f_cell_id')

        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        return log_df_5g


def walk_tour_outdoor_get_df(in_data_path):
    file_list = ['UE', 'table']
    file_dict = get_file_dict(in_data_path, file_list)
    print(file_dict)
    res_tmp_df = deal_ue_table_df(file_dict['UE'], file_dict['table'])
    res_tmp_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(res_tmp_df['PC Time'])
    return res_tmp_df


def walk_tour_indoor_get_df(in_data_path):
    unzip_zcy_data(in_data_path)
    file_list = ['UE', 'table', 'xyToLonLat_ZCY']
    file_dict = get_file_dict(in_data_path, file_list)
    print(file_dict)
    zcy_df = get_zcy_data(file_dict['xyToLonLat_ZCY'])
    ue_table_merge_df = deal_ue_table_df(file_dict['UE'], file_dict['table'])
    res_tmp_df = WalkTour.merge_ue_zcy_df(ue_table_merge_df, zcy_df)
    return res_tmp_df


def walk_tour_outdoor(in_data_path, in_set_scene_data):
    print('室外')
    n_scene = 'Outdoor'
    res_df = walk_tour_outdoor_get_df(in_data_path)
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


def walk_tour_indoor(in_data_path, in_set_scene_data):
    print('室内')
    n_scene = 'indoor'
    res_df = walk_tour_indoor_get_df(in_data_path)
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


def walk_tour(in_data_path, in_set_scene_data):
    if get_file_by_string('zip', in_data_path):
        print('室内')
        n_scene = 'indoor'
        res_df = walk_tour_indoor_get_df(in_data_path)
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
    else:
        print('室外')
        n_scene = 'Outdoor'
        res_df = walk_tour_outdoor_get_df(in_data_path)
        net_type = res_df['Network Type'][0]
        print('net_type: ', net_type)
        if 'LTE' == net_type:
            print('4G')
            net_type = '4G'
            res_df = WalkTour.deal_4g_df_data(res_df, in_set_scene_data)
        elif 'NR' == net_type:
            print('5G')
            net_type = '5G'
            res_df = WalkTour.deal_5g_df_data(res_df, in_set_scene_data)
        else:
            print(f'net_type:{net_type} error')

    # 获取测试时间
    # name_d_time = res_df['pc_time'][0].split(' ')[0]
    # name_d_time = name_d_time[name_d_time.find('-'):].replace('-', '')
    # print('name_d_time: ', name_d_time)

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

    # out_file, cur_p_out_f = standard_output_name(data_path, net_type, 'ue_demo', name_d_time)
    # df_write_to_csv(res_df, out_file)
    # df_write_to_csv(res_df, cur_p_out_f)


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
    # f_msisdn = '533F8040D9351F4A9499FC7825805B14'

    # 检查输出目录是否存在
    # check_path(tmp_res_out_path)

    data_path = r'D:\working\1206_国际财经中心测试V1\场景2\8539'
    data_path_list = get_all_data_path(data_path, 'table')  # outdoor
    for i_path in data_path_list:
        print('i_path: ', i_path)
        if '2934' in i_path:
            f_msisdn = f_msisdn_dict['2934']
        elif '8539' in i_path:
            f_msisdn = f_msisdn_dict['8539']
        else:
            f_msisdn = f_msisdn_dict['8539']
        walk_tour(i_path, wt_indoor_set_scene_data)

    # indoor_data_path = r'D:\working\1206_2934国际财经中心测试'
    # indoor_data_path_list = get_all_data_path(indoor_data_path, 'table')
    # for i_path in indoor_data_path_list:
    #     print('i_path: ', i_path)
    #     if '2934' in i_path:
    #         f_msisdn = f_msisdn_dict['2934']
    #     elif '8539' in i_path:
    #         f_msisdn = f_msisdn_dict['8539']
    #     else:
    #         f_msisdn = f_msisdn_dict['8539']
    #     walk_tour_indoor(i_path, wt_indoor_set_scene_data)

    # outdoor_data_path = r'E:\work\demo_merge\demo_test_data\walktour\outdoor'
    # outdoor_data_path_list = get_all_data_path(outdoor_data_path, 'table')
    # for i_path in outdoor_data_path_list:
    #     print('i_path: ', i_path)
    #     if '2934' in i_path:
    #         f_msisdn = f_msisdn_dict['2934']
    #     elif '8539' in i_path:
    #         f_msisdn = f_msisdn_dict['8539']
    #     else:
    #         f_msisdn = f_msisdn_dict['8539']
    #     walk_tour_outdoor(i_path, wt_indoor_set_scene_data)
