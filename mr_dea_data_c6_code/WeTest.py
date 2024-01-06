# -*- coding: utf-8 -*-

import os
import re

import numpy as np

from Common import deal_df_object, check_path, check_file_exists, split_path_get_list, df_write_to_csv
from CommonDealData import DealData
from DataPreprocessing import DataPreprocessing, convert_timestamp_to_date
from Config import set_scene_data, WeTest_table_format_dict, f_msisdn_dict, name_test_dev, name_test_type, \
    tmp_res_out_path


class WeTest:
    @staticmethod
    def deal_wetest_4g(log_df_4g, in_net_type):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        log_df_4g = deal_df_object.delete_second_level_duplicate_data(log_df_4g, log_df_4g['ts'])

        cell_cnt = 0
        while True:
            cell_cnt += 1
            if f'lte_neighbor_cell_{cell_cnt}_freq' in log_df_4g.columns:
                prefix = f'lte_neighbor_cell_{cell_cnt}'
                suffixes = ['_freq', '_pci', '_rsrp', '_rsrq']
                change_dict = {'_freq': 'f_freq_n', '_pci': 'f_pci_n', '_rsrp': 'f_rsrp_n', '_rsrq': 'f_rsrq_n'}

                for suffix in suffixes:
                    old_column_name = f'{prefix} {suffix}'
                    new_column_name = f'{change_dict[suffix]}{cell_cnt}'

                    log_df_4g.rename(columns={old_column_name: new_column_name}, inplace=True)
            else:
                break

        log_df_4g = log_df_4g.rename(
            columns={
                'imsi': 'f_imsi',  # table
                'imei': 'f_imei',  # table
            })

        log_df_4g = log_df_4g.rename(
            columns={
                'lte_eci': 'f_cell_id',
                'ts': 'f_time',
                'lte_serving_cell_pci': 'f_pci',
                'lte_serving_cell_freq': 'f_freq',
                'lte_serving_cell_rsrp': 'f_rsrp',
                'lte_serving_cell_rsrq': 'f_rsrq',
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

        log_df_4g = log_df_4g.reindex(columns=WeTest_table_format_dict[in_net_type])
        DataPreprocessing.data_filling(log_df_4g, 'f_cell_id')

        # # 标题统一小写
        log_df_4g = log_df_4g.rename(str.lower, axis='columns')
        return log_df_4g

    @staticmethod
    def deal_wetest_5g(log_df_5g, in_net_type):
        # 删除测试log中 秒级重复数据，同秒取第一条。
        log_df_5g = log_df_5g.groupby(log_df_5g['ts']).first().reset_index()

        # 定义要进行替换的正则表达式模式
        pattern = re.compile(r'NCell(\d)(\d)')
        # 使用正则表达式匹配并替换列名
        log_df_5g.columns = [pattern.sub(lambda x: f'NCell{x.group(1)}', col) for col in log_df_5g.columns]

        cnt = 0
        while True:
            cnt += 1
            if f'nr_neighbor_cell_{cnt}_freq' in log_df_5g.columns:
                prefix = f'nr_neighbor_cell_{cnt}'
                suffixes = ['_freq', '_pci', '_rsrp', '_rsrq', '_sinr']
                change_dict = {'_freq': 'f_freq_n', '_pci': 'f_pci_n', '_rsrp': 'f_rsrp_n', '_rsrq': 'f_rsrq_n', '_sinr': 'f_sinr_n'}

                for suffix in suffixes:
                    old_column_name = f'{prefix}{suffix}'
                    new_column_name = f'{change_dict[suffix]}{cnt}'

                    log_df_5g.rename(columns={old_column_name: new_column_name}, inplace=True)
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
                'pc_time': 'pc_time',
            })
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

        # SID暂时都赋值1
        log_df_5g['f_sid'] = 1
        log_df_5g['f_pid'] = (log_df_5g.index + 1).astype(str)

        log_df_5g = log_df_5g.reindex(columns=WeTest_table_format_dict[in_net_type])
        DataPreprocessing.data_filling(log_df_5g, 'f_cell_id')

        log_df_5g = log_df_5g.rename(str.lower, axis='columns')
        return log_df_5g

    @staticmethod
    def deal_data_only_zcy_indoor(in_data_path, name_are):
        print('in_data_path: ', in_data_path)
        cru_out_p = os.path.join(in_data_path, 'output')
        check_path(cru_out_p)
        # 获取数据文件路径
        ue_file, table_file, zcy_file, wifi_bluetooth_file = DealData.get_data_file_path(in_data_path)
        print('zcy_file: ', zcy_file)
        # 读取ue数据
        ue_df = DealData.deal_ue_table_df(ue_file, table_file)
        # 合并走测仪和wifi数据
        name_scenario = 'Outdoor'
        if zcy_file and check_file_exists(zcy_file):  # 如果存在走测仪数据，则是室内，否则是室外数据
            name_scenario = 'Indoor'  # 有走测仪数据，就合并走测仪和wifi数据
            zcy_df = DealData.get_zcy_data(zcy_file)
            df_data = DealData.merge_ue_zcy_df_data(ue_df, zcy_df)
        else:
            df_data = ue_df

        if not ue_df['Network Type'].empty:
            # 获取网络类型和时间，从df中
            net_type = ue_df['Network Type'][0]
            print('net_type: ', net_type)
            name_d_time = ue_df['PC Time'][0].split(' ')[0]
            name_d_time = name_d_time[name_d_time.find('-'):].replace('-', '')
            print('name_d_time: ', name_d_time)

            name_ue = 'UE'
            # 设置f_msisdn
            if '8539' in in_data_path:
                f_msisdn = f_msisdn_dict['8539']
                df_data['f_msisdn'] = f_msisdn
                print('8539 f_msisdn: ', f_msisdn)
                name_ue = 'UE1'
            if '2934' in in_data_path:
                f_msisdn = f_msisdn_dict['2934']
                df_data['f_msisdn'] = f_msisdn
                print('2934 f_msisdn: ', f_msisdn)
                name_ue = 'UE2'

            p_list = split_path_get_list(in_data_path)
            print('p_list: ', p_list)

            file_name = f'{net_type}_{name_are}_{name_scenario}_{name_test_dev}_{name_test_type}_{name_ue}_{name_d_time}_{p_list[-3]}_{p_list[-4]}_{p_list[-2]}_WT_LOG_DT_UE_{p_list[-1]}'
            out_file = os.path.join(tmp_res_out_path, file_name + '.csv')
            cru_p_out_file = os.path.join(cru_out_p, file_name + '.csv')
            print('out_file: ', out_file)

            if check_file_exists(table_file):  # 如果有table数据，就是walktour数据，否则就是wetest数据
                if 'LTE' == net_type:
                    untreated_file = os.path.join(in_data_path, 'lte_原始_merge_文件.csv')
                    df_write_to_csv(df_data, untreated_file)
                    res_df_data = WeTest.deal_wetest_4g(df_data, net_type)
                    df_write_to_csv(res_df_data, out_file)
                    df_write_to_csv(res_df_data, cru_p_out_file)
                elif 'NR' == net_type:
                    untreated_file = os.path.join(in_data_path, 'nr_原始_merge_文件.csv')
                    df_write_to_csv(df_data, untreated_file)
                    nr_res_df = WeTest.deal_wetest_5g(df_data, net_type)
                    df_write_to_csv(nr_res_df, out_file)


# else:
# if 'LTE' == net_type:
#     untreated_file = os.path.join(in_data_path, '原始_merge_文件.csv')
#     df_write_to_csv(df_data, untreated_file)
#     res_df_data = DealData.deal_wetest_4g(df_data, net_type)
#     df_write_to_csv(res_df_data, out_file)
# elif 'NR' == net_type:
#     untreated_file = os.path.join(in_data_path, '原始_merge_文件.csv')
#     df_write_to_csv(df_data, untreated_file)
#     nr_res_df = DealData.deal_wetest_5g(df_data, net_type)
#     df_write_to_csv(nr_res_df, out_file)