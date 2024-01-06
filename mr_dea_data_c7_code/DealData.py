# -*- coding: utf-8 -*-
import re

from Common import *
from GlobalConfig import set_scene_data, WalkTour_table_format_dict, WeTest_table_format_dict
from DataPreprocessing import DataPreprocessing


class DealData:
    @staticmethod
    def deal_WalkTour_4g(log_df_4g, in_net_type):
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
    def deal_WalkTour_5g(log_df_5g, in_net_type):
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
