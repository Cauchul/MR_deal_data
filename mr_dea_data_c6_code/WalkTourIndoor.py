# -*- coding: utf-8 -*-

import os

import numpy as np

from Common import deal_df_object, check_file_exists, split_path_get_list, df_write_to_csv, check_path
from DataPreprocessing import convert_timestamp_to_date, DataPreprocessing
from Config import set_scene_data, WalkTour_table_format_dict, f_msisdn_dict, name_test_dev, name_test_type, \
    tmp_res_out_path
from CommonDealData import DealData


class WalkTour:
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

        # df_write_to_csv(log_df_4g, r'D:\working\1206_国际财经中心测试V1\场景2\8539\LTE\H1\output\test_demo.csv')
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
    def deal_data_merge_zcy_wifi(in_data_path, name_are):
        # 获取数据文件路径
        ue_file, table_file, zcy_file, wifi_bluetooth_file = DealData.get_data_file_path(in_data_path)
        print('ue_file: ', ue_file)
        print('table_file: ', table_file)
        print('zcy_file: ', zcy_file)
        print('wifi_bluetooth_file: ', wifi_bluetooth_file)
        # 读取ue数据
        ue_df = DealData.deal_ue_table_df(ue_file, table_file)
        # 合并走测仪和wifi数据
        name_scenario = 'Outdoor'
        if check_file_exists(zcy_file):  # 如果存在走测仪数据，则是室内，否则是室外数据
            name_scenario = 'Indoor'  # 有走测仪数据，就合并走测仪和wifi数据
            zcy_wifi_merger_df = DealData.merge_zcy_wifi_data(zcy_file, wifi_bluetooth_file)
            df_data = DealData.merge_ue_zcy_df_data(ue_df, zcy_wifi_merger_df)
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
            df_data['f_msisdn'] = '533F8040D9351F4A9499FC7825805B14'
            print('in_data_path: ', in_data_path)
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
            print('out_file: ', out_file)

            if check_file_exists(table_file):  # 如果有table数据，就是walktour数据，否则就是wetest数据
                if 'LTE' == net_type:
                    untreated_file = os.path.join(in_data_path, '原始_merge_文件.csv')
                    df_write_to_csv(df_data, untreated_file)
                    res_df_data = WalkTour.deal_WalkTour_4g(df_data, net_type)
                    df_write_to_csv(res_df_data, out_file)
                elif 'NR' == net_type:
                    untreated_file = os.path.join(in_data_path, '原始_merge_文件.csv')
                    df_write_to_csv(df_data, untreated_file)
                    nr_res_df = WalkTour.deal_WalkTour_5g(df_data, net_type)
                    df_write_to_csv(nr_res_df, out_file)

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
                    res_df_data = WalkTour.deal_WalkTour_4g(df_data, net_type)
                    df_write_to_csv(res_df_data, out_file)
                    df_write_to_csv(res_df_data, cru_p_out_file)
                elif 'NR' == net_type:
                    untreated_file = os.path.join(in_data_path, 'nr_原始_merge_文件.csv')
                    df_write_to_csv(df_data, untreated_file)
                    nr_res_df = WalkTour.deal_WalkTour_5g(df_data, net_type)
                    df_write_to_csv(nr_res_df, out_file)

    @staticmethod
    def deal_data_only_zcy_outdoor(in_data_path, name_are):
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
        df_data = DealData.merge_ue_zcy_df_data(ue_df, None)

        if not ue_df['Network Type'].empty:
            # 获取网络类型和时间，从df中
            net_type = ue_df['Network Type'][0]
            print('net_type: ', net_type)
            name_d_time = ue_df['PC Time'][0].split(' ')[0]
            name_d_time = name_d_time[name_d_time.find('-'):].replace('-', '')
            print('name_d_time: ', name_d_time)

            name_ue = 'UE'
            # 设置f_msisdn
            df_data['f_msisdn'] = '533F8040D9351F4A9499FC7825805B14'
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
                    res_df_data = WalkTour.deal_WalkTour_4g(df_data, net_type)
                    df_write_to_csv(res_df_data, out_file)
                    df_write_to_csv(res_df_data, cru_p_out_file)
                elif 'NR' == net_type:
                    untreated_file = os.path.join(in_data_path, 'nr_原始_merge_文件.csv')
                    df_write_to_csv(df_data, untreated_file)
                    nr_res_df = WalkTour.deal_WalkTour_5g(df_data, net_type)
                    df_write_to_csv(nr_res_df, out_file)

    @staticmethod
    def deal_multiple_dir_data(in_data_path_list, name_are):
        for i_data_path in in_data_path_list:
            # 获取数据路径
            ue_file, table_file, zcy_file, wifi_bluetooth_file = DealData.get_data_file_path(i_data_path)
            print('ue_file: ', ue_file)
            print('table_file: ', table_file)
            print('zcy_file: ', zcy_file)
            print('wifi_bluetooth_file: ', wifi_bluetooth_file)
            # 读取ue数据
            ue_df = DealData.deal_ue_table_df(ue_file, table_file)
            # # 合并走测仪和wifi数据
            name_scenario = 'Outdoor'
            if zcy_file and check_file_exists(zcy_file):  # 如果存在走测仪数据，则是室内，否则是室外数据
                name_scenario = 'Indoor'  # 有走测仪数据，就合并走测仪和wifi数据
                zcy_wifi_merger_df = DealData.merge_zcy_wifi_data(zcy_file, wifi_bluetooth_file)
                df_data = DealData.merge_ue_zcy_df_data(ue_df, zcy_wifi_merger_df)
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
                print('i_data_path: ', i_data_path)
                if '8539' in i_data_path:
                    f_msisdn = f_msisdn_dict['8539']
                    df_data['f_msisdn'] = f_msisdn
                    print('8539 f_msisdn: ', f_msisdn)
                    name_ue = 'UE1'
                if '2934' in i_data_path:
                    f_msisdn = f_msisdn_dict['2934']
                    df_data['f_msisdn'] = f_msisdn
                    print('2934 f_msisdn: ', f_msisdn)
                    name_ue = 'UE2'

                p_list = split_path_get_list(i_data_path)
                print('p_list: ', p_list)

                file_name = f'{net_type}_{name_are}_{name_scenario}_{name_test_dev}_{name_test_type}_{name_ue}_{name_d_time}_{p_list[-3]}_{p_list[-4]}_{p_list[-2]}_WT_LOG_DT_UE_{p_list[-1]}'
                out_file = os.path.join(tmp_res_out_path, file_name + '.csv')
                print('out_file: ', out_file)

                if check_file_exists(table_file):  # 如果有table数据，就是walktour数据，否则就是wetest数据
                    if 'LTE' == net_type:
                        untreated_file = os.path.join(i_data_path, '原始_merge_文件.csv')
                        df_write_to_csv(df_data, untreated_file)
                        res_df_data = WalkTour.deal_WalkTour_4g(df_data, net_type)
                        df_write_to_csv(res_df_data, out_file)
                    elif 'NR' == net_type:
                        untreated_file = os.path.join(i_data_path, '原始_merge_文件.csv')
                        df_write_to_csv(df_data, untreated_file)
                        nr_res_df = WalkTour.deal_WalkTour_5g(df_data, net_type)
                        df_write_to_csv(nr_res_df, out_file)
