# -*- coding: utf-8 -*-
import os.path

from Common import *
from DataPreprocessing import DataPreprocessing
from DealData import DealData
from GlobalConfig import *
from deal_zcy_data import unzip_and_deal_zcy


def get_zcy_data(in_zcy_file):
    in_zcy_df = read_csv_get_df(in_zcy_file)
    tmp_zcy_df = in_zcy_df[
        ['test_time', 'created_by_ue_time', 'f_x', 'f_y', 'f_longitude', 'f_latitude', 'direction', 'altitude']]
    return tmp_zcy_df


def deal_ue_table_df(in_ue_file, in_table_file):
    in_ue_df = read_csv_get_df(in_ue_file)
    if os.path.exists(in_table_file):
        in_table_df = read_csv_get_df(in_table_file)
        res_tmp_merge_df = pd.merge(in_ue_df, in_table_df, left_on="PC Time", right_on="PCTime", how='left')
        return res_tmp_merge_df
    else:
        return in_ue_df


def get_wifi_bluetooth_data(in_wifi_bluetooth_file):
    in_wifi_df = read_csv_get_df(in_wifi_bluetooth_file)
    tmp_wifi_df = in_wifi_df.drop(['f_x', 'f_y', 'f_longitude', 'f_latitude', 'f_direction', 'f_altitude'], axis=1)
    return tmp_wifi_df


def zcy_wifi_bluetooth(in_zcy_df, in_wifi_bluetooth_file):
    in_wifi_df = read_csv_get_df(in_wifi_bluetooth_file)
    res_tmp_merge_df = pd.merge(in_wifi_df, in_zcy_df)
    return res_tmp_merge_df


def get_merge_zcy_data(in_zcy_file, in_wifi_bluetooth_file):
    # 合并走测仪和wifi数据
    zcy_df = get_zcy_data(in_zcy_file)
    wifi_bluetooth_df = get_wifi_bluetooth_data(in_wifi_bluetooth_file)
    # 获取zcy，wifi数据
    tmp_merger_df = pd.merge(wifi_bluetooth_df, zcy_df, left_on="f_time", right_on="created_by_ue_time",
                             how='left')
    return tmp_merger_df


def merge_ue_zcy_df_data(in_ue_df, in_zcy_df):
    in_ue_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_ue_df['PC Time'])
    in_zcy_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_zcy_df['test_time'])
    tmp_df = pd.merge(in_ue_df, in_zcy_df)
    return tmp_df


def deal_walktour_data(in_data_path_list):
    for i_data_path in in_data_path_list:
        # 获取UE数据
        ue_file = get_file_by_str('UE', i_data_path)
        table_file = get_file_by_string('table', i_data_path)
        zcy_file = get_file_by_string('ZCY', i_data_path)
        wifi_bluetooth_file = get_file_by_string('xyToLonLat_WIFI_BlueTooth', i_data_path)
        print('ue_file: ', ue_file)
        print('table_file: ', table_file)
        print('zcy_file: ', zcy_file)
        print('wifi_bluetooth_file: ', wifi_bluetooth_file)

        # # 合并走测仪和wifi数据
        zcy_wifi_merger_df = get_merge_zcy_data(zcy_file, wifi_bluetooth_file)
        ue_df = deal_ue_table_df(ue_file, table_file)

        if not ue_df['Network Type'].empty:
            net_type = ue_df['Network Type'][0]
            print('net_type: ', net_type)

            df_data = merge_ue_zcy_df_data(ue_df, zcy_wifi_merger_df)

            # 设置f_msisdn
            print('i_data_path: ', i_data_path)
            if '8539' in i_data_path:
                f_msisdn = f_msisdn_dict['8539']
                df_data['f_msisdn'] = f_msisdn
                print('8539 f_msisdn: ', f_msisdn)
            if '2934' in i_data_path:
                f_msisdn = f_msisdn_dict['2934']
                df_data['f_msisdn'] = f_msisdn
                print('2934 f_msisdn: ', f_msisdn)

            feature_str = get_dir_base_name(i_data_path)
            print('feature_str: ', feature_str)
            file_name = f'{net_type}_{name_are}_{name_scenario}_{name_test_dev}_{name_test_type}__UE_1206'
            out_file = os.path.join(tmp_res_out_path, file_name + '.csv')
            print('out_file: ', out_file)

            if 'LTE' == net_type:
                untreated_file = os.path.join(i_data_path, '原始_merge_文件.csv')
                df_write_to_csv(df_data, untreated_file)
                res_df_data = DealData.deal_WalkTour_4g(df_data, net_type)
                df_write_to_csv(res_df_data, out_file)
            elif 'NR' == net_type:
                untreated_file = os.path.join(i_data_path, '原始_merge_文件.csv')
                df_write_to_csv(df_data, untreated_file)
                nr_res_df = DealData.deal_WalkTour_5g(df_data, net_type)
                df_write_to_csv(nr_res_df, out_file)


if __name__ == '__main__':
    pass
    # data_path_list = get_all_data_path(g_data_path)
    # print('当前的数据处理目录为：', g_data_path)

    # deal_walktour_data(data_path_list)

    # for data_path in data_path_list:
    #     # 获取UE数据
    #     ue_file = get_file_by_str('UE', data_path)
    #     table_file = get_file_by_string('table', data_path)
    #     zcy_file = get_file_by_string('ZCY', data_path)
    #     wifi_bluetooth_file = get_file_by_string('xyToLonLat_WIFI_BlueTooth', data_path)
    #     print('ue_file: ', ue_file)
    #     print('table_file: ', table_file)
    #     print('zcy_file: ', zcy_file)
    #     print('wifi_bluetooth_file: ', wifi_bluetooth_file)
    #
    #     # # 合并走测仪和wifi数据
    #     zcy_wifi_merger_df = get_merge_zcy_data()
    #     ue_df = deal_ue_table_df(ue_file, table_file)
    #
    #     if not ue_df['Network Type'].empty:
    #         net_type = ue_df['Network Type'][0]
    #         print('net_type: ', net_type)
    #
    #         df_data = merge_ue_zcy_df_data(ue_df, zcy_wifi_merger_df)
    #
    #         # 设置f_msisdn
    #         print('data_path: ', data_path)
    #         if '8539' in data_path:
    #             f_msisdn = f_msisdn_dict['8539']
    #             df_data['f_msisdn'] = f_msisdn
    #             print('8539 f_msisdn: ', f_msisdn)
    #         if '2934' in data_path:
    #             f_msisdn = f_msisdn_dict['2934']
    #             df_data['f_msisdn'] = f_msisdn
    #             print('2934 f_msisdn: ', f_msisdn)
    #
    #         # 生成输出文件名称
    #         index = find_nth_occurrence(os.path.basename(zcy_file), '_', 4)
    #         tmp_v_f = os.path.basename(zcy_file)[:index].replace('-', '_') + '_'
    #         tmp_v_f = tmp_v_f.replace(' ', '_')
    #         feature_str = get_dir_base_name(data_path)
    #         print('feature_str: ', feature_str)
    #         file_name = 'Merge_{}_WT_LOG_DT_UE_{}'.format(tmp_v_f, feature_str)
    #         out_file = os.path.join(out_path, file_name + '.csv')
    #         print('out_file: ', out_file)
    #
    #         if 'LTE' == net_type:
    #             untreated_file = os.path.join(data_path, '原始_merge_文件.csv')
    #             df_write_to_csv(df_data, untreated_file)
    #             res_df_data = DealData.deal_WalkTour_4g(df_data, net_type)
    #             df_write_to_csv(res_df_data, out_file)
    #         elif 'NR' == net_type:
    #             untreated_file = os.path.join(data_path, '原始_merge_文件.csv')
    #             df_write_to_csv(df_data, untreated_file)
    #             nr_res_df = DealData.deal_WalkTour_5g(df_data, net_type)
    #             df_write_to_csv(nr_res_df, out_file)
