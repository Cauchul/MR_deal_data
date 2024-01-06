# -*- coding: utf-8 -*-
import os.path

from Common import *
from DataPreprocessing import DataPreprocessing
from DealData import DealData


class WalkTour:
    class Indoor:
        @staticmethod
        def deal_LTE():
            # 读取处理走测仪数据
            zcy_df = read_csv_get_df(zcy_file)
            table_df = read_csv_get_df(table_file)
            in_ue_df = read_csv_get_df(ue_file)

            ue_merge_df = DealData.walktour_merge_ue_imei(in_ue_df, table_df)
            ue_df = DealData.walktour_indoor_merge_ue_zcy(ue_merge_df, zcy_df)
            ue_df = DealData.deal_WalkTour_indoor_4g(ue_df)
            df_write_to_csv(ue_df, out_file)

        @staticmethod
        def deal_NR():
            zcy_df = read_csv_get_df(zcy_file)
            in_ue_df = read_csv_get_df(ue_file)
            table_df = read_csv_get_df(table_file)

            ue_merge_df = DealData.walktour_merge_ue_imei(in_ue_df, table_df)
            in_ue_df = DealData.walktour_indoor_merge_ue_zcy(ue_merge_df, zcy_df)
            in_ue_df = DealData.deal_WalkTour_indoor_5g(in_ue_df, set_scene_data)
            df_write_to_csv(in_ue_df, out_file)

    class Outdoor:
        @staticmethod
        def deal_LTE():
            ue_df = read_csv_get_df(ue_file)
            imei_df = read_csv_get_df(table_file)
            ue_merge_df = DealData.walktour_merge_ue_imei(ue_df, imei_df)
            ue_df = DealData.deal_WalkTour_outdoor_4g(ue_merge_df, f_msisdn, set_scene_data)
            df_write_to_csv(ue_df, out_file)

        @staticmethod
        def deal_NR():
            ue_df = read_csv_get_df(ue_file)
            imei_df = read_csv_get_df(table_file)
            ue_merge_df = DealData.walktour_merge_ue_imei(ue_df, imei_df)
            ue_df = DealData.deal_WalkTour_outdoor_5g(ue_merge_df, f_msisdn, set_scene_data)
            df_write_to_csv(ue_df, out_file)


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


f_msisdn_dict = {'2934': '533F8040D9351F4A9499FC7825805B14',
                 '8539': '7314E1BE6DF72134E285D6AC1A99D8B7'}

file_char_list = ['UE', 'table', 'ZCY', '_WIFI_BlueTooth']
if __name__ == '__main__':
    # all_data_path = r'E:\work\mr_dea_data_c2\test_data\12月4号\G19\LTE\2934v'
    # data_path_list = get_all_data_path(all_data_path)
    #
    # for data_path in data_path_list:
    data_path = r'D:\working\1206_国际财经中心测试V1\场景1\2934\LTE'
    print('data_path: ', data_path)
    # 获取file_list
    ue_file = get_file_by_str('UE', data_path)
    table_file = get_file_by_string('table', data_path)
    zcy_file = get_file_by_string('ZCY', data_path)
    wifi_bluetooth_file = get_file_by_string('WIFI_BlueTooth', data_path)

    zcy_df = get_zcy_data(zcy_file)
    wifi_bluetooth_df = get_wifi_bluetooth_data(wifi_bluetooth_file)

    # 获取zcy，wifi数据
    zcy_wifi_merger_df = pd.merge(wifi_bluetooth_df, zcy_df, left_on="f_time", right_on="created_by_ue_time",
                                  how='left')

    # to_file = os.path.join(data_path, 'aa_test_1207.csv')
    # df_write_to_csv(zcy_wifi_merger_df, to_file)
    # 获取ue和table数据
    print('ue_file: ', ue_file)
    ue_merger_df = deal_ue_table_df(ue_file, table_file)

    ue_merger_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(ue_merger_df['PC Time'])
    zcy_wifi_merger_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(zcy_wifi_merger_df['test_time'])
    df_data = pd.merge(ue_merger_df, zcy_wifi_merger_df)

    res_df_data = DealData.deal_WalkTour_4G(df_data)

    out_file = os.path.join(data_path, 'demo_test_1207.csv')
    df_write_to_csv(res_df_data, out_file)

    # df_list = get_csv_list_all_df(data_file_list)
    # merged_df = pd.concat(df_list, ignore_index=True)
    # before = os.path.join(data_path, 'AA_1207.csv')
    # df_write_to_csv(merged_df, before)
    # res_df = DealData.deal_WalkTour_demo(merged_df)
    # out_file = os.path.join(data_path, 'demo_test_1207.csv')
    # df_write_to_csv(res_df, out_file)

    # 合并所有 UE，zcy，wifi，table

    # # 数据路径
    # print('data_path: ', data_path)
    # # 输出路径
    # out_path = os.path.join(data_path, 'output')
    # check_path(out_path)
    #
    # # 获取数据路径和名称
    # ue_file = get_file_by_string('UE', data_path)
    # table_file = get_file_by_string('table', data_path)
    # zcy_file = get_file_by_string('ZCY', data_path)
    #
    # # 处理ue
    #
    # print('ue_file: ', ue_file)
    # print('table_file: ', table_file)
    # print('zcy_file: ', zcy_file)
    #
    # tmp_bath = os.path.dirname(ue_file).split("\\")[-2]
    # f_msisdn = f_msisdn_dict['8539']
    # print('f_msisdn: ', f_msisdn)
    #
    # # 通过文件名imei，查找对应的f_msisdn
    # imei_v = ue_file.split('-')[4].split('_')[0]
    # print('imei_v: ', imei_v)
    #
    # # 根据输入文件生成输出文件
    # index = find_nth_occurrence(os.path.basename(zcy_file), '_', 4)
    # tmp_v_f = os.path.basename(zcy_file)[:index].replace('-', '_') + '_'
    # tmp_v_f = tmp_v_f.replace(' ', '_')
    # feature_str = get_dir_base_name(data_path)
    # print('feature_str: ', feature_str)
    # print('tmp_v_f: ', tmp_v_f)
    #
    # file_name = 'Merge_{}_WT_LOG_DT_UE_{}'.format(tmp_v_f, feature_str)
    # out_file = os.path.join(out_path, file_name + '.csv')
    # print('out_file: ', out_file)
    #
    # try:
    #     if 'LTE' in data_path:
    #         WalkTour.Indoor.deal_LTE()
    #     elif 'NR' in data_path:
    #         WalkTour.Indoor.deal_NR()
    # except ValueError:
    #     with open(r'D:\working\merge\log_error.txt', 'a', encoding='utf-8') as e_file:
    #         e_file.write(ue_file)
    #         e_file.write('\n')
