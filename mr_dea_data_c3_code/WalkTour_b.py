# -*- coding: utf-8 -*-
import os.path

from Common import *
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


# def set_scene_data(log_df):
#     # 设置场景信息
#     log_df['f_device_brand'] = f_device_brand
#     log_df['f_device_model'] = f_device_model
#     log_df['f_area'] = f_area
#     log_df['f_floor'] = f_floor
#     log_df['f_scenario'] = f_scenario
#     log_df['f_province'] = "北京"
#     log_df['f_city'] = "北京"
#     log_df['f_district'] = f_district
#     log_df['f_street'] = f_street
#     log_df['f_building'] = f_building
#     log_df['f_prru_id'] = 0
#     log_df['f_source'] = f_source
#     log_df['f_msisdn'] = f_msisdn
#     return log_df


f_msisdn_dict = {'2934': '533F8040D9351F4A9499FC7825805B14',
                 '8539': '7314E1BE6DF72134E285D6AC1A99D8B7'}

if __name__ == '__main__':
    f_device_brand = 'HUAWEI'
    f_device_model = "P40"
    f_area = '国际财经中心室外道路'
    f_floor = '1F'
    f_scenario = 1
    f_district = '海淀区'
    f_street = '西三环北路玲珑路南蓝靛厂南路北洼西街'
    f_building = '国际财经中心'
    f_source = '测试log'  # 测试log；2：MR软采；3：扫频仪；4：WIFI；5：OTT；6：蓝牙;7.WeTest_Log

    all_data_path = r'D:\working\1206_国际财经中心测试V1\场景2\8539'
    data_path_list = get_all_data_path(all_data_path)

    for data_path in data_path_list:
        # 数据路径
        # data_path = get_file_data()
        # data_path = r'E:\work\mr_dea_data_c2\test_data\12月4号\C1\LTE\2934'
        print('data_path: ', data_path)
        # 输出路径
        out_path = os.path.join(data_path, 'output')
        check_path(out_path)

        # 获取数据路径和名称
        ue_file = get_file_by_string('UE', data_path)
        table_file = get_file_by_string('table', data_path)
        zcy_file = get_file_by_string('ZCY', data_path)

        print('ue_file: ', ue_file)
        print('table_file: ', table_file)
        print('zcy_file: ', zcy_file)

        tmp_bath = os.path.dirname(ue_file).split("\\")[-2]
        f_msisdn = f_msisdn_dict['8539']
        print('f_msisdn: ', f_msisdn)

        # 通过文件名imei，查找对应的f_msisdn
        imei_v = ue_file.split('-')[4].split('_')[0]
        print('imei_v: ', imei_v)

        # 根据输入文件生成输出文件
        index = find_nth_occurrence(os.path.basename(zcy_file), '_', 4)
        tmp_v_f = os.path.basename(zcy_file)[:index].replace('-', '_') + '_'
        tmp_v_f = tmp_v_f.replace(' ', '_')
        feature_str = get_dir_base_name(data_path)
        print('feature_str: ', feature_str)
        print('tmp_v_f: ', tmp_v_f)
        # file_name = 'Merge_{}_WT_LOG_DT_{}_UE_{}'.format(tmp_v_f, formatted_date, feature_str)
        file_name = 'Merge_{}_WT_LOG_DT_UE_{}'.format(tmp_v_f, feature_str)
        out_file = os.path.join(out_path, file_name + '.csv')
        print('out_file: ', out_file)

        try:
            if 'LTE' in data_path:
                WalkTour.Indoor.deal_LTE()
            elif 'NR' in data_path:
                WalkTour.Indoor.deal_NR()
        except ValueError:
            with open(r'D:\working\merge\log_error.txt', 'a', encoding='utf-8') as e_file:
                e_file.write(ue_file)
                e_file.write('\n')

    # WalkTour.Outdoor.deal_LTE()
        # WalkTour.Outdoor.deal_NR()
