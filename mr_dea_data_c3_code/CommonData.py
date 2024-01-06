# -*- coding: utf-8 -*-


f_province = "北京"
f_city = "北京"
f_prru_id = 0
f_device_brand = 'HUAWEI'
f_device_model = "P40"
f_area = '国际财经中心'
f_floor = '1F'
f_scenario = 1
f_district = '海淀区'
f_street = '西三环北路玲珑路南蓝靛厂南路北洼西街'
f_building = '国际财经中心'
f_source = '测试log'  # 测试log；2：MR软采；3：扫频仪；4：WIFI；5：OTT；6：蓝牙;7.WeTest_Log


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
#
#     return log_df

def set_scene_data(log_df):
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


class TableFormat:
    WalkTour4G = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
                  'f_building', 'f_floor', 'f_area', 'f_prru_id', 'f_scenario',
                  'f_roaming_type', 'f_imsi', 'f_imei', 'f_msisdn', 'f_cell_id',
                  'f_gnb_id', 'f_time', 'f_longitude', 'f_latitude', 'f_altitude',
                  'f_phr', 'f_enb_received_power', 'f_ta', 'f_aoa', 'f_pci',
                  'f_freq', 'f_rsrp', 'f_rsrq', 'f_sinr', 'f_neighbor_cell_number', 'f_freq_n1',
                  'f_pci_n1', 'f_freq_n2', 'f_pci_n2', 'f_freq_n3', 'f_pci_n3',
                  'f_freq_n4', 'f_pci_n4', 'f_freq_n5', 'f_pci_n5', 'f_freq_n6',
                  'f_pci_n6', 'f_freq_n7', 'f_pci_n7', 'f_freq_n8', 'f_pci_n8',
                  'f_rsrp_n1', 'f_rsrq_n1', 'f_sinr_n1', 'f_rsrp_n2', 'f_rsrq_n2', 'f_sinr_n2', 'f_rsrp_n3',
                  'f_rsrq_n3', 'f_sinr_n3', 'f_rsrp_n4', 'f_rsrq_n4', 'f_sinr_n4', 'f_rsrp_n5', 'f_rsrq_n5',
                  'f_sinr_n5',
                  'f_rsrp_n6', 'f_rsrq_n6', 'f_sinr_n6', 'f_rsrp_n7', 'f_rsrq_n7', 'f_sinr_n7', 'f_rsrp_n8',
                  'f_rsrq_n8', 'f_sinr_n8', 'f_year', 'f_month', 'f_day', 'pc_time', 'f_x', 'f_y',
                  'f_sid', 'f_pid', 'f_direction', 'f_source', 'f_device_brand',
                  'f_device_model'] + ['f_wifi_name_1', 'f_wifi_mac_1', 'f_wifi_rssi_1',
                                       'f_wifi_freq_1', 'f_wifi_name_2', 'f_wifi_mac_2', 'f_wifi_rssi_2',
                                       'f_wifi_freq_2', 'f_wifi_name_3', 'f_wifi_mac_3', 'f_wifi_rssi_3',
                                       'f_wifi_freq_3', 'f_wifi_name_4',
                                       'f_wifi_mac_4', 'f_wifi_rssi_4', 'f_wifi_freq_4', 'f_wifi_name_5',
                                       'f_wifi_mac_5', 'f_wifi_rssi_5', 'f_wifi_freq_5', 'f_wifi_name_6',
                                       'f_wifi_mac_6', 'f_wifi_rssi_6', 'f_wifi_freq_6', 'f_wifi_name_7',
                                       'f_wifi_mac_7', 'f_wifi_rssi_7', 'f_wifi_freq_7', 'f_wifi_name_8',
                                       'f_wifi_mac_8', 'f_wifi_rssi_8', 'f_wifi_freq_8', 'f_bluetooth_mac_1',
                                       'f_bluetooth_uuid_1', 'f_bluetooth_rssi_1', 'f_bluetooth_mac_2',
                                       'f_bluetooth_uuid_2', 'f_bluetooth_rssi_2', 'f_bluetooth_mac_3',
                                       'f_bluetooth_uuid_3', 'f_bluetooth_rssi_3', 'f_bluetooth_mac_4',
                                       'f_bluetooth_uuid_4', 'f_bluetooth_rssi_4', 'f_bluetooth_mac_5',
                                       'f_bluetooth_uuid_5', 'f_bluetooth_rssi_5', 'f_bluetooth_mac_6',
                                       'f_bluetooth_uuid_6', 'f_bluetooth_rssi_6', 'f_bluetooth_mac_7',
                                       'f_bluetooth_uuid_7', 'f_bluetooth_rssi_7', 'f_bluetooth_mac_8',
                                       'f_bluetooth_uuid_8', 'f_bluetooth_rssi_8', ]

    WalkTour5G = ['finger_id', 'f_province', 'f_city', 'f_district', 'f_street',
                  'f_building', 'f_floor', 'f_area', 'f_prru_id', 'f_scenario',
                  'f_roaming_type', 'f_imsi', 'f_imei', 'f_msisdn', 'f_cell_id',
                  'f_gnb_id', 'f_time', 'f_longitude', 'f_latitude', 'f_altitude',
                  'f_phr', 'f_enb_received_power', 'f_ta', 'f_aoa', 'f_pci',
                  'f_freq', 'f_rsrp', 'f_rsrq', 'f_sinr', 'f_neighbor_cell_number', 'f_freq_n1',
                  'f_pci_n1', 'f_freq_n2', 'f_pci_n2', 'f_freq_n3', 'f_pci_n3',
                  'f_freq_n4', 'f_pci_n4', 'f_freq_n5', 'f_pci_n5', 'f_freq_n6',
                  'f_pci_n6', 'f_freq_n7', 'f_pci_n7', 'f_freq_n8', 'f_pci_n8',
                  'f_rsrp_n1', 'f_rsrq_n1', 'f_sinr_n1', 'f_rsrp_n2', 'f_rsrq_n2', 'f_sinr_n2', 'f_rsrp_n3',
                  'f_rsrq_n3', 'f_sinr_n3', 'f_rsrp_n4', 'f_rsrq_n4', 'f_sinr_n4', 'f_rsrp_n5', 'f_rsrq_n5',
                  'f_sinr_n5',
                  'f_rsrp_n6', 'f_rsrq_n6', 'f_sinr_n6', 'f_rsrp_n7', 'f_rsrq_n7', 'f_sinr_n7', 'f_rsrp_n8',
                  'f_rsrq_n8', 'f_sinr_n8', 'f_year', 'f_month', 'f_day', 'pc_time', 'f_x', 'f_y',
                  'f_sid', 'f_pid', 'f_direction', 'f_source', 'f_device_brand',
                  'f_device_model'] + ['f_wifi_name_1', 'f_wifi_mac_1', 'f_wifi_rssi_1',
                                       'f_wifi_freq_1', 'f_wifi_name_2', 'f_wifi_mac_2', 'f_wifi_rssi_2',
                                       'f_wifi_freq_2', 'f_wifi_name_3', 'f_wifi_mac_3', 'f_wifi_rssi_3',
                                       'f_wifi_freq_3', 'f_wifi_name_4',
                                       'f_wifi_mac_4', 'f_wifi_rssi_4', 'f_wifi_freq_4', 'f_wifi_name_5',
                                       'f_wifi_mac_5', 'f_wifi_rssi_5', 'f_wifi_freq_5', 'f_wifi_name_6',
                                       'f_wifi_mac_6', 'f_wifi_rssi_6', 'f_wifi_freq_6', 'f_wifi_name_7',
                                       'f_wifi_mac_7', 'f_wifi_rssi_7', 'f_wifi_freq_7', 'f_wifi_name_8',
                                       'f_wifi_mac_8', 'f_wifi_rssi_8', 'f_wifi_freq_8', 'f_bluetooth_mac_1',
                                       'f_bluetooth_uuid_1', 'f_bluetooth_rssi_1', 'f_bluetooth_mac_2',
                                       'f_bluetooth_uuid_2', 'f_bluetooth_rssi_2', 'f_bluetooth_mac_3',
                                       'f_bluetooth_uuid_3', 'f_bluetooth_rssi_3', 'f_bluetooth_mac_4',
                                       'f_bluetooth_uuid_4', 'f_bluetooth_rssi_4', 'f_bluetooth_mac_5',
                                       'f_bluetooth_uuid_5', 'f_bluetooth_rssi_5', 'f_bluetooth_mac_6',
                                       'f_bluetooth_uuid_6', 'f_bluetooth_rssi_6', 'f_bluetooth_mac_7',
                                       'f_bluetooth_uuid_7', 'f_bluetooth_rssi_7', 'f_bluetooth_mac_8',
                                       'f_bluetooth_uuid_8', 'f_bluetooth_rssi_8', ]


WalkTour_table_format_dict = {'LTE': TableFormat.WalkTour4G, 'NR': TableFormat.WalkTour5G}
