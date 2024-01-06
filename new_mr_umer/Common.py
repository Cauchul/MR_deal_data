# -*- coding: utf-8 -*-
import configparser
import math
import os
import shutil
import time
import zipfile

import numpy as np
import pandas as pd
import pytz
from matplotlib import pyplot as plt

from DataPreprocessing import DataPreprocessing
from GlobalConfig import tmp_res_out_path, f_msisdn_dict


def unzip(in_zip_file, in_out_path):
    with zipfile.ZipFile(in_zip_file, 'r') as zip_ref:
        zip_ref.extractall(in_out_path)


# 删除目录
def clear_path(in_path):
    if os.path.exists(in_path):
        try:
            # os.rmdir(in_path)
            shutil.rmtree(in_path)
            print(f"目录：{in_path}，成功删除")
        except OSError as e:
            print(f"目录：{in_path}，删除失败: {e}")


# 获取包含指定字符串的文件
def get_file_by_string(in_str, in_dir):
    file_list = os.listdir(in_dir)
    for file in file_list:
        if in_str.lower() in file.lower():
            in_file_path = os.path.join(in_dir, file)  # 获取包含指定字符串的文件的完整路径
            return in_file_path
    return


# 获取整个目录下zip文件存在的所有的子目录路径，也即数据路径
def get_all_data_path(in_dir, in_char='zip'):
    tmp_data_path_list = []
    for root, dirs, files in os.walk(in_dir):
        # if root != in_dir:
        for file in files:
            if in_char in file:
                tmp_data_path_list.append(root)
                file_path = os.path.join(root, file)
                print('file_path: ', file_path)

    return tmp_data_path_list


# 生成输出文件名称
def generate_output_file_name(in_data_path, in_df, in_net_type, in_n_scene, in_test_type):
    tmp_cur_out_path = os.path.join(in_data_path, 'output')
    check_path(tmp_cur_out_path)

    tmp_split_list = split_path_get_list(in_data_path)

    name_d_time = in_df['pc_time'][0].split(' ')[0]
    if '-' in name_d_time:
        name_d_time = name_d_time[name_d_time.find('-'):].replace('-', '')
    else:
        name_d_time = name_d_time[name_d_time.find('/'):].replace('/', '')
    print('name_d_time: ', name_d_time)

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


def delete_last_character(in_str, in_chart='_'):
    tmp_res_str = in_str.rsplit(in_chart, 1)[0]
    return tmp_res_str


def get_last_character(in_str, in_chart='_'):
    return in_str.rsplit(in_chart, 1)[1]


# 通过字典值查询key
def get_dict_key_by_value(in_dict, in_value):
    for tmp_key, tmp_value in in_dict.items():
        if tmp_value == in_value:
            # print(key)
            return tmp_key
    return


# 生成标准文件
def standard_output_name(in_path, in_net_type, in_name_ue, in_name_d_time):
    tmp_cur_out_path = os.path.join(in_path, 'output')
    check_path(tmp_cur_out_path)

    p_list = split_path_get_list(in_path)
    print('p_list: ', p_list)

    n_scenario = 'Indoor'
    n_area = 'DaXin'
    n_test_type = 'DT'

    file_name = f'{in_net_type}_{n_area}_{n_scenario}_WT_{n_test_type}_{in_name_ue}_{in_name_d_time}_{p_list[-3]}_{p_list[-4]}_{p_list[-2]}_LOG_UE_{p_list[-1]}'
    tmp_out_file = os.path.join(tmp_res_out_path, file_name + '.csv')
    tmp_cur_p_out_file = os.path.join(tmp_cur_out_path, file_name + '.csv')
    print('tmp_out_file: ', tmp_out_file)
    return tmp_out_file, tmp_cur_p_out_file


# 获取zcy数据
def get_zcy_data(in_zcy_file):
    in_zcy_df = read_csv_get_df(in_zcy_file)
    # tmp_zcy_df = in_zcy_df[
    #     ['test_time', 'created_by_ue_time', 'f_x', 'f_y', 'f_longitude', 'f_latitude', 'direction', 'altitude']]
    tmp_zcy_df = in_zcy_df[
        ['test_time', 'created_by_ue_time', 'f_x', 'f_y', 'f_longitude', 'f_latitude', 'altitude']]
    return tmp_zcy_df


# 获取wifi数据
def get_wifi_bluetooth_data(in_wifi_bluetooth_file):
    in_wifi_df = read_csv_get_df(in_wifi_bluetooth_file)
    tmp_wifi_df = in_wifi_df.drop(['f_x', 'f_y', 'f_longitude', 'f_latitude', 'f_direction', 'f_altitude'], axis=1)
    return tmp_wifi_df


# 合并UE table数据
def deal_ue_table_df(in_ue_file, in_table_file):
    in_ue_df = read_csv_get_df(in_ue_file)
    if os.path.exists(in_table_file):
        in_table_df = read_csv_get_df(in_table_file)
        res_tmp_merge_df = pd.merge(in_ue_df, in_table_df, left_on="PC Time", right_on="PCTime", how='left')
        return res_tmp_merge_df
    else:
        return in_ue_df


# # 合并ue和zcy
# def merge_ue_zcy_df(in_ue_df, in_zcy_df):
#     in_ue_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_ue_df['PC Time'])
#     if not in_zcy_df.empty:
#         in_zcy_df['ts'] = DataPreprocessing.convert_datetime_to_timestamp(in_zcy_df['test_time'])
#         tmp_df = pd.merge(in_ue_df, in_zcy_df)
#         return tmp_df
#     else:
#         return in_ue_df


# 根据特征获取文件字典
def get_file_dict(in_path, in_file_list):
    tmp_file_dict = {}
    for i_char in in_file_list:
        in_res_file = get_file_by_str(i_char, in_path)
        tmp_file_dict[i_char] = in_res_file
    return tmp_file_dict


# 获取包含指定字符串的文件，区分大小写
def get_file_by_str(in_str, in_dir):
    file_list = os.listdir(in_dir)
    for file in file_list:
        if in_str in file:
            in_file_path = os.path.join(in_dir, file)  # 获取包含指定字符串的文件的完整路径
            return in_file_path
    return


def copy_file(in_src_f, in_targ_f):
    shutil.copy2(in_src_f, in_targ_f)


def check_file_exists(in_file):
    return os.path.exists(in_file)


# # 获取包含指定字符串的文件
# def get_file_by_string(in_str, in_dir):
#     file_list = os.listdir(in_dir)
#     for file in file_list:
#         if in_str.lower() in file.lower():
#             in_file_path = os.path.join(in_dir, file)  # 获取包含指定字符串的文件的完整路径
#             return in_file_path
#     return


def read_csv_get_df(in_df_path):
    in_df = pd.read_csv(in_df_path, low_memory=False)
    return in_df


# 走测仪数据转经纬度
def data_conversion(in_value, df_data):
    delta_d = df_data.max() - df_data.min()
    t_x = in_value / delta_d
    n_values = df_data * t_x
    res_dat = n_values - n_values.min()
    return res_dat


def df_write_to_csv(w_df, w_file):
    w_df.to_csv(w_file, index=False)


def clear_merge_path(in_path):
    if os.path.exists(in_path):
        shutil.rmtree(in_path)
        time.sleep(1)


# 获取字符串中，第n次出现某个字符或者字符串的index
def find_nth_occurrence(text, target, n):
    start = text.find(target)
    while start >= 0 and n > 1:
        start = text.find(target, start + len(target))
        n -= 1
    return start


# 遍历当前目录下的所有文件，排除目录
def list_files_in_directory(in_dir):
    tmp_list = []
    for root, dirs, files in os.walk(in_dir):
        if in_dir == root:
            for file in files:
                file_path = os.path.join(root, file)
                # 处理文件的逻辑
                tmp_list.append(file_path)
    return tmp_list


# 获取所有列名
def get_csv_total_columns(csv_file):
    in_df = pd.read_csv(csv_file)
    # 获取所有列名
    tmp_res_columns = in_df.columns
    return tmp_res_columns


# 合并csv文件
def merge_csv_file(*args):
    tmp_data = pd.concat([pd.read_csv(file).assign(FileName=os.path.basename(file)) for file in args])
    return tmp_data


def generate_images(in_x1_v, in_y1_v, in_lon, in_lat, in_out_path, in_image_name):
    plt.clf()

    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['font.sans-serif'] = 'SimHei'
    # Mac系统字体
    # plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
    plt.subplot(2, 1, 1)
    plt.plot(in_x1_v, in_y1_v)
    plt.gca().set_aspect('equal', adjustable='box')
    plt.xlabel('X')
    plt.ylabel('Y')
    plt.title('xy_相对位置轨迹图')
    png_file = os.path.join(in_out_path, f'xy_相对位置轨迹图{in_image_name}.png')
    plt.savefig(png_file)

    # 清除图形
    plt.clf()

    # plt.savefig(png_file)
    plt.subplot(2, 1, 2)
    plt.plot(in_lon, in_lat)
    plt.gca().set_aspect('equal', adjustable='box')
    plt.xlabel('lon')
    plt.ylabel('lat')
    plt.title('lonlat_经纬度轨迹图')
    plt.tight_layout()
    # plt.show()
    png_file = os.path.join(in_out_path, f'lonlat_经纬度轨迹图{in_image_name}.png')
    plt.savefig(png_file)


def split_path_get_list(in_path):
    tmp_path_parts = []
    while True:
        in_path, folder = os.path.split(in_path)
        if folder:
            tmp_path_parts.insert(0, folder)
        else:
            break
    return tmp_path_parts


# 检查路径，如果不存在则创建
def check_path(in_path):
    if not os.path.exists(in_path):
        # os.mkdir(in_path)
        os.makedirs(in_path, exist_ok=True)


class DealDf:
    def __int__(self):
        pass

    def delete_duplicate_columns(self, log_df):
        """
        删除重复列
        """
        log_df = log_df.loc[:, ~log_df.columns.duplicated()]
        return log_df

    def delete_second_level_duplicate_data(self, df_dta, g_df):
        """
        # 删除测试log中 秒级重复数据，同秒取第一条。
        """
        df_dta = df_dta.groupby(g_df).first().reset_index()  # 删除测试log中 秒级重复数据，同秒取第一条。
        return df_dta

    def change_to_Shanghai_time_zone(self, deal_df):
        res_df = pd.to_datetime(deal_df, unit='s').dt.tz_localize(pytz.utc).dt.tz_convert(
            pytz.timezone('Asia/Shanghai'))
        return res_df

    def generate_finger_id(self, time_df, msi_df):
        res_id = 'F' + time_df.dt.strftime('%Y%m%d%H') + '_' + msi_df.str[-4:]
        return res_id

    # 获取领区数
    def get_cell_number(self, log_df):
        tmp_num_list = []
        for i in range(len(log_df)):
            tmp_num_list.append(np.count_nonzero(
                log_df[['f_rsrp_n1', 'f_rsrp_n2', 'f_rsrp_n3', 'f_rsrp_n4', 'f_rsrp_n5',
                        'f_rsrp_n6', 'f_rsrp_n7', 'f_rsrp_n8']].isnull().values[i] == False))
        return tmp_num_list

    def add_and_empty_UEMR_data(self, log_df):
        log_df['f_roaming_type'] = ''
        log_df['f_phr'] = ''
        log_df['f_enb_received_power'] = ''
        log_df['f_ta'] = ''
        log_df['f_aoa'] = ''
        return log_df

    def calculate_directions(self, df):
        """
        计算当前位置的方向
        :return: 返回当前数据相对于上一条数据的方向。
        """

        def get_direction(curr_x, curr_y, prev_x, prev_y):
            delta_x = curr_x - prev_x
            delta_y = curr_y - prev_y

            angle = math.atan2(delta_y, delta_x)
            angle_deg = math.degrees(angle)

            if -45 <= angle_deg <= 45:
                return 'N'
            elif 45 < angle_deg <= 135:
                return 'E'
            elif -135 <= angle_deg < -45:
                return 'S'
            else:
                return 'W'

        prev_x = None
        prev_y = None

        directions = []
        for index, row in df.iterrows():
            curr_x = float(row['f_longitude'])
            curr_y = float(row['f_latitude'])

            if prev_x is not None and prev_y is not None:
                direction = get_direction(curr_x, curr_y, prev_x, prev_y)
                directions.append(direction)
            else:
                directions.append('未发生位移')

            prev_x = curr_x
            prev_y = curr_y

        df['f_direction'] = directions
        return df


# 数据读取对象
deal_df_object = DealDf()
