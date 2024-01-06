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


def copy_file(in_src_f, in_targ_f):
    shutil.copy2(in_src_f, in_targ_f)


def check_file_exists(in_file):
    return os.path.exists(in_file)


# 获取包含指定字符串的文件
def get_file_by_string(in_str, in_dir):
    file_list = os.listdir(in_dir)
    for file in file_list:
        if in_str.lower() in file.lower():
            in_file_path = os.path.join(in_dir, file)  # 获取包含指定字符串的文件的完整路径
            return in_file_path
    return


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
