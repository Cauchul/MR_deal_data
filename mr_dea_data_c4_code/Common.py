# -*- coding: utf-8 -*-

import configparser
import math
import os
import shutil
import zipfile
from datetime import datetime

import numpy as np
import pandas as pd
import pytz
from matplotlib import pyplot as plt


def convert_timestamp_to_date(timestamp):
    date_obj = pd.to_datetime(timestamp, unit='s')
    return date_obj.year, date_obj.month, date_obj.day


current_date = datetime.now()
# 格式化为年_月_日形式
formatted_date = current_date.strftime("%Y_%m_%d")


def split_path_get_list(in_path):
    tmp_path_parts = []
    while True:
        in_path, folder = os.path.split(in_path)
        if folder:
            tmp_path_parts.insert(0, folder)
        else:
            break
    return tmp_path_parts


def copy_file(in_src_f, in_targ_f):
    shutil.copy2(in_src_f, in_targ_f)


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


# 读取配置文件
def read_config_file(in_config_path):
    # in_con_file = os.path.join(in_config_path, 'config.ini')
    in_con_file = os.path.join(r'D:\working\merge', 'config.ini')
    config = configparser.ConfigParser()
    # config.read(in_config_file, encoding='UTF-8')
    config.read(in_con_file, encoding='GBK')
    in_lon_O = config.get('Coordinates', 'lon_O')
    in_lat_O = config.get('Coordinates', 'lat_O')
    in_len_east_x = config.get('Coordinates', 'len_east_x')
    in_len_north_y = config.get('Coordinates', 'len_north_y')
    return float(in_lon_O), float(in_lat_O), float(in_len_east_x), float(in_len_north_y)


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


# 获取所有列名
def get_csv_total_columns(csv_file):
    in_df = pd.read_csv(csv_file)
    # 获取所有列名
    tmp_res_columns = in_df.columns
    return tmp_res_columns


def df_write_to_csv(w_df, w_file):
    w_df.to_csv(w_file, index=False)


def get_specified_column_data(in_df, in_columns):
    tmp_res_df = in_df[in_columns]
    return tmp_res_df


# 获取整个目录下zip文件存在的所有的子目录路径，也即数据路径
def get_all_data_path(in_dir, in_char='zip'):
    tmp_data_path_list = []
    for root, dirs, files in os.walk(in_dir):
        for file in files:
            if in_char in file:
                tmp_data_path_list.append(root)
                file_path = os.path.join(root, file)
                print('file_path: ', file_path)

    return tmp_data_path_list


# 传入csv文件list，输出所有文件的df list
def get_csv_list_all_df(in_f_list):
    in_tmp_df_list = []
    for in_i_f in in_f_list:
        print(in_i_f)
        df = pd.read_csv(in_i_f)
        in_tmp_df_list.append(df)
    return in_tmp_df_list


# 解压zip文件
def unzip(in_zip_file, in_out_path):
    with zipfile.ZipFile(in_zip_file, 'r') as zip_ref:
        zip_ref.extractall(in_out_path)


# 遍历当前目录下的所有文件，排除目录
def list_files_in_directory(in_dir):
    tmp_list = []
    for root, dirs, files in os.walk(in_dir):
        for file in files:
            file_path = os.path.join(root, file)
            # 处理文件的逻辑
            tmp_list.append(file_path)
    return tmp_list


# 检查路径，如果不存在则创建
def check_path(in_path):
    if not os.path.exists(in_path):
        # os.mkdir(in_path)
        os.makedirs(in_path, exist_ok=True)


# 删除目录
def clear_path(in_path):
    if os.path.exists(in_path):
        try:
            # os.rmdir(in_path)
            shutil.rmtree(in_path)
            print(f"目录：{in_path}，成功删除")
        except OSError as e:
            print(f"目录：{in_path}，删除失败: {e}")


# 移动文件到指定目录
def move_file(in_src, in_des):
    shutil.move(in_src, in_des)


# 合并两个csv文件
def merge_mult_csv_file(*args):
    tmp_data = pd.concat([pd.read_csv(file).assign(FileName=os.path.basename(file)) for file in args])
    return tmp_data


# 获取当前目录所有子目录
def get_sub_dir(cur_dir):
    for dir_p, dir_s, filenames in os.walk(cur_dir):
        return dir_s


# 获取包含指定字符串的文件
def get_file_by_string(in_str, in_dir):
    file_list = os.listdir(in_dir)
    for file in file_list:
        if in_str.lower() in file.lower():
            in_file_path = os.path.join(in_dir, file)  # 获取包含指定字符串的文件的完整路径
            return in_file_path
    return


# 获取包含指定字符串的文件，区分大小写
def get_file_by_str(in_str, in_dir):
    file_list = os.listdir(in_dir)
    for file in file_list:
        if in_str in file:
            in_file_path = os.path.join(in_dir, file)  # 获取包含指定字符串的文件的完整路径
            return in_file_path
    return


# 获取包含指定字符串的文件
def get_file_by_mult_string(in_dir, *args):
    file_list = os.listdir(in_dir)
    for file in file_list:
        if all(substr in file for substr in args):
            print('here')
            in_file_path = os.path.join(in_dir, file)  # 获取包含指定字符串的文件的完整路径
            return in_file_path
    return


# 获取路径的最后一个文件夹的名称
def get_dir_base_name(in_dir):
    in_folder_name = os.path.basename(in_dir)
    return in_folder_name


# 获取字符串中，第n次出现某个字符或者字符串的index
def find_nth_occurrence(text, target, n):
    start = text.find(target)
    while start >= 0 and n > 1:
        start = text.find(target, start + len(target))
        n -= 1
    return start


def check_file_exists(in_file):
    return os.path.exists(in_file)


# # 获取第n次出现某个字符的位置index
# def find_nth_occurrence(string, target_char, n):
#     start = -1
#     for _ in range(n):
#         start = string.find(target_char, start + 1)
#         if start == -1:
#             break
#     return start


# 通过分隔符截取文件名的前一部分
def extract_file_name(in_file, in_char, in_cnt):
    index = find_nth_occurrence(os.path.basename(in_file), in_char, in_cnt)
    tmp_v_f = os.path.basename(in_file)[:index].replace(in_char, '_') + '_'
    tmp_v_f = tmp_v_f.replace(' ', '_')
    return tmp_v_f


# 数据写入文件中
def data_to_file(in_data, in_file=r'E:\work\data_path.txt'):
    # 打开文件并写入数据
    with open(in_file, 'w') as in_f:
        in_f.write(in_data)


def get_file_data(in_file=r'E:\work\data_path.txt'):
    with open(in_file, 'r') as file:
        res_content = file.read()
        return res_content


# 数据写入文件中
def data_add_to_file(in_data, in_file=r'E:\work\v_h_data_path.txt'):
    # 打开文件并写入数据
    with open(in_file, 'a') as in_f:
        in_f.write(in_data)
        in_f.write('\n')


def get_add_file_data(in_file=r'E:\work\v_h_data_path.txt'):
    with open(in_file, 'r') as file:
        res_content = file.read()
    # 删除文件
    os.remove(in_file)
    return res_content


class DataRead:
    def find_files_with_string(self, path, string):
        for root, dirs, files in os.walk(path):
            for file in files:
                if string in file:
                    return os.path.join(root, file), os.path.basename(file).rsplit(".", 1)[0]
        return False

    # 读取UE数据
    def read_ue_dara(self, in_ue_path):
        # 获取ue数据文件路径和文件名
        in_ue_file, in_ue_f_name = self.find_files_with_string(in_ue_path, 'UE')
        print('in_ue_file: ', in_ue_file)

        # 读取ue数据
        in_ue_data = pd.read_csv(in_ue_file)
        return in_ue_data, in_ue_f_name

    def read_zxy_dara(self, in_zcy_path):
        # 获取zcy数据文件路径和文件名
        in_zcy_file, in_zcy_f_name = self.find_files_with_string(in_zcy_path, 'ZCY')
        print('in_zcy_file: ', in_zcy_file)

        # 读取ue数据
        in_zcy_data = pd.read_csv(in_zcy_file, low_memory=False)
        return in_zcy_data

    def read_imei_dara(self, in_imei_path):
        in_imei_file, in_zcy_f_name = self.find_files_with_string(in_imei_path, 'IMEI')
        print('in_imei_file: ', in_imei_file)

        imei_data = pd.read_csv(in_imei_file, low_memory=False)
        return imei_data


data_read_object = DataRead()


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
