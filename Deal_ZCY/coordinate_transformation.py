# -*- coding: utf-8 -*-
# 坐标纠正
from Common import read_csv_get_df


# 走测仪数据转经纬度
def data_conversion(in_value, df_data):
    delta_d = df_data.max() - df_data.min()  # 最大最小的差值
    t_x = in_value / delta_d
    n_values = df_data * t_x
    res_dat = n_values - n_values.min()  # 去除负值
    return res_dat


# 示例数据点
data_file = r'E:\work\MR_Data\coordinate_transformation\5_财经中心_2_5G_20240118-1527采样点数据-chart.csv'
len_east_x = 30
len_north_y = 30

char_df = read_csv_get_df(data_file)

# 像素坐标转xy坐标
res_x = data_conversion(len_east_x, char_df['x'])
res_y = data_conversion(len_north_y, char_df['y'])

char_df['f_x'] = res_x
char_df['f_y'] = res_y

# 坐标纠正