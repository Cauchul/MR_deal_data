# -*- coding: utf-8 -*-
from Common import read_csv_get_df, df_write_to_csv


# 像素位置转米坐标
def data_conversion(in_value, df_data):
    delta_d = df_data.max() - df_data.min()
    t_x = in_value / delta_d
    n_values = df_data * t_x
    res_dat = n_values - n_values.min()
    return res_dat


# 初始值
lon_o = 116.30297229
lat_o = 39.93413488
len_east_x = 30
len_north_y = 30

data_file = r'E:\work\MR_Data\clean\1_财经中心_2_5G_20240118-0959采样点数据-chart.csv'

char_df = read_csv_get_df(data_file)

# 像素坐标转xy坐标
res_x = data_conversion(len_east_x, char_df['x'])
res_y = data_conversion(len_north_y, char_df['y'])

char_df['f_x'] = res_x
char_df['f_y'] = res_y

# 处理完的数据，写回csv文件
df_write_to_csv(char_df, data_file)

# filtered_df = res_df.loc[res_df['x'] > 25]
# filtered_df = res_df.loc[res_df['y'] > 25]

# filtered_df = char_df.loc[char_df['x'].between(491, 602)]
# df_write_to_csv(filtered_df, r'E:\work\MR_Data\clean\demo.csv')

# 数据写回源文件
# df_write_to_csv(res_df, data_file)
