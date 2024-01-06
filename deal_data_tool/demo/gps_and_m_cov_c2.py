# -*- coding: utf-8 -*-
import pyproj
import pandas as pd

# # 创建转换器对象，从GPS经纬度坐标转换为UTM坐标
# tf_for_gps_to_utm = pyproj.Transformer.from_crs(4326, 32750)
#
# # 创建转换器对象，从UTM坐标转换为GPS经纬度坐标
# tf_for_utm_to_gps = pyproj.Transformer.from_crs(32750, 4326)
#
# # 示例：将经纬度坐标转换为XY值坐标
# lon = 39.93413488
# lat = 116.30297229
# x, y = tf_for_gps_to_utm.transform(lon, lat)
# print("XY Coordinates: ", x, y)
#
# # 示例：将XY值坐标转换为经纬度坐标
# lon, lat = tf_for_utm_to_gps.transform(x, y)
# print("Lon Lat Coordinates: ", lon, lat)

# 指定CSV文件路径
csv_file_path = r'D:\working\data_conv\src_data\finger_zte_4g_huawei.csv'

# 读取CSV文件为DataFrame对象
df = pd.read_csv(csv_file_path)

# 遍历每一行数据
for index, row in df.iterrows():
    # 在这里对每一行数据进行处理
    # 例如，打印每一行数据
    # print(row)
    print(row['f_x'])
    print(row['f_y'])
    f_x
    f_y
    print(df[index:])
    break
