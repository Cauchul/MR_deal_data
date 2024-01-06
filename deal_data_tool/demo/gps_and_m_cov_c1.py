# -*- coding: utf-8 -*-
import pyproj

# 创建转换器对象，从GPS经纬度坐标转换为UTM坐标
tf_for_gps_to_utm = pyproj.Transformer.from_crs(4326, 32750)

# 创建转换器对象，从UTM坐标转换为GPS经纬度坐标
tf_for_utm_to_gps = pyproj.Transformer.from_crs(32750, 4326)

# 示例：将经纬度坐标转换为XY值坐标
lon = 39.93413488
lat = 116.30297229
x, y = tf_for_gps_to_utm.transform(lon, lat)
print("XY Coordinates: ", x, y)

# 示例：将XY值坐标转换为经纬度坐标
lon, lat = tf_for_utm_to_gps.transform(x, y)
print("Lon Lat Coordinates: ", lon, lat)
