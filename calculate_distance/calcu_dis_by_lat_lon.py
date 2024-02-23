# -*- coding: utf-8 -*-
from geopy.distance import geodesic


def calculate_distance(in_lat1, in_lon1, in_lat2, in_lon2):
    # 经纬度点1
    point1 = (in_lat1, in_lon1)

    # 经纬度点2
    point2 = (in_lat2, in_lon2)

    # 使用geopy库计算距离
    res_distance = geodesic(point1, point2).meters
    return res_distance


if __name__ == '__main__':
    # 举例：计算两个经纬度点之间的距离
    lon1, lat1 = 116.3029793, 39.93414134
    lon2, lat2 = 116.3029797, 39.93415095

    # 输入经纬度，计算两个点之间的距离，米
    distance = calculate_distance(lat1, lon1, lat2, lon2)
    print(f"距离: {distance:.2f} 米")
