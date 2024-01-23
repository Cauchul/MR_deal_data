# -*- coding: utf-8 -*-
import math

import numpy as np
from geopy.distance import geodesic
from pyproj import Transformer


class CommonFunc:
    tf_for_gps_to_utm = Transformer.from_crs(4326, 32750)
    tf_for_utm_to_gps = Transformer.from_crs(32750, 4326)

    @staticmethod
    def transform_gps_to_utm(longitude, latitude):
        utm_x, utm_y = CommonFunc.tf_for_gps_to_utm.transform(latitude, longitude)
        return utm_x, utm_y

    @staticmethod
    def transform_utm_to_gps(x, y):
        longitude, latitude = CommonFunc.tf_for_utm_to_gps.transform(x, y)
        return longitude, latitude

    @staticmethod
    def transform_x_y_list_to_gps(zero_point_lon, zero_point_lat, x_list, y_list):
        """
        转换xy坐标到经纬度
        :param:
            zero_point_lon : 原点经度
            zero_point_lat : 原点维度
            x_list : 待转换的x坐标列表
            y_list : 待转换的y坐标列表
        :return:
            lon_list: 经度
            lat_list: 纬度
        """
        zero_point_utm_x, zero_point_utm_y = CommonFunc.transform_gps_to_utm(zero_point_lon, zero_point_lat)
        cov_len = len(x_list)
        x_list_utm = np.zeros(cov_len, dtype=np.float64)
        y_list_utm = np.zeros(cov_len, dtype=np.float64)
        for i in range(cov_len):
            x_list_utm[i] = zero_point_utm_x + x_list[i]
            y_list_utm[i] = zero_point_utm_y + y_list[i]
        lon_list = np.zeros(cov_len, dtype=np.float64)
        lat_list = np.zeros(cov_len, dtype=np.float64)
        for i in range(cov_len):
            lon_list[i], lat_list[i] = CommonFunc.transform_utm_to_gps(x_list_utm[i], y_list_utm[i])
        return lon_list, lat_list


if __name__ == '__main__':
    lon_lat_list = [[116.30379346, 39.93350915], [116.30379238, 39.9335078]]
    x1, y1 = CommonFunc.transform_gps_to_utm(lon_lat_list[0][0], lon_lat_list[0][1])
    print(f'x1: {x1} y1: {y1}')

    x2, y2 = CommonFunc.transform_gps_to_utm(lon_lat_list[1][0], lon_lat_list[1][1])
    print(f'x2: {x2} y2: {y2}')
    # distance = math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)
    # print(distance)
    distance = math.sqrt(pow(x2 - x1, 2) + pow(y2 - y1, 2))
    print(distance)

    lon_lat_list = [[39.93350915, 116.30379346], [39.9335078,  116.30379238]]
    distance = geodesic(lon_lat_list[0], lon_lat_list[1]).meters  # 距离的单位为米

    # 0.17597853658185758
    print("距离:", distance, "米")

    def calculate_distance(in_x1, in_y1, in_x2, in_y2):
        in_res_distance = math.sqrt(pow(in_x2 - in_x1, 2) + pow(in_y2 - in_y1, 2))
        print(in_res_distance)