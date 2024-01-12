# -*- coding: utf-8 -*-

import math


def calculate_coordinate(in_origin_lon, in_origin_lat, in_x_distance, in_y_distance):
    origin_lon_rad = math.radians(in_origin_lon)  # 经度转成弧度
    origin_lat_rad = math.radians(in_origin_lat)  # 维度转成弧度
    earth_radius = 6371.0  # 地球半径

    lat_offset = in_y_distance / earth_radius
    in_new_lat = origin_lat_rad + lat_offset

    lon_offset = in_x_distance / (earth_radius * math.cos(origin_lat_rad))
    in_new_lon = origin_lon_rad + lon_offset

    new_lon_deg = math.degrees(in_new_lon)
    new_lat_deg = math.degrees(in_new_lat)

    return new_lon_deg, new_lat_deg


origin_lon = -74.0060
origin_lat = 40.7128
x_distance = 10
y_distance = -5

new_lon, new_lat = calculate_coordinate(origin_lon, origin_lat, x_distance, y_distance)
print(f"New Longitude: {new_lon}")
print(f"New Latitude: {new_lat}")