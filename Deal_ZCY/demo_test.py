# -*- coding: utf-8 -*-
import math


def calculate_angle(slope):
    if slope > 0:
        return math.atan(slope)
    elif slope < 0:
        return math.atan(slope) + math.pi
    else:
        return math.pi / 2


# 示例：直线与 x 轴的夹角
slope = 1.5  # 假设直线的斜率为 1.5
angle = calculate_angle(slope)
print(math.degrees(angle))  # 将弧度转换为角度并打印

# 示例：直线与 y 轴的夹角
slope = -0.8  # 假设直线的斜率为 -0.8
angle = calculate_angle(1 / slope)
print(math.degrees(angle))  # 将弧度转换为角度并打印
