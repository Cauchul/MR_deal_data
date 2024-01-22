# -*- coding: utf-8 -*-
import math

import numpy as np
from scipy.optimize import least_squares

from Common import read_csv_get_df, data_conversion

# 示例数据点
data_file = r'E:\work\MR_Data\clean\1_财经中心_2_5G_20240118-0959采样点数据-chart.csv'
len_east_x = 30
len_north_y = 30

char_df = read_csv_get_df(data_file)

# 像素坐标转xy坐标
res_x = data_conversion(len_east_x, char_df['x'])
res_y = data_conversion(len_north_y, char_df['y'])

char_df['f_x'] = res_x
char_df['f_y'] = res_y


# 定义拟合函数
def line_func(params, x):
    a, b = params
    return a * x + b


# 定义误差函数
def error_func(params, x, y):
    return line_func(params, x) - y


def calculate_angle(slope):
    if slope > 0:
        return math.atan(slope)
    elif slope < 0:
        return math.atan(slope) + math.pi
    else:
        return math.pi / 2


# 初始参数猜测
params_guess = np.array([1, 1])

# 使用最小二乘法进行拟合
result = least_squares(error_func, params_guess, args=(res_x, res_y))

# 获取拟合结果
a_fit, b_fit = result.x

# 获取夹角
angle = calculate_angle(a_fit)
print(math.degrees(angle))  # 将弧度转换为角度并打印
# 打印拟合结果
print("拟合直线方程：y =", a_fit, "* x +", b_fit)
