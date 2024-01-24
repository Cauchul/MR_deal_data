# -*- coding: utf-8 -*-
import math
import os

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from sklearn.decomposition import PCA

from Common import FindFile, read_csv_get_df


class CommonFunc:
    @staticmethod
    def find_square_edges(points):
        # 应用PCA
        pca = PCA(n_components=1)  # 我们只需要第一个主成分
        pca.fit(points)

        # 得到第一个主成分方向
        component = pca.components_[0]

        # 计算该方向与x轴正方向之间的夹角
        angle = np.arctan2(component[1], component[0])

        # 旋转反向得到垂直于长边的斜率k
        slope = -1 / np.tan(angle)

        # 计算长边的截距b
        # 取所有点在第一个主成分方向上的投影的平均值来估计中心位置，
        # 然后计算中心点在垂直方向上的截距
        center_projection = pca.transform(pca.mean_.reshape(1, -1))[0][0]
        intercept = pca.mean_[1] - slope * pca.mean_[0]

        return slope, intercept

    @staticmethod
    def LineFit(x, y):
        points = np.zeros((len(x), 2), dtype=np.float64)
        for i in range(len(x)):
            points[i][0] = x[i]
            points[i][1] = y[i]
        k, b = CommonFunc.find_square_edges(points)
        return k, b

    @staticmethod
    def get_vector_degree(vector1, vector2):
        cos_angle = vector1.dot(vector2) / (np.linalg.norm(vector1) * np.linalg.norm(vector2))
        degree = math.degrees(math.acos(cos_angle))
        return degree

    # 顺时针旋转
    @staticmethod
    def Srotate(angle, valuex, valuey, pointx, pointy):
        valuex = np.array(valuex)
        valuey = np.array(valuey)
        sRotatex = (valuex - pointx) * math.cos(angle) + (valuey - pointy) * math.sin(angle) + pointx
        sRotatey = (valuey - pointy) * math.cos(angle) - (valuex - pointx) * math.sin(angle) + pointy
        return sRotatex, sRotatey

    # 逆时针旋转
    @staticmethod
    def Nrotate(angle, valuex, valuey, pointx, pointy):
        valuex = np.array(valuex)
        valuey = np.array(valuey)
        nRotatex = (valuex - pointx) * math.cos(angle) - (valuey - pointy) * math.sin(angle) + pointx
        nRotatey = (valuex - pointx) * math.sin(angle) + (valuey - pointy) * math.cos(angle) + pointy
        return nRotatex, nRotatey

    @staticmethod
    def rotate_point_by_radian(x, y, radian, is_s):
        if is_s:
            x, y = CommonFunc.Srotate(radian, x, y, 0.0, 0.0)
        else:
            x, y = CommonFunc.Nrotate(radian, x, y, 0.0, 0.0)
        return x, y

    @staticmethod
    def rotate_point_by_angle(x, y, angle, is_s):
        # 将角度转换为弧度
        radian = math.radians(angle)
        return CommonFunc.rotate_point_by_radian(x, y, radian, is_s)

    @staticmethod
    def cov_x_y_to_real_x_y(x_list, y_list):
        # 校正基准向量
        list_base = [0.0, 1.0]
        vector2 = np.array(list_base)
        k, b = CommonFunc.LineFit(x_list, y_list)
        list_to = [1, k]
        vector1 = np.array(list_to)
        degree = CommonFunc.get_vector_degree(vector2, vector1)
        if degree < 90:
            degree = 90 - degree
            ans = CommonFunc.rotate_point_by_angle(x_list, y_list, degree, True)
        elif degree == 90:
            return x_list, y_list
        else:
            degree = degree - 90
            ans = CommonFunc.rotate_point_by_angle(x_list, y_list, degree, False)
        return ans[0], ans[1]

    @staticmethod
    def data_conversion(in_value, df_data):
        delta_d = df_data.max() - df_data.min()
        t_x = in_value / delta_d
        n_values = df_data * t_x
        res_dat = n_values - n_values.min()
        return res_dat


# data_file = r'E:\work\MR_Data\1月22号\20210122\孙晨\IQOO7\4G\1\iQOO7-1_财经中心_2_4G_20240122-1451采样点数据-chart.csv'
#
# df = pd.read_csv(data_file, header=0, encoding="utf-8")
# x1, y1 = CommonFunc.cov_x_y_to_real_x_y(df['x'].values, df['y'].values)
# plt.plot(x1, y1)  # 绘制折线图
# plt.plot(df['x'], df['y'])  # 绘制折线图
# # plt.plot(x2, y2)  # 绘制折线图
# # plt.plot(df2['x'], df2['y'])  # 绘制折线图
# # plt.plot(x3, y3)  # 绘制折线图
# # plt.plot(x4, y4)  # 绘制折线图
# plt.title('Simple Line Plot')  # 设置标题
# plt.xlabel('X-axis')  # 设置x轴标签
# plt.ylabel('Y-axis')  # 设置y轴标签
# plt.savefig('')  # 显示图形
# print("Simple Line Plot")
# png_file = os.path.join(os.path.dirname(data_file), f'char_数据_转换图.png')
# plt.savefig(png_file)

if __name__ == '__main__':
    folder_path = r'E:\work\MR_Data\1月18号\20240118_源数据_clear'

    x = 30
    y = 30

    res_file_list = FindFile.find_files_with_string(folder_path, 'chart_clear')
    res_file_list = [x for x in res_file_list if 'unzip' not in x]
    for i_f in res_file_list:
        print(i_f)
        char_df = read_csv_get_df(i_f)
        x1, y1 = CommonFunc.cov_x_y_to_real_x_y(char_df['x'].values, char_df['y'].values)
        # res_x = CommonFunc.data_conversion(x, x1)
        # res_y = CommonFunc.data_conversion(y, y1)
        plt.plot(x1, y1)  # 绘制折线图
        plt.plot(char_df['x'], char_df['y'])  # 绘制折线图
        plt.title('Simple Line Plot')  # 设置标题
        plt.xlabel('X-axis')  # 设置x轴标签
        plt.ylabel('Y-axis')  # 设置y轴标签
        plt.savefig('')  # 显示图形
        png_file = os.path.join(os.path.dirname(i_f), f'走侧仪坐标转换图.png')
        plt.savefig(png_file)
        plt.clf()

        # plt.subplot(2, 1, 1)
        # plt.plot(res_x, res_y)
        # plt.gca().set_aspect('equal', adjustable='box')
        # plt.xlabel('X')
        # plt.ylabel('Y')
        # plt.title('坐标转换图')
        # png_file = os.path.join(os.path.dirname(i_f), f'走侧仪坐标转换图.png')
        # plt.savefig(png_file)
        # # 清除图形
        # plt.clf()
        #
        # plt.subplot(2, 1, 2)
        # plt.plot(char_df['x'], char_df['y'])
        # plt.gca().set_aspect('equal', adjustable='box')
        # plt.xlabel('X')
        # plt.ylabel('Y')
        # plt.title('原始图')
        # plt.tight_layout()
        # # plt.show()
        # png_file = os.path.join(os.path.dirname(i_f), f'走侧仪坐标原始图.png')
        # plt.savefig(png_file)
