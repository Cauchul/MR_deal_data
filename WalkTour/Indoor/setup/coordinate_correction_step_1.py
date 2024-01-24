# -*- coding: utf-8 -*-
import math
import os

import numpy as np
from matplotlib import pyplot as plt
from sklearn.decomposition import PCA

from Common import FindFile, read_csv_get_df, df_write_to_csv, file_add_specified_suffix, print_with_line_number


# 坐标纠偏

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
    def S_rotate(angle, in_x, in_y, point_x, point_y):
        in_x = np.array(in_x)
        in_y = np.array(in_y)
        sRotate_x = (in_x - point_x) * math.cos(angle) + (in_y - point_y) * math.sin(angle) + point_x
        sRotate_y = (in_y - point_y) * math.cos(angle) - (in_x - point_x) * math.sin(angle) + point_y
        return sRotate_x, sRotate_y

    # 逆时针旋转
    @staticmethod
    def N_rotate(angle, in_x, in_y, point_x, point_y):
        in_x = np.array(in_x)
        in_y = np.array(in_y)
        sRotate_x = (in_x - point_x) * math.cos(angle) - (in_y - point_y) * math.sin(angle) + point_x
        sRotate_y = (in_x - point_x) * math.sin(angle) + (in_y - point_y) * math.cos(angle) + point_y
        return sRotate_x, sRotate_y

    @staticmethod
    def rotate_point_by_radian(x, y, radian, is_s):
        if is_s:
            x, y = CommonFunc.S_rotate(radian, x, y, 0.0, 0.0)
        else:
            x, y = CommonFunc.N_rotate(radian, x, y, 0.0, 0.0)
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

    @staticmethod
    def find_center(points):

        # 检测方向 - 使用PCA来找到数据的主方向
        pca = PCA(n_components=2)  # 我们要检测到二维数据集的两个主成分
        pca.fit(points)

        # 得到旋转后的坐标系中的点的坐标
        transformed_points = pca.transform(points)

        # 在新坐标系中计算中心点
        center_point_transformed = np.mean(transformed_points, axis=0)

        # 将中心点坐标转换回原来的坐标系
        center_point = pca.inverse_transform(center_point_transformed)

        return center_point

    @staticmethod
    def move_xy_to_real_xy(x_list, y_list, center_need_x, center_need_y):
        points = np.zeros((len(x_list), 2), dtype=np.float64)
        for i in range(len(x_list)):
            points[i][0] = x_list[i]
            points[i][1] = y_list[i]
        center_point = CommonFunc.find_center(points)
        # 剩下的就简单了，中心点和中心点需要的位置都知道了，那么就可以直接平移了
        x_out = np.zeros((len(x_list)), dtype=np.float64)
        y_out = np.zeros((len(x_list)), dtype=np.float64)
        x_move = center_need_x - center_point[0]
        y_move = center_need_y - center_point[1]
        for i in range(len(x_list)):
            x_out[i] = x_list[i] + x_move
            y_out[i] = y_list[i] + y_move
        return x_out, y_out

def generate_PNG_image(in_png_file):
    plt.plot(x1, y1)  # 绘制折线图
    plt.plot(char_df['x'], char_df['y'])  # 绘制折线图
    plt.title('Simple Line Plot')  # 设置标题
    plt.xlabel('X-axis')  # 设置x轴标签
    plt.ylabel('Y-axis')  # 设置y轴标签
    plt.savefig('')  # 显示图形
    print_with_line_number(f'走侧仪坐标转换图：{in_png_file}', __file__)
    plt.savefig(in_png_file)
    plt.clf()


if __name__ == '__main__':
    folder_path = r'E:\work\MR_Data\1月18号\20240118_源数据_clear\室内'

    res_file_list = FindFile.find_files_with_string(folder_path, 'chart')
    res_file_list = [x for x in res_file_list if
                     'unzip' not in x and 'coordinate_correction' not in os.path.basename(x)]

    res_file_list = [x for x in res_file_list if 'chart_clear' in os.path.basename(x)] if any('chart_clear' in os.path.basename(x) for x in res_file_list) else res_file_list

    for i_f in res_file_list:
        print_with_line_number(f'当前处理文件：{i_f}', __file__)
        char_df = read_csv_get_df(i_f)
        # # 像素坐标转米坐标
        # res_x = CommonFunc.data_conversion(30, char_df['x'])
        # res_y = CommonFunc.data_conversion(30, char_df['y'])

        # 旋转纠偏
        x1, y1 = CommonFunc.cov_x_y_to_real_x_y(char_df['x'], char_df['y'])
        # 水平平移纠偏
        x1_1, y1_1 = CommonFunc.move_xy_to_real_xy(x1, y1, 800, 600)

        char_df['x'] = x1_1
        char_df['y'] = y1_1

        res_name = file_add_specified_suffix(i_f, 'coordinate_correction')
        print_with_line_number(f'坐标纠偏之后zcy文件：{res_name}', __file__)

        df_write_to_csv(char_df, res_name)

        # 生成png图片
        # png_file = os.path.join(os.path.dirname(i_f), f'走侧仪坐标转换图.png')
        # generate_PNG_image(png_file)

        print('---' * 50)
