# -*- coding: utf-8 -*-
# 创建目录结构

import os


def create_dir(in_path, in_num):
    """
    :param in_path: 需要创建目录的路径
    :param in_num: 创建目录的个数
    """
    for i in range(1, in_num + 1):
        # 拼接子目录的路径
        path = os.path.join(in_path, str(i))
        # 创建子目录
        os.makedirs(path)


if __name__ == '__main__':
    # 在当前目录下创建三个目录
    data_path = r'E:\work\MR_Data\1月23号\20240123(1)_new_no_table\20240123\岳云伟\IQOO7'
    create_dir(data_path, 3)