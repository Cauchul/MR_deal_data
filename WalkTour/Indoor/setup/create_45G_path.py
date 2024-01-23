# -*- coding: utf-8 -*-
# 创建目录结构

import os


def create_dir(in_path, in_list):
    for i_dir in in_list:
        # 拼接子目录的路径
        path = os.path.join(in_path, i_dir)
        # 创建子目录
        os.makedirs(path)


if __name__ == '__main__':
    dir_list = ['4G', '5G']
    # 在当前目录下创建三个目录
    data_path = r'E:\work\MR_Data\1月22号\20210122\孙晨\小米12'
    create_dir(data_path, dir_list)
