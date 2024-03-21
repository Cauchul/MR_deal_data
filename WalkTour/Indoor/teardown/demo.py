# -*- coding: utf-8 -*-

import os

# 获取当前工作目录
current_directory = r'E:\work\MrData\data_place\merge\4g'

# 列出当前目录下的所有文件和子目录
files_and_directories = os.listdir(current_directory)

# 筛选出所有的子目录
subdirectories = [os.path.join(current_directory, d) for d in files_and_directories if os.path.isdir(os.path.join(current_directory, d))]

# 打印所有子目录
for subdir in subdirectories:
    print(subdir)
