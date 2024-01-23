# -*- coding: utf-8 -*-
# 找到目录下的所有的output目录

import os


def find_output_dir(in_path):
    output_directories = []

    # 遍历根目录及其子目录
    for in_res_folder_name, in_res_sub_folder, in_res_file_name in os.walk(in_path):
        # 检查当前目录是否包含 "output"
        if "output" in in_res_sub_folder:
            output_directories.append(os.path.join(in_res_folder_name, "output"))

    return output_directories


# 指定根目录
root_directory = r"D:\working\data_conv\data_back\4G输出"

# 调用函数查找所有包含 "output" 的目录
result = find_output_dir(root_directory)

# 输出结果
print(result)
