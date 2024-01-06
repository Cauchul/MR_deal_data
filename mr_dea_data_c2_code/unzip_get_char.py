# -*- coding: utf-8 -*-
import shutil

from matplotlib import pyplot as plt

from Common import *
import zipfile

from zcy_data_conversion import read_config_file


# def copy_file(in_src_f, in_targ_f):
#     shutil.copy2(in_src_f, in_targ_f)
    # shutil.copy(in_src_f, in_targ_f)

def list_files(directory):
    res_char_f_n = ''
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            if '-chart' in file_path:
                res_char_f_n = file
            if '-chart' in file_path or '_pc' in file_path:
                print(file_path)
                target_path = os.path.join(data_path, file)
                # 检查目标文件是否存在
                # if not os.path.exists(target_path):
                copy_file(file_path, data_path)

    return res_char_f_n


# 解压zip文件

def unzip(in_zip_file, in_out_path):
    with zipfile.ZipFile(in_zip_file, 'r') as zip_ref:
        zip_ref.extractall(in_out_path)


# 获取zip文件
data_path = r'D:\working\12月4号\C1\LTE\2934'
data_to_file(data_path)
# data_add_to_file(data_path)

out_path = os.path.join(data_path, 'tmp_path')
check_path(out_path)
# 解压路径
extraction_path = os.path.join(data_path, 'unzip')

zip_file = get_file_by_string('zip', data_path)
print('zip_file: ', zip_file)
# 调用函数解压ZIP文件到当前目录
unzip(zip_file, extraction_path)

# 遍历unzip路径，获取目录下的chart文件,拷贝到当前目录
char_f_n = list_files(extraction_path)
char_file = os.path.join(data_path, char_f_n)
print('char_file: ', char_file)

# Common.set_char(char_file)
#
# print('char: ', Common.get_char())
