# -*- coding: utf-8 -*-
# 压缩文件
import os
import zipfile


# 创建一个zip文件
# zip_file = zipfile.ZipFile(r'D:\working\data_conv\out_path\example.zip', 'w')
#
# file_list = [r'D:\working\data_conv\out_path\小米12-1-4G-uemr_xyToLonLat_ZCY.csv', r'D:\working\data_conv\out_path\小米12-1-5G-uemr_xyToLonLat_ZCY.csv']

def zip_file(in_file_list, in_zip_file):
    tmp_zip_file = zipfile.ZipFile(in_zip_file, 'w')
    for i_f in in_file_list:
        tmp_zip_file.write(i_f, os.path.basename(i_f))
    tmp_zip_file.close()

# 向zip文件中添加一个目录及其下的所有文件
# zip_file.write(r'D:\working\data_conv\out_path', arcname='out_path')

# 关闭zip文件对象

