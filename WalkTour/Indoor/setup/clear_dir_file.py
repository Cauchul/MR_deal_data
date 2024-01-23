# -*- coding: utf-8 -*-
import os

from Common import FindFile

# 删除目录下指定文件

data_path = r'E:\work\MR_Data\1月22号\20210122\孙晨\RENO8'

# res_file_list = get_data_path_by_char(data_path, 'WalkTour')
# 找到当前目录下，所有的包含 WalkTour 的文件 WeTest
res_zip_file_list = FindFile.find_files_with_string(data_path, 'WalkTour')

for i_f in res_zip_file_list:
    print(i_f)
    os.remove(i_f)
