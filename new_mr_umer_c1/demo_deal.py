# -*- coding: utf-8 -*-
from Common import list_files_in_directory, get_file_by_str
from unzip_file import unzip_zcy_data

data_path = r'D:\working\1214\1214国际财经中心(1)\国际财经中心\国际财经中心5G横1_20231214'

file_list = list_files_in_directory(data_path)

unzip_zcy_data(data_path)
print(file_list)
for i_f in file_list:
    if i_f.endswith('.csv'):
        print(i_f)


