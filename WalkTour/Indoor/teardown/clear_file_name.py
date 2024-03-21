# -*- coding: utf-8 -*-
import os


def get_cur_dir_all_csv(in_src_data):
    tmp_csv_files = [os.path.join(in_src_data, file) for file in os.listdir(in_src_data) if
                     file.endswith('.csv')]
    return tmp_csv_files


if __name__ == '__main__':
    folder_path = r'D:\MrData\2月28号\20240228\5G\WeTest\荣耀90'
    # 获取当前路径下的所有csv文件
    res_file_list = get_cur_dir_all_csv(folder_path)

    for i_f in res_file_list:
        print(i_f)
        if 'Beam_' in i_f:
            new_name = i_f.replace('Beam_', '')
            os.rename(i_f, new_name)

