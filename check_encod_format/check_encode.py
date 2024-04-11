# -*- coding: utf-8 -*-
import chardet

from Find_file.find_csv_file import get_current_dir_sub_dir, get_current_dir_file


def detect_encoding(in_file_path):
    with open(in_file_path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        res_encod = result['encoding']
        res_confidence = result['confidence']
        return res_encod, res_confidence


# # 要检查的文件路径
# file_path = r'G:\数据整理\验收\IQ7\4G_iQOO7_indoor_WT_LOG_DT_UE_0409_uemr.csv'  # 或者 'path_to_your_file.csv'
#
# # 检查文件的编码格式
# encoding, confidence = detect_encoding(file_path)
# print(f"File encoding: {encoding}, Confidence: {confidence}")

file_dir = r'G:\数据整理\验收'
res_dir_list = get_current_dir_sub_dir(file_dir)
for i_dir in res_dir_list:
    file_list = get_current_dir_file(i_dir)
    # print(file_list)
    for i_f in file_list:
        # detect_encoding(i_f)
        encoding, confidence = detect_encoding(i_f)
        print(f"File encoding: {encoding}, Confidence: {confidence}")
        if 'utf-8' != encoding:
            print(f"文件：{i_f} {encoding} 错误")
