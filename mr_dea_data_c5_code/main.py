# -*- coding: utf-8 -*-
from Common import get_all_data_path, check_path, clear_merge_path
from deal_ue_log_data import deal_log_data
from deal_zcy_data import unzip_and_deal_zcy
from merge_h_v_data import merge_res_file
# 输出路径
out_data_path = r'E:\work\demo_merge\merged'
clear_merge_path(out_data_path)
check_path(out_data_path)


# 数据路径
data_path = r'D:\working\1206_国际财经中心测试V1\场景2\8539\LTE'
if '国际财经中心' in data_path:
    test_area = 'HaiDian'
elif '国家会议中心' in data_path:
    test_area = 'CaoYang'
else:
    test_area = 'DaXin'

data_path_list = get_all_data_path(data_path)
# 处理走测仪数据
unzip_and_deal_zcy(data_path_list)
# 合并ue、zcy数据，生成标准文件
deal_log_data(data_path_list, test_area)

# 合并输出横纵数据, in_clear_flag 表示是否清理中间数据，如果为true表示清理
merge_res_file(out_data_path, in_clear_flag=False)
