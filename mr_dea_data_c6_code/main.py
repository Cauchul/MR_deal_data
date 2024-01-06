# -*- coding: utf-8 -*-
from Common import get_all_data_path, check_path, clear_merge_path
from DealZCY import unzip_and_deal_zcy, unzip_deal_zcy_one_file, unzip_deal_zcy_no_wifi
from WalkTour import WalkTour
from WeTest import WeTest
from merge_h_v_data import merge_res_file

# 输出路径
out_data_path = r'E:\work\demo_merge\merged'
clear_merge_path(out_data_path)
check_path(out_data_path)


if __name__ == '__main__':
    # 数据路径
    data_path = r'D:\working\1206_国际财经中心测试V1\场景2\8539\NR'
    if '国际财经中心' in data_path:
        test_area = 'HaiDian'
    elif '国家会议中心' in data_path:
        test_area = 'CaoYang'
    else:
        test_area = 'DaXin'

    data_path_list = get_all_data_path(data_path)
    print(data_path_list)
    # 处理走测仪数据
    # unzip_deal_zcy_no_wifi(data_path)
    unzip_and_deal_zcy(data_path_list)
    # # 合并ue、zcy数据，生成标准文件
    # deal_log_data(data_path_list, test_area)
    # walktour 室外
    # WalkTour.deal_data_only_zcy_outdoor(data_path, 'HaiDian')
    # WalkTour.deal_multiple_dir_data(data_path_list, test_area)
    # WeTest.deal_data_only_zcy_indoor(data_path, test_area)

    # # 合并输出横纵数据, in_clear_flag 表示是否清理中间数据，如果为true表示清理
    # merge_res_file(out_data_path, in_clear_flag=False)
