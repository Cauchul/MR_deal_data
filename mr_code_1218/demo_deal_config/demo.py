# -*- coding: utf-8 -*-
import pandas as pd

from demo_deal_config.mr_deal_data import DealData

uemr_file = r'D:\working\1214\1214国际财经中心(1)\bug_data\uemr_data\4g1219hive.csv'
char_file = r'D:\working\1214\1214国际财经中心(1)\bug_data\uemr_data\国际财经中心_A座_2_5G_20231214采样点数据-chart.csv'


uemr_df = pd.read_csv(uemr_file, low_memory=False)
char_df = pd.read_csv(char_file, low_memory=False)

# char_df = char_df[
#     ['test_time', 'created_by_ue_time', 'x', 'y', 'longitude', 'latitude', 'direction', 'altitude']]
#
# uemr_df['f_direction'] = ''
# print(char_df.columns)
merge_df = pd.merge(uemr_df, char_df, left_on="f_time", right_on="created_by_ue_time",
                                    how='left')

print(merge_df)
