# -*- coding: utf-8 -*-
import os

import pandas as pd

from Common import list_files_in_directory, get_csv_list_all_df

deal_path = r'D:\working\merge\wifi_data\NR'
out_path = os.path.join(deal_path, 'ne_nr_merged_file.csv')
# 2. 读取所有CSV文件并合并
file_list = list_files_in_directory(deal_path)
# df_list = []
#
# for i_f in file_list:
#     print(i_f)
#     df = pd.read_csv(i_f)
#     df_list.append(df)
df_list = get_csv_list_all_df(file_list)

merged_df = pd.concat(df_list, ignore_index=True)

# 3. 将合并后的数据保存为新的CSV文件
merged_df.to_csv(out_path, index=False)
