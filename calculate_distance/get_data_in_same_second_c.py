# -*- coding: utf-8 -*-
import csv

import pandas as pd

from Common import df_write_to_csv

# 读取 CSV 文件
df = pd.read_csv(r'E:\work\MR_Data\data_place\demo\test.csv')

# 按照 'Category' 列的值进行分组
grouped = df.groupby('f_time')

with open(r'E:\work\MR_Data\data_place\demo\out.csv', mode='a', newline='') as file:
    csv_writer = csv.writer(file)

    for group_name, group_data in grouped:
        # print(f"Group {group_name}:")
        print(group_data.columns)
        # df_write_to_csv(group_data,)
        csv_writer.writerows(group_data)

        cnt = 1
        if cnt > 4:
            break


# 打印结果
# print(result)

