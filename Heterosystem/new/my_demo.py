# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

# 创建一个包含数字的 DataFrame
data = {'A': ['foo', 'bar', 'foo', 'bar', 'foo', 'bar'],
        'B': [10, 20, 30, 40, 50, 60],
        'C': [100, 200, 300, 400, 500, 600]}
df = pd.DataFrame(data)

# 显示原始 DataFrame
print("Original DataFrame:")
print(df)
print('==' * 50)
# new_df = pd.DataFrame()

# 按照列'A'的值进行分组
for i_c, i_group in df.groupby('A'):
    # print(i_group)
    cnt = 0
    # 遍历每一行数据
    for i_idx, i_data in i_group.iterrows():
        if cnt > 0:
            print('i_data', i_data)
            for i_in_c in ['B', 'C']:
                new_c = f'{i_in_c}{cnt}'
                df.loc[i_group.index[0], new_c] = i_data[i_in_c]
            df.drop(i_idx, inplace=True)
        cnt += 1
    print('--' * 50)

# df.reset_index()

print(df.index)

new_df = pd.DataFrame(columns=df.columns)

# 根据某一列进行行拓展
start = df['B'].iloc[0]
cn_index = inter_circ_index = 0

new_df.loc[cn_index] = df.iloc[inter_circ_index]
new_df.loc[cn_index, 'B'] = start

while True:
    if start >= df['B'].iloc[-1]:
        print('start', start)
        break

    cn_index += 1
    start += 1

    if start in df['B'].values:
        inter_circ_index += 1

    new_df.loc[cn_index] = df.iloc[inter_circ_index]
    new_df.loc[cn_index, 'B'] = start

print('--' * 50)
print(new_df)
