# -*- coding: utf-8 -*-
import pandas as pd

# 读取数据文件，假设文件名为 data.csv
df = pd.read_csv(r'E:\work\MR_Data\1月12号\下午测试(1)_new_no_table\下午测试\3\5G\output\Beam_5G_HaiDian_indoor_WT_LOG_DT_UE_0112_finger.csv')

df = df.dropna(subset=['f_longitude', 'f_latitude'], how='any')
print('---' * 50)

# 遍历每一行数据
for index, row in df.iterrows():
    # 获取 f_server_sid 列的值
    server_sid = int(row['f_server_sid'])

    print('server_sid: ', server_sid)

    # 检查 f_sid_1_rsrp 和 f_sid_1_rsrq 是否为空
    if pd.isna(row[f'f_sid_{server_sid}_rsrp']) or pd.isna(row[f'f_sid_{server_sid}_rsrq']):
        # 如果任一为空，就删除这一行数据
        df.drop(index, inplace=True)

# 显示处理后的数据
print(df)
df.to_csv(r'E:\work\MR_Data\1月12号\下午测试(1)_new_no_table\下午测试\3\5G\output\demo.csv', index=False)