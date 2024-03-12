# -*- coding: utf-8 -*-
# 删除5G rsrp sinr等信息列
import pandas as pd

from Common import df_write_to_csv


def del_deal_data(in_data_file):
    res_df = pd.read_csv(in_data_file, low_memory=False)

    # 获取要删除的列索引范围
    if 'NARFCN' in res_df.columns:
        start_col = res_df.columns.get_loc('NARFCN')
        end_col = start_col + 5
        print(f'start_col:{start_col} end_col:{end_col}')

        # 根据列索引范围删除连续列
        res_df = res_df.drop(res_df.columns[start_col:end_col], axis=1)

        res_df = res_df.rename(
            columns={
                'PCI.1': 'PCI',
                'RSRP.1': 'RSRP',
                'RSRQ.1': 'RSRQ',
            })

        df_write_to_csv(res_df, in_data_file)


if __name__ == '__main__':
    data_file = r'D:\MrData\3月4号_new\NR_MR_Detail_20240311095550_demo.csv'
    del_deal_data(data_file)
