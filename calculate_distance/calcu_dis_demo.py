# -*- coding: utf-8 -*-
import pandas as pd

from geopy.distance import geodesic
from itertools import combinations

from calculate_distance.calcu_dis_by_lat_lon import calculate_distance


def calculate_dis_by_df(in_df_data):
    # tmp_dis_dict = {}
    max_dis = 0
    for pair in combinations(in_df_data, 2):
        coord_1, coord2 = pair
        res_dis = calculate_distance(coord_1[1], coord_1[0], coord2[1], coord2[0])
        # print('res_dis: ', res_dis)
        # if int(res_dis) > 5 or res_dis >= max_dis:
        if res_dis >= max_dis:
            max_dis = res_dis
            # print('res_dis: ', res_dis)
            # tmp_dis_dict[in_f_time] = max_dis

    return max_dis


if __name__ == '__main__':
    df = pd.read_csv(r'E:\work\MR_Data\data_place\demo\out.csv')

    merged_data = df[['f_longitude', 'f_latitude']].values.tolist()

    print(df['f_time'][0])

    res = calculate_dis_by_df(merged_data)
    print(res)
