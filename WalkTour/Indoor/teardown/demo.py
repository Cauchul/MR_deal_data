# -*- coding: utf-8 -*-
import pandas as pd

from Common import read_csv_get_df, df_write_to_csv, file_add_specified_suffix, print_with_line_number


# 创建示例 DataFrame
# data = {'column_name': [10, 20, 30, 40, 50]}
# df = pd.DataFrame(data)

def between_df(in_data_df, in_range_dict, in_sid_v):
    in_data_df.loc[in_data_df['f_x'].between(in_range_dict['x'][0], in_range_dict['x'][1]) & in_data_df['f_y'].between(
        in_range_dict['y'][0], in_range_dict['y'][1]), 'f_sid'] = in_sid_v


# data_file = r'E:\work\MR_Data\1月15号\demo\4G_HaiDian_indoor_WT_LOG_DT_UE_0115_finger_正横_h1.csv'

# data_df = read_csv_get_df(data_file)
# data_df.loc[(data_df['f_y'] > 27.2) | ((data_df['f_x'] < 11.85) & (data_df['f_y'] < 25)), 'f_sid'] = 1
#
# data_df.loc[data_df['f_y'] < 4.255, 'f_sid'] = 3
#
# data_df.loc[~((data_df['f_y'] > 26.7) | (data_df['f_y'] < 2.66)), 'f_sid'] = 2


# data_df.loc[(data_df['f_x'] < 13.8) & (data_df['f_y'] > 26.7), 'f_sid'] = 1

# data_df.loc[data_df['f_y'] > 26.7, 'f_sid'] = 1
#
# data_df.loc[data_df['f_y'] < 2.66, 'f_sid'] = 3
#
# data_df.loc[~((data_df['f_y'] > 26.7) | (data_df['f_y'] < 2.66)), 'f_sid'] = 2

# data_df.loc[(data_df['f_x'] > 9.82) & (data_df['f_y'] > 2.66) & data_df['f_y'] < 26.7, 'f_sid'] = 1

# range_dict = {'x': [12.3, 30], 'y': [0, 4.25]}
# between_df(data_df, range_dict, 2)
#
# range_dict = {'x': [9.67, 30], 'y': [4.25, 27.2]}
# between_df(data_df, range_dict, 2)
#
# range_dict = {'x': [9.67, 30], 'y': [0, 3.266]}
# between_df(data_df, range_dict, 2)

# def between_df(in_data_df, in_range_dict, in_sid_v):
#     in_data_df.loc[in_data_df['f_x'].between(in_range_dict['x'][0], in_range_dict['x'][1]) & in_data_df['f_y'].between(
#         in_range_dict['y'][0], in_range_dict['y'][1]), 'f_sid'] = in_sid_v


# data_df.loc[data_df['f_x'].between(9.8, 19.43) & data_df['f_y'].between(3.26, 26.6), 'f_sid'] = 2
#
# data_df.loc[(data_df['f_x'] > 9.67) & (data_df['f_y'] < 2.66), 'f_sid'] = 3

# filtered_df = data_df[(data_df['f_x'] > x_bound) & (data_df['f_y'] < y_bound)]

# data_df.loc[(data_df['f_x'] > 9.67) & (data_df['f_y'] < 2.66), 'f_sid'] = 3

# filtered_df = data_df[data_df['f_sid'] == 3
# print(filtered_df)
# df_write_to_csv(filtered_df, r'E:\work\MR_Data\1月15号\dede.csv')
# 打印筛选结果
# print(data_df)
#
# res_name = file_add_specified_suffix(data_file, 'set_sid')
# print(res_name)
#
# df_write_to_csv(data_df, r'E:\work\MR_Data\1月15号\demo.csv')

def set_abeam_h1(in_data_file, in_sid):
    in_data_df = read_csv_get_df(in_data_file)

    print('in_sid: ', in_sid)
    # in_data_df.loc[(in_data_df['f_x'] < 11.85) & ((in_data_df['f_y'] > 25) | (in_data_df['f_y'] > 27.6)), 'f_sid'] = in_sid
    in_data_df.loc[(in_data_df['f_y'] > 27.6) | (in_data_df['f_x'] < 11.85), 'f_sid'] = in_sid

    in_data_df.loc[in_data_df['f_y'] < 4.255, 'f_sid'] = in_sid + 2

    in_data_df.loc[
        ~((in_data_df['f_y'] < 4.255) | (in_data_df['f_y'] > 27.6) | (in_data_df['f_x'] < 11.82)), 'f_sid'] = in_sid + 1

    in_res_name = file_add_specified_suffix(in_data_file, 'set_sid')
    print_with_line_number(f'输出文件：{in_res_name}', __file__)

    df_write_to_csv(in_data_df, in_res_name)

    check_df(in_res_name, in_sid + 1)


def set_ortho_vertical_h1():
    pass


def check_df(in_data_file, in_check_id):
    in_data_df = read_csv_get_df(in_data_file)
    in_filtered_df = in_data_df[in_data_df['f_sid'] == in_check_id]
    in_res_name = file_add_specified_suffix(in_data_file, f'check_id_{in_check_id}')
    print_with_line_number(f'输出文件：{in_res_name}', __file__)
    df_write_to_csv(in_filtered_df, in_res_name)


def check_in_df(in_data_df, in_check_id, in_data_file):
    in_res_check_name = file_add_specified_suffix(in_data_file, f'check_id_{in_check_id}')
    print_with_line_number(f'输出文件：{in_res_check_name}', __file__)
    in_filtered_df = in_data_df[in_data_df['f_sid'] == in_check_id]
    df_write_to_csv(in_filtered_df, in_res_check_name)


if __name__ == '__main__':
    # set_abeam_h1(r'E:\work\MR_Data\1月15号\demo\4G_HaiDian_indoor_WT_LOG_DT_UE_0115_finger_正横_h1.csv', 7)

    data_file = r'E:\work\MR_Data\1月15号\demo\4G_HaiDian_indoor_WT_LOG_DT_UE_0115_finger_正横_h1.csv'
    start_sid = 7

    data_df = read_csv_get_df(data_file)

    x1 = [0, 29.27]
    y1 = [27, 30.1]
    data_df.loc[data_df['f_x'].between(x1[0], x1[1]) & data_df['f_y'].between(y1[0], y1[1]), 'f_sid'] = start_sid

    x3 = [11.1, 30.1]
    y3 = [0, 4.256]
    data_df.loc[data_df['f_x'].between(x3[0], x3[1]) & data_df['f_y'].between(y3[0], y3[1]), 'f_sid'] = start_sid + 2

    data_df.loc[~(data_df['f_x'].between(x1[0], x1[1]) & data_df['f_y'].between(y1[0], y1[1]) | data_df['f_x'].between(
        x3[0], x3[1]) & data_df['f_y'].between(y3[0], y3[1])), 'f_sid'] = start_sid + 1

    res_name = file_add_specified_suffix(data_file, 'set_sid')
    print_with_line_number(f'输出文件：{res_name}', __file__)

    for i_v in range(3):
        check_in_df(data_df, start_sid, data_file)
        start_sid += 1
    # check_id = start_sid + 1
    # filtered_df = data_df[data_df['f_sid'] == check_id]
    # check_name = file_add_specified_suffix(data_file, f'check_id_{check_id}')
    # print_with_line_number(f'输出文件：{check_name}', __file__)
    # df_write_to_csv(filtered_df, check_name)

    df_write_to_csv(data_df, res_name)
