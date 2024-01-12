# -*- coding: utf-8 -*-
# 设置pid
from Common import get_all_data_path, print_with_line_number, Common, read_csv_get_df, df_write_to_csv


def set_pid(in_char='4G'):
    print_with_line_number(f'当前查找文件的标志为：{in_char}', __file__)
    in_file_list = []
    in_res_list = get_all_data_path(folder_path, in_char)
    # print(res_list)
    for i_path in in_res_list:
        print_with_line_number(f'当前处理的路径：{i_path}', __file__)
        # res_list = Common.split_path_get_list(i_path)
        # print_with_line_number(f'路径list: {res_list}', __file__)
        res_file_list = Common.list_files_in_directory(i_path)
        in_file_list.extend(res_file_list)
        print_with_line_number(f'返回的文件列表：{res_file_list}', __file__)
        print('---' * 50)
    # print_with_line_number(f'需要合并的文件列表为：{in_file_list}', __file__)
    for i_f in in_file_list:
        print_with_line_number(f'修改文件的pid：{i_f}', __file__)
        res_df = read_csv_get_df(i_f)
        res_df['f_pid'] = (res_df.index + 1).astype(str)
        df_write_to_csv(res_df, i_f)
        # print('---' * 50)


if __name__ == '__main__':
    folder_path = r'E:\work\MR_Data\data_place'
    set_pid('4G')
    print('++' * 50)
    set_pid('5G')
    # file_list = []
    # res_list = get_all_data_path(folder_path, '5G')
    # # print(res_list)
    # for i_path in res_list:
    #     print_with_line_number(f'当前处理的路径：{i_path}', __file__)
    #     # res_list = Common.split_path_get_list(i_path)
    #     # print_with_line_number(f'路径list: {res_list}', __file__)
    #     res_file_list = Common.list_files_in_directory(i_path)
    #     file_list.extend(res_file_list)
    #     print_with_line_number(f'返回的文件列表：{res_file_list}', __file__)
    #     print('---' * 50)
    # print_with_line_number(f'需要合并的文件列表为：{file_list}', __file__)
    # for i_f in file_list:
    #     res_df = read_csv_get_df(i_f)
    #     res_df['f_pid'] = (res_df.index + 1).astype(str)
    #     df_write_to_csv(res_df, i_f)
