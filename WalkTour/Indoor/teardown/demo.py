# -*- coding: utf-8 -*-

# def get_values_from_list(input_number, in_list):
#     # 计算起始索引和结束索引
#     start_index = -input_number
#
#     for i in range(input_number):
#         print(i)
#     end_index = start_index - 3
#
#     # 使用切片获取相应位置上的值
#     selected_values = in_list[start_index:end_index:-1]
#
#     return selected_values
#
#
# # 示例列表
# my_list = [1, 2, 3, 4, 5, 6]
#
# # 输入为2，获取-2, -3, -4位置上的值
# result_for_input_2 = get_values_from_list(2, my_list)
# print("输入为2的结果:", result_for_input_2)
#
# # 输入为3，获取-3, -4, -5位置上的值
# result_for_input_3 = get_values_from_list(3, my_list)
# print("输入为3的结果:", result_for_input_3)

my_list = [1, 2, 3, 4, 5, 6]

cnt_flag = 2

selected_values = my_list[-(cnt_flag + 1):-1]
print(selected_values)


