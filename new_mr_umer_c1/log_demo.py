# -*- coding: utf-8 -*-
import inspect
import os


def print_with_line_number(message):
    # 获取当前行号
    current_line = inspect.currentframe().f_back.f_lineno
    # 使用 f-string 格式化字符串，包含文件名和行号信息
    print(f"{os.path.basename(__file__)}:{current_line} - {message}")


# 例子
print_with_line_number("This is a message with line number.")
