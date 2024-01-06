# -*- coding: utf-8 -*-
import re

input_string = "abc123def456"


def get_split_str(in_str):
    res_list = re.findall(r'[a-zA-Z]+|\d+', in_str)
    return res_list


result = get_split_str(input_string)

print(result)
