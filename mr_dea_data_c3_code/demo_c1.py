# -*- coding: utf-8 -*-
import time

time_str = "2023-12-08 14:30:45.123456"
time_obj = time.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
print(time.mktime(time_obj))

