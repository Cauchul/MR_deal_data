# -*- coding: utf-8 -*-
# 判断时间戳是否正确
from datetime import datetime

# 示例秒数
seconds = 1639492300

# 将秒数转换为 datetime 类型
datetime_object = datetime.fromtimestamp(seconds)

print("Datetime object:", datetime_object)

