# -*- coding: utf-8 -*-
from ftplib import FTP

# FTP服务器的连接信息
host = '116.6.50.82'
username = 'dingliftp'
password = 'dingliftp2023-2024'

# 本地文件路径
local_file_path = r'D:\working\data_conv\Reno8.7z'

# 远程目标目录
remote_directory = '/var/www/html/walkingindoor_data/MR'

# 创建FTP对象并连接到服务器
ftp = FTP(host)
ftp.login(username, password)

# 进入远程目标目录
ftp.cwd(remote_directory)

# 以二进制模式打开本地文件
with open(local_file_path, 'rb') as file:
    # 上传文件到远程服务器
    ftp.storbinary(f'STOR Reno8.7z', file)

# 关闭FTP连接
ftp.quit()
