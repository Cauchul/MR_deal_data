import os

from Common import clear_path, get_file_by_string, unzip


def unzip_zip_file(self, in_path):
    # 解压目录
    in_extraction_path = os.path.join(in_path, 'unzip')
    clear_path(in_extraction_path)
    # 获取压缩文件
    in_zip_file = get_file_by_string('zip', in_path)
    print('zip_file: ', in_zip_file)
    print('unzip_path: ', in_extraction_path)
    # 解压
    unzip(in_zip_file, in_extraction_path)
    return in_extraction_path
