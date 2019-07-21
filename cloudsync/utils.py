import os
import sys
import hashlib
from catalog import Catalog

# 加密算法 (可选 sha1 | sha256 | sha512)
hash_type = "sha256"
# 临时文件路径，计算云文件的摘要时，预先将文件下载至临时文件
temp_file_path = os.path.join(sys.path[0], 'cloudsync_temp_file')


def get_local_file_hash(file_path):
    """
    计算本地整个文件的摘要
    :param file_path: 本地文件路径
    :return: 文件摘要
    """
    try:
        with open(file_path, 'rb') as f:
            return getattr(hashlib, hash_type)(f.read()).hexdigest()
    except Exception as e:
        print(e)
        return ''


def get_cloud_file_hash(cloud_path, cfs):
    """
    计算云端整个文件的摘要
    :param cloud_path: 云端文件路径
    :param cfs: 云文件系统
    :return: 文件摘要
    """
    cfs.download(cloud_path, temp_file_path)
    return get_local_file_hash(temp_file_path)


def get_buffer_hash(buffer: bytes):
    """
    计算字节数组的摘要
    :param buffer: 要计算摘要的字节数组
    :return: 摘要值
    """
    return getattr(hashlib, hash_type)(buffer).hexdigest()


def get_entire_local_directory_hash(directory):
    """
    计算本地目录状态的摘要
    :param directory: 本地目录状态
    :return: 文件摘要
    """
    files_hash_value = ''
    for child in directory.child:
        if child.file_type == Catalog.IS_FOLDER:
            files_hash_value += get_entire_local_directory_hash(child)
        else:
            child.hash_value = get_local_file_hash(child.filename)
            files_hash_value += child.hash_value
    directory.hash_value = get_buffer_hash(files_hash_value.encode())
    return directory.hash_value


def get_entire_cloud_directory_hash(directory, cfs):
    """
    计算云端目录状态的摘要
    :param directory: 云端目录状态
    :param cfs: 云文件系统
    :return: 文件摘要
    """
    files_hash_value = ''
    for child in directory.child:
        if child.file_type == Catalog.IS_FOLDER:
            files_hash_value += get_entire_cloud_directory_hash(child, cfs)
        else:
            child.hash_value = cfs.get_hash(child.filename)
            files_hash_value += child.hash_value
    directory.hash_value = get_buffer_hash(files_hash_value.encode())
    return directory.hash_value
