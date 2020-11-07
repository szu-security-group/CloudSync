import os
import sys
import hashlib

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
