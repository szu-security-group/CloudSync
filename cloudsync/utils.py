import os
import sys
import hashlib
from catalog import Catalog, initialize_metatree_local, initialize_metatree_cloud

# 加密算法 (可选 sha1 | sha256 | sha512)
hash_type = "sha256"
# 临时文件路径，计算云文件的摘要时，预先将文件下载至临时文件
temp_file_path = os.path.join(sys.path[0], 'cloudsync_temp_file')


def get_local_file_hash(file_path):
    """
    计算本地整个文件的摘要
    :param file_path: 文件路径
    :return: 文件摘要
    """
    with open(file_path, 'rb') as f:
        return getattr(hashlib, hash_type)(f.read()).hexdigest()


def get_buffer_hash(buffer: bytes):
    """
    计算字节数组的摘要
    :param buffer:
    :return: 摘要值
    """
    return getattr(hashlib, hash_type)(buffer).hexdigest()


def get_cloud_file_hash(cloud_path, cfs):
    cfs.download(cloud_path, temp_file_path)
    return get_local_file_hash(temp_file_path)


def get_entire_local_directory_hash(directory):
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
    files_hash_value = ''
    for child in directory.child:
        if child.file_type == Catalog.IS_FOLDER:
            files_hash_value += get_entire_cloud_directory_hash(child, cfs)
        else:
            child.hash_value = cfs.get_hash(child.filename)
            files_hash_value += child.hash_value
    directory.hash_value = get_buffer_hash(files_hash_value.encode())
    return directory.hash_value


def get_entire_directory(directory):
    files_hash_value = ''
    for child in directory.child:
        if child.file_type == Catalog.IS_FOLDER:
            files_hash_value += get_entire_directory(child)
        else:
            files_hash_value += child.hash_value
    directory.hash_value = get_buffer_hash(files_hash_value.encode())
    return directory.hash_value


def get_entire_directory_hash_by_local_path(local_path):
    root = initialize_metatree_local(local_path)
    files_hash_value = ''
    for it in root.child:
        if it.file_type == Catalog.IS_FOLDER:
            it.hash_value = get_entire_directory_hash_by_local_path(local_path + it.filename)
            files_hash_value += it.hash_value
        elif it.file_type == Catalog.IS_FILE:
            it.hash_value = get_local_file_hash(local_path + it.filename)
            files_hash_value += it.hash_value
        else:
            print("Unknown file type!")
    # 计算目录摘要值
    return get_buffer_hash(files_hash_value.encode())


def get_entire_directory_hash_by_cloud_path(cloud_path, cfs):
    root = initialize_metatree_cloud(cloud_path, cfs)
    files_hash_value = ''
    for it in root.child:
        if it.file_type == Catalog.IS_FOLDER:
            it.hash_value = get_entire_directory_hash_by_cloud_path(cloud_path + it.filename, cfs)
            files_hash_value += it.hash_value
        elif it.file_type == Catalog.IS_FILE:
            it.hash_value = get_cloud_file_hash(cloud_path + it.filename, cfs)
            files_hash_value += it.hash_value
        else:
            print("Unknown file type!")
    # 计算目录摘要值
    return get_buffer_hash(files_hash_value.encode())


def str2hex(s):
    """
    将字符串转换成十六进制
    :param s: 输入串
    :return: 十六进制字符串
    """
    return ''.join([hex(ord(i))[2:] for i in s])
