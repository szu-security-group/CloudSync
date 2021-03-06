import os
import time
import logging
import inspect
from sortedcontainers import SortedList

import utils
from cfs import CloudFileSystem


class Catalog:
    # 文件或目录的标志值
    IS_FOLDER = 1
    IS_FILE = 2
    # 文件目录标志
    file_type = 0

    # 文件目录名
    filename = ''
    # 文件修改时间
    mtime = ''
    # 文件的 hash
    file_id = ''

    def __init__(self, file_type, filename, mtime, file_id):
        self.file_type = file_type
        self.filename = filename
        self.mtime = mtime if mtime is not None else ''
        self.file_id = file_id if file_id is not None else ''

    def __eq__(self, other):
        return self.filename == str(other.filename)

    def __lt__(self, other):
        return self.filename < str(other.filename)

    def __str__(self):
        return '[Catalog: filename={filename} file_id={file_id}]'.format(
            filename=self.filename,
            file_id=self.file_id[:6]
        )


class DirectoryStatus(Catalog):
    """
    存储文件夹元信息的结构
    """
    children = []

    def __init__(self, filename, mtime=None, file_id=None, children=None):
        filename += '/' if not filename.endswith('/') else ''
        super().__init__(Catalog.IS_FOLDER, filename, mtime, file_id)
        self.children = children if isinstance(children, SortedList) else SortedList([])

    def insert(self, catalog):
        """
        将文件(夹)结构插入该文件夹的子文件列表中
        :param catalog: 文件(夹)结构
        :return: None
        """
        if catalog not in self.children:
            self.children.add(catalog)

    def __str__(self):
        return '[DirectoryStatus: filename={filename} file_id={file_id}'.format(
            filename=self.filename,
            file_id=self.file_id[:6]
        )


class FileStatus(Catalog):
    """
    存储文件元信息的结构
    """

    def __init__(self, filename, mtime=None, file_id=None):
        super().__init__(Catalog.IS_FILE, filename, mtime, file_id)

    def __str__(self):
        return '[FileStatus: filename={filename} mtime={mtime} file_id={file_id}]'.format(
            filename=self.filename,
            mtime=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(self.mtime))),
            file_id=self.file_id[:6]
        )


def initialize_metatree_local(local_path: str, local_dict: dict):
    """
    初始化本地元信息树
    :param local_path: 元信息树的根目录
    :param local_dict: 用于加速查找的字典
    :return: 以 local_path 为根的元信息树
    """
    logger = logging.getLogger('{function_name}'.format(function_name=inspect.stack()[0].function))
    logger.info('构建本地当前目录状态 {local_path}'.format(local_path=local_path))
    # 构建该目录的目录状态
    mtime = file_id = ''
    try:
        logger.debug('获取本地目录的修改时间')
        mtime = str(int(os.path.getmtime(local_path)))
        logger.debug('获取本地目录的修改时间 成功')
    except Exception as err:
        logger.exception('获取本地目录的修改时间 失败, 错误信息为: {err}'.format(err=err))
    root = DirectoryStatus(local_path, mtime=mtime)

    # 遍历目录下的子目录及子文件，并添加到 child 中
    # files_hash_sum = ''
    logger.info('遍历本地目录 {local_path} 下的子目录和子文件，将它们加入当前目录的孩子列表中'.format(local_path=local_path))
    for filename in os.listdir(local_path):
        filename = local_path + filename
        if os.path.isdir(filename):
            # 插入目录
            filename += '/'
            logger.debug('发现子目录 {filename}'.format(filename=filename))
            subdir = initialize_metatree_local(filename, local_dict)
            # files_hash_sum += subdir.file_id
            root.insert(subdir)
            logger.info('将子目录 {filename} 的目录状态插入到当前目录 {local_path}'.format(filename=filename, local_path=local_path))
        elif os.path.isfile(filename):
            # 插入文件
            logger.debug('发现子文件 {filename}'.format(filename=filename))
            mtime = ''
            try:
                logger.debug('获取本地文件的修改时间和文件 ID')
                mtime = str(int(os.path.getmtime(filename)))
                file_id = utils.get_local_file_hash(filename)
                # files_hash_sum += file_id
                logger.debug('获取本地文件的修改时间和文件 ID 成功')
            except Exception as err:
                logger.exception('获取本地文件的修改时间和文件 ID 失败, 错误信息为: {err}'.format(err=err))
            child_file = FileStatus(filename, mtime=mtime, file_id=file_id)
            root.insert(child_file)
            local_dict[child_file.file_id] = local_dict.get(child_file.file_id, set()).union({child_file.filename})
            logger.info('将子文件 {filename} 的文件状态插入到当前目录 {local_path}'.format(filename=filename, local_path=local_path))
            logger.debug('子文件 {filename} 的文件状态为: {child_file}'.format(filename=filename, child_file=child_file))
        else:
            logger.error('未知的的文件: {filename}'.format(filename=filename))

    # 计算目录 hash
    # root.file_id = utils.get_buffer_hash(files_hash_sum.encode())
    logger.info('构建本地当前目录状态 {local_path} 完成'.format(local_path=local_path))
    return root


def initialize_metatree_cloud(cloud_path: str, cfs: CloudFileSystem, cloud_dict: dict):
    """
    初始化云端元信息树
    :param cloud_path: 元信息树的根目录
    :param cfs: 云文件系统，包含 stat_file 等函数
    :param cloud_dict: 用于加速查找的字典
    :return: 以 cloud_path 为根的元信息树
    """
    logger = logging.getLogger('{function_name}'.format(function_name=inspect.stack()[0].function))
    logger.info('构建云端当前目录状态 {cloud_path}'.format(cloud_path=cloud_path))
    # 构建该目录的目录状态
    mtime = file_id = ''
    try:
        logger.debug('获取云端目录的元信息(修改时间)')
        stat = cfs.stat_file(cloud_path)
        mtime = stat['mtime']
        logger.debug('获取云端目录的元信息(修改时间)成功')
    except Exception as err:
        logger.exception('获取云端目录的元信息(修改时间)失败, 错误信息为: {err}'.format(err=err))
    root = DirectoryStatus(cloud_path, mtime=mtime)

    # 遍历目录下的子目录及子文件，并添加到 child 中
    # files_hash_sum = ''
    logger.info('遍历云端目录 {cloud_path} 下的子目录和子文件，将它们加入当前目录的孩子列表中'.format(cloud_path=cloud_path))
    for filename in cfs.list_files(cloud_path):
        filename = cloud_path + filename
        if filename.endswith('/'):
            # 插入目录
            logger.debug('发现子目录 {filename}'.format(filename=filename))
            subdir = initialize_metatree_cloud(filename, cfs, cloud_dict)
            # files_hash_sum += subdir.file_id
            root.insert(subdir)
            logger.info('将子目录 {filename} 的目录状态插入到当前目录 {cloud_path}'.format(filename=filename, cloud_path=cloud_path))
        else:
            # 插入文件
            logger.debug('发现子文件 {filename}'.format(filename=filename))
            mtime = ''
            try:
                logger.debug('获取云端文件的修改时间')
                stat = cfs.stat_file(filename)
                mtime = stat['mtime']
                file_id = stat['hash']
                # files_hash_sum += file_id
                logger.debug('获取云端文件的修改时间成功')
            except Exception as err:
                logger.exception('获取云端文件的修改时间失败, 错误信息为: {err}'.format(err=err))
            child_file = FileStatus(filename, mtime=mtime, file_id=file_id)
            root.insert(child_file)
            cloud_dict[child_file.file_id] = cloud_dict.get(child_file.file_id, set()).union({child_file.filename})
            logger.info('将子文件 {filename} 的文件状态插入到当前目录 {cloud_path}'.format(filename=filename, cloud_path=cloud_path))
            logger.debug('子文件 {filename} 的文件状态为: {child_file}'.format(filename=filename, child_file=child_file))

    # 计算目录 hash
    # root.file_id = utils.get_buffer_hash(files_hash_sum.encode())
    logger.info('构建云端当前目录状态 {cloud_path} 完成'.format(cloud_path=cloud_path))
    return root
