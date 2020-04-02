import os
import time
import logging
import inspect
from sortedcontainers import SortedList


class Catalog:
    # 文件或目录的标志值
    IS_FOLDER = 1
    IS_FILE = 2

    # 文件目录名
    filename = ''
    # 文件目录标志
    file_type = 0
    # 文件哈希值
    hash_value = ''
    # 文件修改时间
    mtime = ''
    # 文件的 inode 或 UUID
    file_id = ''

    def __init__(self, filename, file_type, hash_value, mtime, file_id):
        self.filename = filename
        self.file_type = file_type
        self.hash_value = hash_value if hash_value is not None else ''
        self.mtime = mtime if mtime is not None else ''
        self.file_id = file_id if file_id is not None else ''

    def __eq__(self, other):
        return self.filename == str(other.filename)

    def __lt__(self, other):
        return self.filename < str(other.filename)

    def __str__(self):
        return '[Catalog: filename={filename} hash={hash}]'.format(
            filename=self.filename,
            hash=self.hash_value[:6]
        )


class DirectoryStatus(Catalog):
    """
    存储文件夹元信息的结构
    """
    def __init__(self, filename, hash_value=None, mtime=None, file_id=None, child=None):
        filename += '/' if not filename.endswith('/') else ''
        super().__init__(filename, Catalog.IS_FOLDER, hash_value, mtime, file_id)
        self.child = child if child is not None else SortedList([])

    def insert(self, catalog):
        """
        将文件(夹)结构插入该文件夹的子文件列表中
        :param catalog: 文件(夹)结构
        :return: None
        """
        if catalog not in self.child:
            self.child.add(catalog)

    def remove(self, filename):
        """
        将文件(夹)结构从该文件夹的子文件列表中移除
        :param filename: 文件(夹)结构 或 文件(夹)的名字
        :return: None
        """
        if isinstance(filename, Catalog):
            filename = filename.filename
        filename = str(filename)
        for catalog in self.child.__iter__():
            if catalog.filename == filename:
                self.child.remove(catalog)

    def find_catalog(self, obj):
        """
        根据 obj 在子文件列表中查找文件(夹)
        :param obj: 子文件(夹)的 文件名 或 散列值 或 文件ID
        :return: 文件(夹)结构
        """
        obj = str(obj)
        for catalog in self.child.__iter__():
            if obj in [catalog.filename, catalog.hash_value, catalog.file_id]:
                return catalog
        return None

    def __str__(self):
        return '[DirectoryStatus: filename={filename} file_id={file_id} hash={hash}]'.format(
            filename=self.filename,
            file_id=self.file_id,
            hash=self.hash_value[:6]
        )


class FileStatus(Catalog):
    """
    存储文件元信息的结构
    """
    def __init__(self, filename, hash_value=None, mtime=None, file_id=None):
        super().__init__(filename, Catalog.IS_FILE, hash_value, mtime, file_id)

    def __str__(self):
        return '[FileStatus: filename={filename} mtime={mtime} file_id={file_id} hash={hash}]'.format(
            filename=self.filename,
            mtime=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(self.mtime))),
            file_id=self.file_id,
            hash=self.hash_value[:6]
        )


def initialize_metatree_local(local_path):
    """
    初始化本地元信息树
    :param local_path: 元信息树的根目录
    :return: 以 local_path 为根的元信息树
    """
    logger = logging.getLogger('{function_name}'.format(function_name=inspect.stack()[0].function))
    logger.info('构建以 {local_path} 为根的目录状态(本地元信息树)'.format(local_path=local_path))
    # 构建该目录的目录状态
    mtime = file_id = ''
    try:
        logger.debug('获取本地目录的修改时间和文件 ID')
        mtime = str(int(os.path.getmtime(local_path)))
        file_id = str(os.stat(local_path).st_ino)
        logger.debug('获取本地目录的修改时间和文件 ID 成功')
    except Exception as err:
        logger.exception('获取本地目录的修改时间和文件 ID 失败, 错误信息为: {err}'.format(err=err))
    root = DirectoryStatus(local_path, mtime=mtime, file_id=file_id)
    logger.info('创建本地当前目录状态 {root}'.format(root=root))

    # 遍历目录下的子目录及子文件，并添加到 child 中
    logger.info('遍历本地目录 {local_path} 下的子目录和子文件，将它们加入当前目录的孩子列表中'.format(local_path=local_path))
    for filename in os.listdir(local_path):
        filename = local_path + filename
        if os.path.isdir(filename):
            # 插入目录
            filename += '/'
            logger.info('发现子目录 {filename}'.format(filename=filename))
            subdir = initialize_metatree_local(filename)
            root.insert(subdir)
            logger.info('将子目录 {filename} 的目录状态插入到当前目录 {local_path}'.format(filename=filename, local_path=local_path))
        elif os.path.isfile(filename):
            # 插入文件
            logger.info('发现子文件 {filename}'.format(filename=filename))
            mtime = ''
            try:
                logger.debug('获取本地文件的修改时间')
                mtime = str(int(os.path.getmtime(filename)))
                file_id = str(os.stat(filename).st_ino)
                logger.debug('获取本地文件的修改时间成功')
            except Exception as err:
                logger.exception('获取本地文件的修改时间失败, 错误信息为: {err}'.format(err=err))
            subfile = FileStatus(filename, mtime=mtime, file_id=file_id)
            root.insert(subfile)
            logger.info('将子文件 {filename} 的文件状态插入到当前目录 {local_path}'.format(filename=filename, local_path=local_path))
            logger.debug('子文件 {filename} 的文件状态为: {subfile}'.format(filename=filename, subfile=subfile))
        else:
            logger.error('未知的的文件: {filename}'.format(filename=filename))
    logger.info('构建以 {local_path} 为根的目录状态(本地元信息树) 完成'.format(local_path=local_path))
    return root


def initialize_metatree_cloud(cloud_path, cfs):
    """
    初始化云端元信息树
    :param cloud_path: 元信息树的根目录
    :param cfs: 云文件系统，包含 stat_file 等函数
    :return: 以 cloud_path 为根的元信息树
    """
    logger = logging.getLogger('{function_name}'.format(function_name=inspect.stack()[0].function))
    logger.info('构建以 {cloud_path} 为根的目录状态(云端元信息树)'.format(cloud_path=cloud_path))
    # 构建该目录的目录状态
    mtime = file_id = ''
    try:
        logger.debug('获取云端目录的元信息(修改时间和文件 ID )')
        stat = cfs.stat_file(cloud_path)
        mtime = stat['mtime']
        file_id = stat['uuid']
        logger.debug('获取云端目录的元信息(修改时间和文件 ID )成功')
    except Exception as err:
        logger.exception('获取云端目录的元信息(修改时间和文件 ID )失败, 错误信息为: {err}'.format(err=err))
    root = DirectoryStatus(cloud_path, mtime=mtime, file_id=file_id)
    logger.info('创建云端当前目录状态 {root}'.format(root=root))

    # 遍历目录下的子目录及子文件，并添加到 child 中
    logger.info('遍历云端目录 {cloud_path} 下的子目录和子文件，将它们加入当前目录的孩子列表中'.format(cloud_path=cloud_path))
    for filename in cfs.list_files(cloud_path):
        filename = cloud_path + filename
        if filename.endswith('/'):
            # 插入目录
            logger.info('发现子目录 {filename}'.format(filename=filename))
            subdir = initialize_metatree_cloud(filename, cfs)
            root.insert(subdir)
            logger.info('将子目录 {filename} 的目录状态插入到当前目录 {cloud_path}'.format(filename=filename, cloud_path=cloud_path))
        else:
            # 插入文件
            logger.info('发现子文件 {filename}'.format(filename=filename))
            mtime = ''
            try:
                logger.debug('获取云端文件的修改时间')
                stat = cfs.stat_file(filename)
                mtime = stat['mtime']
                file_id = stat['uuid']
                logger.debug('获取云端文件的修改时间成功')
            except Exception as err:
                logger.exception('获取云端文件的修改时间失败, 错误信息为: {err}'.format(err=err))
            subfile = FileStatus(filename, mtime=mtime, file_id=file_id)
            root.insert(subfile)
            logger.info('将子文件 {filename} 的文件状态插入到当前目录 {cloud_path}'.format(filename=filename, cloud_path=cloud_path))
            logger.debug('子文件 {filename} 的文件状态为: {subfile}'.format(filename=filename, subfile=subfile))
    logger.info('构建以 {cloud_path} 为根的目录状态(云端元信息树) 完成'.format(cloud_path=cloud_path))
    return root
