import os
import time
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
            hash=self.hash_value[6:]
        )


class DirectoryStatus(Catalog):
    def __init__(self, filename, hash_value=None, mtime=None, file_id=None, child=None):
        filename += '/' if not filename.endswith('/') else ''
        super().__init__(filename, Catalog.IS_FOLDER, hash_value, mtime, file_id)
        self.child = child if child is not None else SortedList([])

    def insert(self, catalog):
        if catalog not in self.child:
            self.child.add(catalog)

    def remove(self, filename):
        if isinstance(filename, Catalog):
            filename = filename.filename
        filename = str(filename)
        for catalog in self.child.__iter__():
            if catalog.filename == filename:
                self.child.remove(catalog)

    def find_catalog(self, obj, number=1):
        obj = str(obj)
        for catalog in self.child.__iter__():
            if obj in [catalog.filename, catalog.hash_value, catalog.file_id]:
                number -= 1
            if number == 0:
                return catalog
        return None

    def __str__(self):
        return '[DirectoryStatus: filename={filename} file_id={file_id} hash={hash}]'.format(
            filename=self.filename,
            file_id=self.file_id,
            hash=self.hash_value[6:]
        )


class FileStatus(Catalog):
    def __init__(self, filename, hash_value=None, mtime=None, file_id=None):
        super().__init__(filename, Catalog.IS_FILE, hash_value, mtime, file_id)

    def __str__(self):
        return '[FileStatus: filename={filename} mtime={mtime} hash={hash}]'.format(
            filename=self.filename,
            mtime=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(self.mtime))),
            hash=self.hash_value[6:]
        )


def initialize_metatree_local(local_path):
    # 构建该目录的目录状态
    mtime = file_id = ''
    try:
        mtime = str(int(os.path.getmtime(local_path)))
        file_id = str(os.stat(local_path).st_ino)
    except Exception as e:
        print(e)
    root = DirectoryStatus(local_path, mtime=mtime, file_id=file_id)

    # 遍历目录下的子目录及子文件，并添加到 child 中
    for filename in os.listdir(local_path):
        filename = local_path + filename
        if os.path.isdir(filename):
            # 插入目录
            filename += '/'
            subdir = initialize_metatree_local(filename)
            root.insert(subdir)
        elif os.path.isfile(filename):
            # 插入文件
            mtime = ''
            try:
                mtime = str(int(os.path.getmtime(filename)))
            except Exception as e:
                print(e)
            subfile = FileStatus(filename, mtime=mtime)
            root.insert(subfile)
        else:
            print('Unsupported File!')
    return root


def initialize_metatree_cloud(cloud_path, cfs):
    # 构建该目录的目录状态
    mtime = file_id = ''
    try:
        stat = cfs.stat_file(cloud_path)
        mtime = stat['mtime']
        file_id = stat['uuid']
    except Exception as e:
        print(e)
    root = DirectoryStatus(cloud_path, mtime=mtime, file_id=file_id)

    # 遍历目录下的子目录及子文件，并添加到 child 中
    for filename in cfs.list_files(cloud_path):
        if filename.endswith('/'):
            # 插入目录
            subdir = initialize_metatree_cloud(cloud_path + filename, cfs)
            root.insert(subdir)
        else:
            # 插入文件
            mtime = ''
            try:
                mtime = cfs.get_mtime(cloud_path + filename)
            except Exception as e:
                print(e)
            subfile = FileStatus(filename, mtime=mtime)
            root.insert(subfile)
    return root
