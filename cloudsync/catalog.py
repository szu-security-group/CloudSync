import os
from sortedcontainers import SortedList


class Catalog:
    # 文件或目录的标志值
    IS_FOLDER = 1
    IS_FILE = 2

    # 文件目录名
    filename = ''
    # 文件哈希值
    hash_value = ''
    # 文件修改时间
    mtime = ''
    # 文件目录标志
    type = 0

    def __init__(self, filename):
        self.filename = filename

    def __eq__(self, other):
        return self.filename == str(other.filename)

    def __lt__(self, other):
        return self.filename < str(other.filename)

    def __str__(self):
        return '[Catalog: filename={filename} hash={hash}]'.format(
            filename=self.filename,
            hash=self.hash_value
        )


class DirectoryStatus(Catalog):
    def __init__(self, filename, child=None):
        filename += '/' if not filename.endswith('/') else ''
        super().__init__(filename)
        self.type = Catalog.IS_FOLDER
        if child is None:
            self.child = SortedList([])
        else:
            self.child = child

    def insert(self, catalog):
        if catalog not in self.child:
            self.child.add(catalog)

    def remove(self, object_):
        if isinstance(object_, str):
            for obj in self.child.__iter__():
                if obj.filename == str(object_):
                    self.child.remove(obj)
        if isinstance(object_, Catalog):
            self.child.remove(object_)

    def find_catalog(self, object_, number=1):
        if isinstance(object_, str):
            for obj in self.child.__iter__():
                if obj.filename == str(object_) or obj.hash_value == str(object_):
                    number -= 1
                    if number == 0:
                        return obj
        return None

    @staticmethod
    def find_catalog_by_path(path: str, root):
        paths = path.split('/')
        for i in range(0, len(paths) - 1):
            root = root.find_catalog(paths[i] + '/')
        if path.endswith('/'):
            return root.find_catalog(paths[len(paths) - 1] + '/')
        else:
            return root.find_catalog(paths[len(paths) - 1])

    @staticmethod
    def find_last_directory_by_path(path, root):
        paths = path.split('/')
        for i in range(1, len(paths) - 1):
            root = root.find_catalog(paths[i] + '/')
        return root

    def __str__(self):
        return '[DirectoryStatus: child={child} filename={filename} hash={hash}'.format(
            child=self.child,
            filename=self.filename,
            hash=self.hash_value
        )


class FileStatus(Catalog):
    def __init__(self, filename, hash_value=None, mtime=None):
        super().__init__(filename)
        self.type = self.IS_FILE
        self.hash_value = hash_value if hash_value is not None else ''
        self.mtime = mtime if mtime is not None else ''

    def copy(self, file_status):
        self.filename = file_status.filename
        self.hash_value = file_status.hash_value
        self.mtime = file_status.mtime

    def __str__(self):
        return '[FileStatus: filename={filename} hash={hash} mtime={mtime}'.format(
            filename=self.filename,
            hash=self.hash_value,
            mtime=self.mtime
        )


def initialize_metatree_local(local_path: str):
    root = DirectoryStatus(local_path)
    for filename in os.listdir(local_path):
        if os.path.isdir(filename):
            # 插入目录
            subdir = DirectoryStatus(filename + '/')
            subdir.mtime = str(int(os.path.getmtime(filename)))
            root.insert(subdir)
            # 递归
            initialize_metatree_local(local_path + filename + '/')
        elif os.path.isfile(filename):
            # 插入文件
            file = FileStatus(filename)
            file.mtime = str(int(os.path.getmtime(filename)))
            root.insert(file)
        else:
            # todo
            print('Unsupported File!')
    return root


def initialize_metatree_cloud(cloud_path: str, cloud_list_files_function, cloud_get_mtime_function):
    root = DirectoryStatus(cloud_path)
    try:
        for filename in cloud_list_files_function(cloud_path):
            if filename.endswith('/'):
                # 插入目录
                subdir = DirectoryStatus(filename)
                subdir.mtime = cloud_get_mtime_function(cloud_path + filename)
                root.insert(subdir)
                # 递归
                initialize_metatree_cloud(cloud_path + filename, cloud_list_files_function, cloud_get_mtime_function)
            else:
                # 插入文件
                file = FileStatus(filename)
                file.mtime = cloud_get_mtime_function(cloud_path + filename)
                root.insert(file)
    except Exception as e:
        print('Something happened...')
        print(e)
    finally:
        return root
