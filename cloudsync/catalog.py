from sortedcontainers import SortedSet


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
        if child is None:
            super().__init__(filename)
            self.type = Catalog.IS_FOLDER
            self.child = SortedSet([])
        else:
            super().__init__(filename)
            self.child = child

    def insert(self, catalog):
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
        if hash_value is not None:
            self.hash_value = hash_value
        if mtime is not None:
            self.mtime = mtime

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
