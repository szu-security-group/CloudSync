import os

from catalog import DirectoryStatus, FileStatus


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
