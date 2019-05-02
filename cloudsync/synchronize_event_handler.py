import os

from cfs import CloudFileSystem
from cos_config import ops_constants
from synchronize_event_emitter import SynchronizeEventEmitter
from catalog import DirectoryStatus, FileStatus


class SynchronizeEventHandler:
    cfs: CloudFileSystem = ''

    def __init__(self, cfs):
        self.cfs = cfs

    def create_cloud_folder(self, from_path, to_path, directory_status):
        """
        上传到云端一个目录
        :param from_path:
        :param to_path:
        :param directory_status:
        :return: None
        """
        print('Create cloud folder: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        self.cfs.create_folder(to_path)
        # 缓存树新增空子目录
        folder_name = to_path.split('/')[-2] + '/'
        dir_child = DirectoryStatus(folder_name)
        directory_status.insert(dir_child)
        for filename in os.listdir(from_path):
            if os.path.isdir(filename):
                self.create_cloud_folder(from_path + filename + '/', to_path + filename + '/', dir_child)
            else:
                self.upload(from_path + filename, to_path + filename, dir_child)

    def create_local_folder(self, from_path, to_path, directory_status):
        """
        下载到本地一个目录
        :param from_path:
        :param to_path:
        :param directory_status:
        :return: None
        """
        print('Create local folder: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        os.mkdir(to_path)
        folder_name = to_path.split('/')[-2] + '/'
        dir_child = DirectoryStatus(folder_name)
        directory_status.insert(dir_child)
        for filename in self.cfs.list_files(from_path):
            if filename.endswith('/'):
                self.create_local_folder(from_path + filename, to_path + filename, dir_child)
            else:
                self.download(from_path + filename, to_path + filename, dir_child)

    def upload(self, from_path, to_path, directory_status):
        """
        上传文件
        :param from_path:
        :param to_path:
        :param directory_status:
        :return: None
        """
        print('Upload file: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        self.cfs.upload(to_path, from_path)
        filename = to_path.split('/')[-1]
        file_status = FileStatus(filename)
        file_status.mtime = self.cfs.get_mtime(to_path)
        file_status.hash_value = self.cfs.get_hash(to_path)
        directory_status.insert(file_status)

    def delete_cloud_file(self, cloud_path, directory_status):
        """
        删除云端文件
        :param cloud_path:
        :param directory_status:
        :return: None
        """
        print('Delete cloud file: {cloud_path}'
              .format(cloud_path=cloud_path))
        self.cfs.delete(cloud_path)
        filename = cloud_path.split('/')[-1]
        directory_status.remove(filename)

    def delete_local_file(self, local_path, directory_status):
        """
        删除本地文件
        :param local_path:
        :param directory_status:
        :return: None
        """
        print('Delete local file: {local_path}'
              .format(local_path=local_path))
        os.remove(local_path)
        filename = local_path.split('/')[-1]
        directory_status.remove(filename)

    def delete_cloud_folder(self, cloud_path, directory_status):
        """
        删除云端目录
        :param cloud_path:
        :param directory_status:
        :return: None
        """
        print('Delete cloud folder: {cloud_path}'
              .format(cloud_path=cloud_path))
        folder_name = cloud_path.split('/')[-2] + '/'
        dir_child = directory_status.find_catalog(folder_name)
        for filename in self.cfs.list_files(cloud_path):
            if filename.endswith('/'):
                self.delete_cloud_folder(cloud_path + filename, dir_child)
            else:
                directory_status.remove(filename)
                self.cfs.delete(cloud_path + filename)
        directory_status.remove(folder_name)
        self.cfs.delete(cloud_path)

    def delete_local_folder(self, local_path, directory_status):
        """
        删除本地目录
        :param local_path:
        :param directory_status:
        :return: None
        """
        print('Delete local folder: {local_path}'
              .format(local_path=local_path))
        folder_name = local_path.split('/')[-2] + '/'
        dir_child = directory_status.find_catalog(folder_name)
        for filename in os.listdir(local_path):
            if os.path.isdir(filename):
                self.delete_local_folder(local_path + filename + '/', dir_child)
            else:
                dir_child.remove(filename)
                os.remove(filename)
        directory_status.remove(folder_name + '/')
        os.removedirs(folder_name)

    def update_cloud_file(self, from_path, to_path, file_status):
        """
        更新云文件
        :param from_path:
        :param to_path:
        :param file_status:
        :return: None
        """
        print('Update cloud file: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        self.cfs.update(to_path, from_path)
        file_status.mtime = self.cfs.get_mtime(to_path)
        file_status.hash_value = self.cfs.get_hash(to_path)

    def update_local_file(self, from_path, to_path, directory_status):
        """
        更新本地文件 == 下载文件 todo
        :param from_path:
        :param to_path:
        :param directory_status:
        :return: None
        """
        print('Update local file: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        self.cfs.download(from_path, to_path)
        filename = to_path.split('/')[-1]
        file_status = directory_status.find_catalog(filename)
        if file_status is not None:
            file_status.mtime = self.cfs.get_mtime(from_path)
            file_status.hash_value = self.cfs.get_hash(from_path)
        else:
            file_status = FileStatus(filename)
            file_status.mtime = self.cfs.get_mtime(from_path)
            file_status.hash_value = self.cfs.get_hash(from_path)
            directory_status.insert(file_status)

    def download(self, from_path, to_path, directory_status):
        """
        下载文件 == 更新本地文件 todo
        :param from_path:
        :param to_path:
        :param directory_status:
        :return: None
        """
        print('Download file: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        self.cfs.download(from_path, to_path)
        filename = to_path.split('/')[-1]
        file_status = directory_status.find_catalog(filename)
        if file_status is not None:
            file_status.mtime = self.cfs.get_mtime(from_path)
            file_status.hash_value = self.cfs.get_hash(from_path)
        else:
            file_status = FileStatus(filename)
            file_status.mtime = self.cfs.get_mtime(from_path)
            file_status.hash_value = self.cfs.get_hash(from_path)
            directory_status.insert(file_status)

    def rename_cloud_file(self, from_path, to_path, file_status):
        """
        重命名云端文件
        :param from_path:
        :param to_path:
        :param file_status:
        :return: None
        """
        print('Rename cloud file: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        self.cfs.rename(from_path, to_path)
        file_status.filename = to_path.split('/')[-1]
        file_status.mtime = self.cfs.get_mtime(to_path)

    def rename_local_file(self, from_path, to_path, file_status):
        """
        重命名本地文件
        :param from_path:
        :param to_path:
        :param file_status:
        :return: None
        """
        print('Rename local file: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        os.rename(from_path, to_path)
        file_status.filename = to_path.split('/')[-1]
        file_status.mtime = str(int(os.path.getmtime(to_path)))

    def rename_cloud_folder(self, from_path, to_path, directory_status):
        """
        重命名云端目录
        :param from_path:
        :param to_path:
        :param directory_status:
        :return: None
        """
        print('Rename cloud folder: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        self.cfs.rename(from_path, to_path)
        filename = to_path.split('/')[-2] + '/'
        directory_status.filename = filename

    def rename_local_folder(self, from_path, to_path, directory_status):
        """
        重命名本地目录
        :param from_path:
        :param to_path:
        :param directory_status:
        :return: None
        """
        print('Rename local folder: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        os.rename(from_path, to_path)
        filename = to_path.split('/')[-2] + '/'
        directory_status.filename = filename

    def update(self, observable: SynchronizeEventEmitter):
        task_index = observable.task_index
        from_path = observable.from_path
        to_path = observable.to_path
        directory_list = observable.directory_list
        stat_file = observable.stat_file

        if task_index == ops_constants['CREATE_CLOUD_FOLDER']:
            self.create_cloud_folder(from_path, to_path, directory_list)
        elif task_index == ops_constants['CREATE_LOCAL_FOLDER']:
            self.create_local_folder(from_path, to_path, directory_list)
        elif task_index == ops_constants['UPLOAD_FILE']:
            self.upload(from_path, to_path, directory_list)
        elif task_index == ops_constants['DELETE_CLOUD_FILE']:
            self.delete_cloud_file(from_path, directory_list)
        elif task_index == ops_constants['DELETE_LOCAL_FILE']:
            self.delete_local_file(from_path, directory_list)
        elif task_index == ops_constants['DELETE_CLOUD_FOLDER']:
            self.delete_cloud_folder(from_path, directory_list)
        elif task_index == ops_constants['DELETE_LOCAL_FOLDER']:
            self.delete_local_folder(from_path, directory_list)
        elif task_index == ops_constants['UPDATE_CLOUD_FILE']:
            self.update_cloud_file(from_path, to_path, stat_file)
        elif task_index == ops_constants['UPDATE_LOCAL_FILE']:
            self.update_local_file(from_path, to_path, directory_list)
        elif task_index == ops_constants['RENAME_CLOUD_FILE']:
            self.rename_cloud_file(from_path, to_path, stat_file)
        elif task_index == ops_constants['RENAME_LOCAL_FILE']:
            self.rename_local_file(from_path, to_path, stat_file)
        elif task_index == ops_constants['DOWNLOAD_FILE']:
            self.download(from_path, to_path, directory_list)
        elif task_index == ops_constants['RENAME_CLOUD_FOLDER']:
            self.rename_cloud_folder(from_path, to_path, directory_list)
        elif task_index == ops_constants['RENAME_LOCAL_FOLDER']:
            self.rename_local_folder(from_path, to_path, directory_list)
        else:
            print('Unknown task index!')
