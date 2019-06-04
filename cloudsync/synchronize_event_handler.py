import os

from cos_config import ops_constants
from synchronize_event_emitter import SynchronizeEventEmitter


class SynchronizeEventHandler:
    def __init__(self, cfs):
        self.cfs = cfs
        self.from_path = ''
        self.to_path = ''

    def create_cloud_folder(self, from_path=None, to_path=None):
        """
        上传到云端一个目录
        :param from_path:
        :param to_path:
        :return: None
        """
        from_path = self.from_path if from_path is None else from_path
        to_path = self.to_path if to_path is None else to_path
        print('Create cloud folder: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        self.cfs.create_folder(to_path)
        for filename in os.listdir(from_path):
            if os.path.isdir(filename):
                self.create_cloud_folder(from_path + filename + '/', to_path + filename + '/')
            else:
                self.upload(from_path + filename, to_path + filename)

    def create_local_folder(self, from_path=None, to_path=None):
        """
        下载到本地一个目录
        :param from_path:
        :param to_path:
        :return: None
        """
        from_path = self.from_path if from_path is None else from_path
        to_path = self.to_path if to_path is None else to_path
        print('Create local folder: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        os.mkdir(to_path)
        for filename in self.cfs.list_files(from_path):
            if filename.endswith('/'):
                self.create_local_folder(from_path + filename, to_path + filename)
            else:
                self.download(from_path + filename, to_path + filename)

    def upload(self, from_path=None, to_path=None):
        """
        上传文件
        :param from_path:
        :param to_path:
        :return: None
        """
        from_path = self.from_path if from_path is None else from_path
        to_path = self.to_path if to_path is None else to_path
        print('Upload file: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        self.cfs.upload(to_path, from_path)

    def delete_cloud_file(self):
        """
        删除云端文件
        :return: None
        """
        cloud_path = self.from_path
        print('Delete cloud file: {cloud_path}'
              .format(cloud_path=cloud_path))
        self.cfs.delete(cloud_path)

    def delete_local_file(self):
        """
        删除本地文件
        :return: None
        """
        local_path = self.from_path
        print('Delete local file: {local_path}'
              .format(local_path=local_path))
        os.remove(local_path)

    def delete_cloud_folder(self, cloud_path=None):
        """
        删除云端目录
        :param cloud_path:
        :return: None
        """
        cloud_path = self.from_path if cloud_path is None else cloud_path
        print('Delete cloud folder: {cloud_path}'
              .format(cloud_path=cloud_path))
        for filename in self.cfs.list_files(cloud_path):
            if filename.endswith('/'):
                self.delete_cloud_folder(cloud_path + filename)
            else:
                self.cfs.delete(cloud_path + filename)
        self.cfs.delete(cloud_path)

    def delete_local_folder(self, local_path=None):
        """
        删除本地目录
        :param local_path:
        :return: None
        """
        local_path = self.from_path if local_path is None else local_path
        print('Delete local folder: {local_path}'
              .format(local_path=local_path))
        folder_name = local_path.split('/')[-2] + '/'
        for filename in os.listdir(local_path):
            if os.path.isdir(filename):
                self.delete_local_folder(local_path + filename + '/')
            else:
                os.remove(filename)
        os.removedirs(folder_name)

    def update_cloud_file(self):
        """
        更新云文件
        :return: None
        """
        from_path = self.from_path
        to_path = self.to_path
        print('Update cloud file: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        self.cfs.update(to_path, from_path)

    def update_local_file(self):
        """
        更新本地文件 == 下载文件 hack
        :return: None
        """
        from_path = self.from_path
        to_path = self.to_path
        print('Update local file: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        self.cfs.download(from_path, to_path)

    def download(self, from_path=None, to_path=None):
        """
        下载文件 == 更新本地文件 hack
        :param from_path:
        :param to_path:
        :return: None
        """
        from_path = self.from_path if from_path is None else from_path
        to_path = self.to_path if to_path is None else to_path
        print('Download file: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        self.cfs.download(from_path, to_path)

    def rename_cloud_file(self):
        """
        重命名云端文件
        :return: None
        """
        from_path = self.from_path
        to_path = self.to_path
        print('Rename cloud file: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        self.cfs.rename(from_path, to_path)

    def rename_local_file(self):
        """
        重命名本地文件
        :return: None
        """
        from_path = self.from_path
        to_path = self.to_path
        print('Rename local file: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        os.rename(from_path, to_path)

    def rename_cloud_folder(self):
        """
        重命名云端目录
        :return: None
        """
        from_path = self.from_path
        to_path = self.to_path
        print('Rename cloud folder: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        self.cfs.rename(from_path, to_path)

    def rename_local_folder(self):
        """
        重命名本地目录
        :return: None
        """
        from_path = self.from_path
        to_path = self.to_path
        print('Rename local folder: from {from_path} to {to_path}'
              .format(from_path=from_path, to_path=to_path))
        os.rename(from_path, to_path)

    def update(self, observable: SynchronizeEventEmitter):
        task_index = observable.task_index
        self.from_path = observable.from_path
        self.to_path = observable.to_path

        if task_index == ops_constants['CREATE_CLOUD_FOLDER']:
            self.create_cloud_folder()
        elif task_index == ops_constants['CREATE_LOCAL_FOLDER']:
            self.create_local_folder()
        elif task_index == ops_constants['UPLOAD_FILE']:
            self.upload()
        elif task_index == ops_constants['DELETE_CLOUD_FILE']:
            self.delete_cloud_file()
        elif task_index == ops_constants['DELETE_LOCAL_FILE']:
            self.delete_local_file()
        elif task_index == ops_constants['DELETE_CLOUD_FOLDER']:
            self.delete_cloud_folder()
        elif task_index == ops_constants['DELETE_LOCAL_FOLDER']:
            self.delete_local_folder()
        elif task_index == ops_constants['UPDATE_CLOUD_FILE']:
            self.update_cloud_file()
        elif task_index == ops_constants['UPDATE_LOCAL_FILE']:
            self.update_local_file()
        elif task_index == ops_constants['RENAME_CLOUD_FILE']:
            self.rename_cloud_file()
        elif task_index == ops_constants['RENAME_LOCAL_FILE']:
            self.rename_local_file()
        elif task_index == ops_constants['DOWNLOAD_FILE']:
            self.download()
        elif task_index == ops_constants['RENAME_CLOUD_FOLDER']:
            self.rename_cloud_folder()
        elif task_index == ops_constants['RENAME_LOCAL_FOLDER']:
            self.rename_local_folder()
        else:
            print('Unknown task index!')
