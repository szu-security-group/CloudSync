import os
import logging
import inspect

from global_value import OP, hash_table_local, hash_table_cloud
from synchronize_event_emitter import SynchronizeEventEmitter


class SynchronizeEventHandler:
    def __init__(self, cfs):
        self.cfs = cfs
        self.from_path = ''
        self.to_path = ''
        self.kwargs = {}

    def create_cloud_folder(self, from_path=None, to_path=None):
        """
        上传本地目录到云端
        :param from_path: 本地目录路径
        :param to_path: 云端目录路径
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        from_path = self.from_path if from_path is None else from_path
        to_path = self.to_path if to_path is None else to_path
        logger.info('准备将本地文件夹 {from_path} 上传到云端文件夹 {to_path}'.format(from_path=from_path, to_path=to_path))

        # check if folders exist
        if not os.path.exists(from_path):
            logger.warning('本地不存在文件夹 {from_path}，操作中止'.format(from_path=from_path))
            return
        if self.cfs.stat_file(to_path) is not None:
            logger.warning('云端已存在文件夹 {to_path}，操作中止'.format(to_path=to_path))
            return

        # create folder
        self.cfs.create_folder(to_path)
        logger.info('已创建云端文件夹 {to_path}'.format(to_path=to_path))
        logger.info('遍历本地文件夹 {from_path} 中的内容，递归上传'.format(from_path=from_path))
        for filename in os.listdir(from_path):
            filename = from_path + filename
            if os.path.isdir(filename):
                logger.info('发现本地文件夹 {filename}'.format(filename=filename + '/'))
                self.create_cloud_folder(filename + '/', to_path + filename[len(from_path):] + '/')
            else:
                logger.info('发现本地文件 {filename}'.format(filename=filename))
                self.upload(filename, to_path + filename[len(from_path):])
        logger.info('上传本地文件夹 {from_path} 到云端文件夹 {to_path} 完成'.format(from_path=from_path, to_path=to_path))

    def create_local_folder(self, from_path=None, to_path=None):
        """
        下载云端目录到本地
        :param from_path: 云端目录路径
        :param to_path: 本地目录路径
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        from_path = self.from_path if from_path is None else from_path
        to_path = self.to_path if to_path is None else to_path
        logger.info('准备将云端文件夹 {from_path} 下载到本地文件夹 {to_path}'.format(from_path=from_path, to_path=to_path))

        # check if folders exist
        if self.cfs.stat_file(from_path) is None:
            logger.warning('云端不存在文件夹 {from_path}，操作中止'.format(from_path=from_path))
            return
        if os.path.exists(to_path):
            logger.warning('本地已存在文件夹 {to_path}，操作中止'.format(to_path=to_path))
            return

        # create folder
        os.mkdir(to_path)
        logger.info('已创建本地文件夹 {to_path}'.format(to_path=to_path))
        logger.info('遍历云端文件夹 {from_path} 中的内容，递归下载'.format(from_path=from_path))
        for filename in self.cfs.list_files(from_path):
            if filename.endswith('/'):
                logger.info('发现云端文件夹 {from_path}{filename}'.format(from_path=from_path, filename=filename))
                self.create_local_folder(from_path + filename, to_path + filename)
            else:
                logger.info('发现云端文件 {from_path}{filename}'.format(from_path=from_path, filename=filename))
                self.download(from_path + filename, to_path + filename)
        logger.info('下载云端文件夹 {from_path} 到本地文件夹 {to_path} 完成'.format(from_path=from_path, to_path=to_path))

    def upload(self, from_path=None, to_path=None):
        """
        上传本地文件到云端
        :param from_path: 本地文件路径
        :param to_path: 云端文件路径
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        from_path = self.from_path if from_path is None else from_path
        to_path = self.to_path if to_path is None else to_path

        # check if files exist
        if not os.path.exists(from_path):
            logger.warning('本地不存在文件 {from_path}，操作中止'.format(from_path=from_path))
            return
        if self.cfs.stat_file(to_path) is not None:
            logger.warning('云端已存在文件 {to_path}，操作中止'.format(to_path=to_path))
            return

        if 'file_id' in self.kwargs:
            records = hash_table_cloud.get(self.kwargs['file_id'], [])
            if len(records) > 0:
                logger.info('发现云端存在相同 file_id 的文件，准备复制')
                for record in records:
                    try:
                        self.cfs.copy(record, to_path)
                        logger.info('复制成功')
                        return
                    except Exception as e:
                        print(e)
                        logger.info('复制失败')
                        pass

        # upload file
        logger.info('准备将本地文件 {from_path} 上传到云端文件 {to_path}'.format(from_path=from_path, to_path=to_path))
        self.cfs.upload(to_path, from_path)
        logger.info('上传本地文件 {from_path} 到云端文件 {to_path} 完成'.format(from_path=from_path, to_path=to_path))

    def download(self, from_path=None, to_path=None):
        """
        下载云端文件到本地
        :param from_path: 云端文件路径
        :param to_path: 本地文件路径
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        from_path = self.from_path if from_path is None else from_path
        to_path = self.to_path if to_path is None else to_path

        # check if files exist
        if self.cfs.stat_file(from_path) is None:
            logger.warning('云端不存在文件 {from_path}，操作中止'.format(from_path=from_path))
            return
        if os.path.exists(to_path):
            logger.warning('本地已存在文件 {to_path}，操作中止'.format(to_path=to_path))
            return

        if 'file_id' in self.kwargs:
            records = hash_table_local.get(self.kwargs['file_id'], [])
            if len(records) > 0:
                logger.info('发现本地存在相同 file_id 的文件，准备复制')
                for record in records:
                    try:
                        os.rename(record, to_path)
                        logger.info('复制成功')
                        return
                    except Exception as e:
                        print(e)
                        logger.info('复制失败')
                        pass

        # download file
        logger.info('准备将云端文件 {from_path} 下载到本地文件 {to_path}'.format(from_path=from_path, to_path=to_path))
        self.cfs.download(from_path, to_path)
        logger.info('下载云端文件 {from_path} 到本地文件 {to_path} 完成'.format(from_path=from_path, to_path=to_path))

    def delete_cloud_file(self):
        """
        删除云端文件
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        cloud_path = self.from_path

        # check if the cloud file exists
        if self.cfs.stat_file(cloud_path) is None:
            logger.warning('云端不存在文件 {cloud_path}，操作中止'.format(cloud_path=cloud_path))
            return

        # delete file
        logger.info('准备删除云端文件 {cloud_path}'.format(cloud_path=cloud_path))
        self.cfs.delete(cloud_path)
        logger.info('删除云端文件 {cloud_path} 完成'.format(cloud_path=cloud_path))

    def delete_local_file(self):
        """
        删除本地文件
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        local_path = self.from_path

        # check if the local file exists
        if not os.path.exists(local_path):
            logger.warning('本地不存在文件 {local_path}，操作中止'.format(local_path=local_path))
            return

        # delete file
        logger.info('准备删除本地文件 {local_path}'.format(local_path=local_path))
        os.remove(local_path)
        logger.info('删除本地文件 {local_path} 完成'.format(local_path=local_path))

    def delete_cloud_folder(self, cloud_path=None):
        """
        删除云端目录
        :param cloud_path: 云端目录路径
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        cloud_path = self.from_path if cloud_path is None else cloud_path

        # check if the cloud folder exists
        if self.cfs.stat_file(cloud_path) is None:
            logger.warning('云端不存在文件夹 {cloud_path}，操作中止'.format(cloud_path=cloud_path))
            return

        # delete folder
        logger.info('准备删除云端文件夹 {cloud_path}'.format(cloud_path=cloud_path))
        logger.info('遍历云端文件夹 {cloud_path} 中的内容，递归删除'.format(cloud_path=cloud_path))
        for filename in self.cfs.list_files(cloud_path):
            if filename.endswith('/'):
                logger.info('发现云端文件夹 {cloud_path}{filename}'.format(cloud_path=cloud_path, filename=filename))
                self.delete_cloud_folder(cloud_path + filename)
            else:
                logger.info('发现云端文件 {cloud_path}{filename}'.format(cloud_path=cloud_path, filename=filename))
                self.cfs.delete(cloud_path + filename)
        self.cfs.delete(cloud_path)
        logger.info('删除云端文件夹 {cloud_path} 完成'.format(cloud_path=cloud_path))

    def delete_local_folder(self, local_path=None):
        """
        删除本地目录
        :param local_path: 本地目录路径
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        local_path = self.from_path if local_path is None else local_path

        # check if the local folder exists
        if not os.path.exists(local_path):
            logger.warning('本地不存在文件夹 {local_path}，操作中止'.format(local_path=local_path))
            return

        # delete folder
        logger.info('准备删除本地文件夹 {local_path}'.format(local_path=local_path))
        logger.info('遍历本地文件夹 {local_path} 中的内容，递归删除'.format(local_path=local_path))
        for filename in os.listdir(local_path):
            filename = local_path + filename
            if os.path.isdir(filename):
                logger.info('发现本地文件夹 {filename}'.format(filename=filename + '/'))
                self.delete_local_folder(filename + '/')
            else:
                logger.info('发现本地文件 {filename}'.format(filename=filename))
                os.remove(filename)
        os.rmdir(local_path)
        logger.info('删除本地文件夹 {local_path} 完成'.format(local_path=local_path))

    def update_cloud_file(self):
        """
        更新云文件
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        from_path = self.from_path
        to_path = self.to_path

        if 'file_id' in self.kwargs:
            records = hash_table_cloud.get(self.kwargs['file_id'], [])
            if len(records) > 0:
                logger.info('发现云端存在相同 file_id 的文件，准备复制')
                for record in records:
                    try:
                        self.cfs.copy(record, to_path)
                        logger.info('复制成功')
                        return
                    except Exception as e:
                        print(e)
                        logger.info('复制失败')
                        pass

        logger.info('准备将本地文件 {from_path} 上传到云端文件 {to_path}'.format(from_path=from_path, to_path=to_path))
        self.cfs.update(to_path, from_path)
        logger.info('上传本地文件 {from_path} 到云端文件 {to_path} 完成'.format(from_path=from_path, to_path=to_path))

    def update_local_file(self):
        """
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        from_path = self.from_path
        to_path = self.to_path

        if 'file_id' in self.kwargs:
            records = hash_table_local.get(self.kwargs['file_id'], [])
            if len(records) > 0:
                logger.info('发现本地存在相同 file_id 的文件，准备复制')
                for record in records:
                    try:
                        os.rename(record, to_path)
                        logger.info('复制成功')
                        return
                    except Exception as e:
                        print(e)
                        logger.info('复制失败')
                        pass

        logger.info('准备将云端文件 {from_path} 下载到本地文件 {to_path}'.format(from_path=from_path, to_path=to_path))
        self.cfs.download(from_path, to_path)
        logger.info('下载云端文件 {from_path} 到本地文件 {to_path} 完成'.format(from_path=from_path, to_path=to_path))

    def rename_cloud_file(self):
        """
        重命名云端文件
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        from_path = self.from_path
        to_path = self.to_path

        # check if the cloud files exist
        if self.cfs.stat_file(from_path) is None:
            logger.warning('云端不存在文件 {from_path}，操作中止'.format(from_path=from_path))
            return
        if self.cfs.stat_file(to_path) is not None:
            logger.warning('云端已存在文件 {to_path}，操作中止'.format(to_path=to_path))
            return

        # rename file
        logger.info('准备将云端文件 {from_path} 重命名为 {to_path}'.format(from_path=from_path, to_path=to_path))
        self.cfs.rename(from_path, to_path)
        logger.info('重命名云端文件 {from_path} 为 {to_path} 成功'.format(from_path=from_path, to_path=to_path))

    def rename_local_file(self):
        """
        重命名本地文件
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        from_path = self.from_path
        to_path = self.to_path

        # check if the local files exist
        if not os.path.exists(from_path):
            logger.warning('本地不存在文件 {from_path}，操作中止'.format(from_path=from_path))
            return
        if os.path.exists(to_path):
            logger.warning('本地已存在文件 {to_path}，操作中止'.format(to_path=to_path))
            return

        # rename file
        logger.info('准备将本地文件 {from_path} 重命名为 {to_path}'.format(from_path=from_path, to_path=to_path))
        os.rename(from_path, to_path)
        logger.info('重命名本地文件 {from_path} 为 {to_path} 成功'.format(from_path=from_path, to_path=to_path))

    def rename_cloud_folder(self):
        """
        重命名云端目录
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        from_path = self.from_path
        to_path = self.to_path

        # check if the cloud folders exist
        if self.cfs.stat_file(from_path) is None:
            logger.warning('云端不存在文件夹 {from_path}，操作中止'.format(from_path=from_path))
            return
        if self.cfs.stat_file(to_path) is not None:
            logger.warning('云端已存在文件夹 {to_path}，操作中止'.format(to_path=to_path))
            return

        # rename folder
        logger.info('准备将云端文件夹 {from_path} 重命名为 {to_path}'.format(from_path=from_path, to_path=to_path))
        self.cfs.rename(from_path, to_path)
        logger.info('重命名云端文件夹 {from_path} 为 {to_path} 成功'.format(from_path=from_path, to_path=to_path))

    def rename_local_folder(self):
        """
        重命名本地目录
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        from_path = self.from_path
        to_path = self.to_path

        # check if the local folders exist
        if not os.path.exists(from_path):
            logger.warning('本地不存在文件夹 {from_path}，操作中止'.format(from_path=from_path))
            return
        if os.path.exists(to_path):
            logger.warning('本地已存在文件夹 {to_path}，操作中止'.format(to_path=to_path))
            return

        # rename folder
        logger.info('准备将本地文件夹 {from_path} 重命名为 {to_path}'.format(from_path=from_path, to_path=to_path))
        os.rename(from_path, to_path)
        logger.info('重命名本地文件夹 {from_path} 为 {to_path} 成功'.format(from_path=from_path, to_path=to_path))

    def update(self, observable: SynchronizeEventEmitter):
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))

        task_index = observable.task_index
        self.from_path = observable.from_path
        self.to_path = observable.to_path
        self.kwargs = observable.kwargs

        logger.debug('任务编号: {task_index}, 参数列表: [\'{from_path}\', \'{to_path}\'], 将执行编号对应操作'
                     .format(task_index=task_index, from_path=self.from_path, to_path=self.to_path))

        {
            OP.CREATE_CLOUD_FOLDER: self.create_cloud_folder,
            OP.CREATE_LOCAL_FOLDER: self.create_local_folder,
            OP.UPLOAD_FILE: self.upload,
            OP.DELETE_CLOUD_FILE: self.delete_cloud_file,
            OP.DELETE_LOCAL_FILE: self.delete_local_file,
            OP.DELETE_CLOUD_FOLDER: self.delete_cloud_folder,
            OP.DELETE_LOCAL_FOLDER: self.delete_local_folder,
            OP.UPDATE_CLOUD_FILE: self.update_cloud_file,
            OP.UPDATE_LOCAL_FILE: self.update_local_file,
            OP.RENAME_CLOUD_FILE: self.rename_cloud_file,
            OP.RENAME_LOCAL_FILE: self.rename_local_file,
            OP.DOWNLOAD_FILE: self.download,
            OP.RENAME_CLOUD_FOLDER: self.rename_cloud_folder,
            OP.RENAME_LOCAL_FOLDER: self.rename_local_folder
        }[task_index]()
