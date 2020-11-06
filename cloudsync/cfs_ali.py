import time
import logging
import oss2
from uuid import uuid1 as uuid

import utils


class CloudFileSystem:
    """
    云操作方法汇总：
    云端如按要求实现此接口，就可以直接进行导入同步功能类
    """

    def __init__(self):
        """
        初始化云文件系统
        需要初始化COS客户端，以及指定存储痛
        """
        oss2.set_stream_logger(level=logging.WARNING)
        oss2.set_file_logger(file_path='cloudsync.log', level=logging.WARNING)

        import cos_config
        self._access_key_id = cos_config.ali['access_key_id']
        self._access_key_secret = cos_config.ali['access_key_secret']
        self._endpoint = cos_config.ali['endpoint']
        self._bucket_name = cos_config.ali['bucket_name']
        self._auth = oss2.Auth(self._access_key_id, self._access_key_secret)
        self._client = oss2.Bucket(self._auth, self._endpoint, self._bucket_name)

        self.path_config = {
            'history_path': cos_config.ali['history_path'],
            'local_path': cos_config.ali['local_path'],
            'cloud_path': cos_config.ali['cloud_path']
        }

    def upload(self, cloud_path, local_path):
        """
        上传文件
        要求附加自定义属性修改时间 mtime 、摘要 hash 和 文件ID
        - 修改时间 mtime 使用当前的时间
        - 摘要 hash 使用选定算法对文件内容计算散列值而得
        - 文件ID 使用 uuid.uuid1() 函数生成
        :param cloud_path: 云端文件路径
        :param local_path: 本地文件路径
        :return: None
        """
        if local_path.endswith('/'):
            response = self.create_folder(cloud_path)
        else:
            # get file metadata
            file_hash = ''
            try:
                file_hash = utils.get_local_file_hash(local_path)
            except Exception as e:
                print(e)
            file_mtime = str(int(time.time()))
            file_id = str(uuid())

            # upload file
            with open(local_path, 'rb') as f:
                response = self._client.put_object(key=cloud_path,
                                                   data=f,
                                                   headers={
                                                       'x-oss-meta-hash': file_hash,
                                                       'x-oss-meta-mtime': file_mtime,
                                                       'x-oss-meta-uuid': file_id
                                                   })

    def download(self, cloud_path, local_path):
        """
        下载文件
        :param cloud_path: 云端文件路径
        :param local_path: 本地文件路径
        :return: None
        """
        import shutil
        response = self._client.get_object(key=cloud_path)
        # 写入本地文件
        with open(local_path, 'wb') as f:
            shutil.copyfileobj(response, f)

    def delete(self, cloud_path):
        """
        删除文件
        :param cloud_path: 云端文件路径
        :return: None
        """
        self._client.delete_object(key=cloud_path)

    def update(self, cloud_path, local_path):
        """
        使用本地文件的内容更新云端文件的内容
        要求附加自定义属性修改时间 mtime 、摘要 hash 和 文件ID
        - 修改时间 mtime 使用当前的时间
        - 摘要 hash 使用选定算法对新本地文件内容计算散列值而得
        - 文件ID 使用被更新的云端文件的文件ID
        :param cloud_path: 云端文件路径
        :param local_path: 本地文件路径
        :return: None
        """
        import os
        if cloud_path.endswith('/') or not self._client.object_exists(key=cloud_path):
            return

        # get file metadata
        file_hash = ''
        try:
            file_hash = utils.get_local_file_hash(local_path)
        except Exception as e:
            print(e)
        file_mtime = str(int(time.time()))
        file_id = self.stat_file(cloud_path)['uuid']

        # upload file
        with open(local_path, 'rb') as f:
            response = self._client.put_object(key=cloud_path,
                                               data=f,
                                               headers={
                                                   'x-oss-meta-hash': file_hash,
                                                   'x-oss-meta-mtime': file_mtime,
                                                   'x-oss-meta-uuid': file_id
                                               })

    def rename(self, old_cloud_path: str, new_cloud_path: str):
        """
        重命名文件或目录
        如果是目录，需要对目录下的所有子文件都重命名
        要求重命名完成后，设置最新的修改时间 mtime
        :param old_cloud_path: 重命名前的云端文件路径
        :param new_cloud_path: 重命名后的云端文件路径
        :return: None
        """
        if old_cloud_path.endswith('/'):
            # 对目录下的文件进行递归重命名
            for filename in self.list_files(old_cloud_path):
                self.rename(old_cloud_path + filename, new_cloud_path + filename)
            response = self._client.copy_object(source_bucket_name=self._bucket_name,
                                                source_key=old_cloud_path,
                                                target_key=new_cloud_path)
        else:
            response = self._client.copy_object(source_bucket_name=self._bucket_name,
                                                source_key=old_cloud_path,
                                                target_key=new_cloud_path)

        self.delete(old_cloud_path)
        self.set_mtime(cloud_path=new_cloud_path, mtime=str(int(time.time())))

    def copy(self, src_path: str, dist_path: str):
        """
        复制文件
        :param src_path: 复制的源文件
        :param dist_path: 复制的目标文件
        :return: None
        """
        self._client.copy_object(source_bucket_name=self._bucket_name,
                                 source_key=src_path,
                                 target_key=dist_path)
        self.set_mtime(cloud_path=dist_path, mtime=str(int(time.time())))

    def create_folder(self, cloud_path: str):
        """
        创建一个空目录
        要求附加最新修改时间 mtime, 散列值 hash 和 文件ID
        - 修改时间 mtime 使用当前的时间
        - 摘要 hash 使用选定算法对空字符串计算散列值而得
        - 文件ID 使用被更新的云端文件的文件ID
        :param cloud_path: 云端目录路径
        :return: None
        """
        # cloud_path 结尾若不是 / , 上传之后不会表现为文件夹
        if not cloud_path.endswith('/'):
            cloud_path += '/'

        # get file metadata
        file_hash = utils.get_buffer_hash(b'')
        file_mtime = str(int(time.time()))
        file_id = str(uuid())

        # create folder
        response = self._client.put_object(key=cloud_path,
                                           data=b'',
                                           headers={
                                               'x-oss-meta-hash': file_hash,
                                               'x-oss-meta-mtime': file_mtime,
                                               'x-oss-meta-uuid': file_id
                                           })

    def list_files(self, cloud_path):
        """
        查询子目录和文件
        要求返回值为子目录名、子文件名所组成的数组
        :param cloud_path: 云端目录路径
        :return: 由子目录名和子文件名组成的数组
        """
        files = []
        items = oss2.ObjectIterator(self._client, prefix=cloud_path, delimiter='/')
        items = [item for item in items]
        # 添加目录名
        files += [item.key.split('/')[-2] + '/' for item in items if item.is_prefix()]
        # 添加文件名
        files += [item.key.split('/')[-1] for item in items if not item.key.endswith('/')]
        return files

    def stat_file(self, cloud_path):
        """
        查询并返回文件元信息
        若文件不存在，则返回 None
        若文件元信息存在空值，则设置该文件的此项的元信息
        :param cloud_path: 云端文件路径
        :return: None 或 含有文件元信息（包括 hash、mtime、uuid）的字典
        """
        set_stat_flag = False
        try:
            metadata = self._client.head_object(key=cloud_path)
        except oss2.exceptions.NotFound:
            return None

        # get hash
        if 'x-oss-meta-hash' in metadata.headers:
            hash_value = metadata.headers['x-oss-meta-hash']
        else:
            hash_value = utils.get_cloud_file_hash(cloud_path, self)
            set_stat_flag = True
        # get mtime
        if 'x-oss-meta-mtime' in metadata.headers:
            mtime = metadata.headers['x-oss-meta-mtime']
        else:
            mtime = str(int(time.time()))
            set_stat_flag = True
        # get uuid
        if 'x-oss-meta-uuid' in metadata.headers:
            file_id = metadata.headers['x-oss-meta-uuid']
        else:
            file_id = str(uuid())
            set_stat_flag = True

        stat = {
            'hash': hash_value,
            'mtime': mtime,
            'uuid': file_id
        }
        if set_stat_flag:
            self.set_stat(cloud_path, stat)
        return stat

    def set_stat(self, cloud_path, stat):
        """
        修改文件元信息
        :param cloud_path: 云端文件路径
        :param stat: 文件元信息
        :return: None
        """
        metadata = {
            'x-oss-meta-hash': stat['hash'],
            'x-oss-meta-mtime': stat['mtime'],
            'x-oss-meta-uuid': stat['uuid'],
        }
        self._client.copy_object(source_bucket_name=self._bucket_name,
                                 source_key=cloud_path,
                                 target_key=cloud_path,
                                 headers=metadata)

    def set_hash(self, cloud_path, hash_value):
        """
        设置文件摘要 hash
        :param cloud_path: 云端文件路径
        :param hash_value: 文件内容摘要
        :return: None
        """
        stat = self.stat_file(cloud_path)
        stat['hash'] = hash_value
        self.set_stat(cloud_path, stat)

    def set_mtime(self, cloud_path, mtime):
        """
        设置最近修改时间，单位为毫秒
        :param cloud_path: 云端文件路径
        :param mtime: 修改时间
        :return: None
        """
        stat = self.stat_file(cloud_path)
        stat['mtime'] = mtime
        self.set_stat(cloud_path, stat)
