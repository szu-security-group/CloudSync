import time
import oss2
from uuid import uuid1 as uuid

import utils


class CloudFileSystem:
    """
    云操作方法汇总：
    云端如按要求实现此接口，就可以直接进行导入同步功能类
    """

    def __init__(self):
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
        要求附加自定义属性修改时间 mtime 、摘要 hash
        :param cloud_path:
        :param local_path:
        :return:
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
        return response

    def download(self, cloud_path, local_path):
        """
        下载文件
        如果下载的文件在云端没有附加属性摘要 hash ，要求计算后附加上去
        :param cloud_path:
        :param local_path:
        :return:
        """
        import shutil
        response = self._client.get_object(key=cloud_path)
        # 写入本地文件
        with open(local_path, 'wb') as f:
            shutil.copyfileobj(response, f)
        return response

    def delete(self, cloud_path):
        """
        删除文件
        :param cloud_path:
        :return:
        """
        response = self._client.delete_object(key=cloud_path)
        return response

    def update(self, cloud_path, local_path):
        """
        上传文件
        要求附加自定义属性修改时间 mtime 、摘要 hash
        :param cloud_path:
        :param local_path:
        :return:
        """
        import os
        if cloud_path.endswith('/') or not self._client.object_exists(key=cloud_path):
            return

        # get file metadata
        file_size = file_hash = ''
        try:
            file_size = str(os.path.getsize(local_path))
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
                                                   'Content-Length': file_size,
                                                   'x-oss-meta-hash': file_hash,
                                                   'x-oss-meta-mtime': file_mtime,
                                                   'x-oss-meta-uuid': file_id
                                               })
        return response

    def rename(self, old_cloud_path: str, new_cloud_path: str):
        """
        重命名文件或目录
        要求重命名完成后，设置最新的修改时间 mtime
        :param old_cloud_path:
        :param new_cloud_path:
        :return:
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
        return response

    def create_folder(self, cloud_path: str):
        """
        创建一个空目录
        要求附加最新修改时间 mtime
        :param cloud_path:
        :return:
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
        return response

    def list_files(self, cloud_path):
        """
        查询子目录和文件
        要求返回值为子文件名、子目录名所组成的链表
        :param cloud_path:
        :return:
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
        查询文件属性
        要求返回值中，key 至少包括 hash、mtime、uuid
        :param cloud_path:
        :return:
        """
        set_stat_flag = False
        metadata = self._client.head_object(key=cloud_path)
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
        修改文件属性
        :param cloud_path: 云端文件路径
        :param stat: 文件熟悉
        :return:
        """
        metadata = {
            'x-oss-meta-hash': stat['hash'],
            'x-oss-meta-mtime': stat['mtime'],
            'x-oss-meta-uuid': stat['uuid'],
        }
        response = self._client.copy_object(source_bucket_name=self._bucket_name,
                                            source_key=cloud_path,
                                            target_key=cloud_path,
                                            headers=metadata)
        return response

    def set_hash(self, cloud_path, hash_value):
        """
        设置文件摘要 hash
        :param cloud_path:
        :param hash_value:
        :return:
        """
        stat = self.stat_file(cloud_path)
        stat['hash'] = hash_value
        response = self.set_stat(cloud_path, stat)
        return response

    def get_hash(self, cloud_path):
        """
        获取文件摘要 hash
        如果不存在，则读取云文件并计算 hash 值，作为属性附加值附加到云属性中
        :param cloud_path:
        :return:
        """
        return self.stat_file(cloud_path)['hash']

    def set_mtime(self, cloud_path, mtime):
        """
        设置最近修改时间，单位为毫秒
        :param cloud_path:
        :param mtime:
        :return:
        """
        stat = self.stat_file(cloud_path)
        stat['mtime'] = mtime
        response = self.set_stat(cloud_path, stat)
        return response

    def get_mtime(self, cloud_path):
        """
        获取文件/目录的最近修改时间
        如果不存在，则将当前时间作为属性附加值附加到云属性中
        :param cloud_path:
        :return:
        """
        return self.stat_file(cloud_path)['mtime']
