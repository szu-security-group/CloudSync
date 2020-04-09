import os
import time
import qcloud_cos
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
        import cos_config
        self._secret_id = cos_config.tencent['secret_id']
        self._secret_key = cos_config.tencent['secret_key']
        self._region = cos_config.tencent['region']
        self._token = None
        self._bucket_name = cos_config.tencent['bucket_name']
        self._app_id = cos_config.tencent['app_id']
        self._bucket = '{bucket_name}-{app_id}'.format(bucket_name=self._bucket_name, app_id=self._app_id)
        self._config = qcloud_cos.CosConfig(Region=self._region, Token=self._token,
                                            SecretId=self._secret_id, SecretKey=self._secret_key)  # 获取配置对象
        self._client = qcloud_cos.CosS3Client(self._config)

        self.path_config = {
            'history_path': cos_config.tencent['history_path'],
            'local_path': cos_config.tencent['local_path'],
            'cloud_path': cos_config.tencent['cloud_path']
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
                response = self._client.put_object(Bucket=self._bucket,
                                                   Key=cloud_path,
                                                   Body=f,
                                                   Metadata={
                                                       'x-cos-meta-hash': file_hash,
                                                       'x-cos-meta-mtime': file_mtime,
                                                       'x-cos-meta-uuid': file_id
                                                   })

    def download(self, cloud_path, local_path):
        """
        下载文件
        :param cloud_path: 云端文件路径
        :param local_path: 本地文件路径
        :return: None
        """
        temp_local_path = local_path + str(uuid())
        response = self._client.get_object(Bucket=self._bucket, Key=cloud_path)
        # 先下载到临时文件，再替换。避免因为本地文件已存在而导致异常的情况。
        response['Body'].get_stream_to_file(temp_local_path)
        os.replace(temp_local_path, local_path)

    def delete(self, cloud_path):
        """
        删除文件
        :param cloud_path: 云端文件路径
        :return: None
        """
        self._client.delete_object(Bucket=self._bucket, Key=cloud_path)

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
        if cloud_path.endswith('/') or not self._client.object_exists(Bucket=self._bucket, Key=cloud_path):
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
            self._client.put_object(Bucket=self._bucket,
                                    Key=cloud_path,
                                    Body=f,
                                    Metadata={
                                        'x-cos-meta-hash': file_hash,
                                        'x-cos-meta-mtime': file_mtime,
                                        'x-cos-meta-uuid': file_id
                                    })

    def rename(self, old_cloud_path, new_cloud_path):
        """
        重命名文件或目录
        如果是目录，需要对目录下的所有子文件都重命名
        要求重命名完成后，设置最新的修改时间 mtime
        :param old_cloud_path: 重命名前的云端文件路径
        :param new_cloud_path: 重命名后的云端文件路径
        :return: None
        """
        if old_cloud_path.endswith('/'):
            for filename in self.list_files(old_cloud_path):
                self.rename(old_cloud_path + filename, new_cloud_path + filename)

        response = self._client.copy_object(Bucket=self._bucket,
                                            Key=new_cloud_path,
                                            CopySource={
                                                'Appid': self._app_id,
                                                'Bucket': self._bucket_name,
                                                'Key': old_cloud_path,
                                                'Region': self._region
                                            },
                                            CopyStatus='Copy')
        self.delete(old_cloud_path)
        self.set_mtime(cloud_path=new_cloud_path, mtime=str(int(time.time())))

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
        response = self._client.put_object(Bucket=self._bucket,
                                           Key=cloud_path,
                                           Body=b'',
                                           Metadata={
                                               'x-cos-meta-hash': file_hash,
                                               'x-cos-meta-mtime': file_mtime,
                                               'x-cos-meta-uuid': file_id
                                           })

    def list_files(self, cloud_path):
        """
        查询子目录和文件
        要求返回值为子目录名、子文件名所组成的数组
        :param cloud_path: 云端目录路径
        :return: 由子目录名和子文件名组成的数组
        """
        files = []
        items = self._client.list_objects(Bucket=self._bucket, Prefix=cloud_path, Delimiter='/')
        # 添加目录名
        if 'CommonPrefixes' in items.keys():
            dirs = [item['Prefix'] for item in items['CommonPrefixes']]
            files += [filename.split('/')[-2] + '/' for filename in dirs if filename.endswith('/')]
        # 添加文件名
        items = [item['Key'] for item in items['Contents']]
        files += [filename.split('/')[-1] for filename in items if not filename.endswith('/')]
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
            metadata = self._client.head_object(Bucket=self._bucket, Key=cloud_path)
        except qcloud_cos.cos_exception.CosServiceError:
            return None

        # get hash
        if 'x-cos-meta-hash' in metadata.keys():
            hash_value = metadata['x-cos-meta-hash']
        else:
            hash_value = utils.get_cloud_file_hash(cloud_path, self)
            set_stat_flag = True
        # get mtime
        if 'x-cos-meta-mtime' in metadata.keys():
            mtime = metadata['x-cos-meta-mtime']
        else:
            mtime = str(int(time.time()))
            set_stat_flag = True
        # get uuid
        if 'x-cos-meta-uuid' in metadata.keys():
            file_id = metadata['x-cos-meta-uuid']
        else:
            file_id = str(uuid())
            set_stat_flag = True

        # construct stat
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
            'x-cos-meta-hash': stat['hash'],
            'x-cos-meta-mtime': stat['mtime'],
            'x-cos-meta-uuid': stat['uuid'],
        }
        response = self._client.copy_object(Bucket=self._bucket,
                                            Key=cloud_path,
                                            CopySource={
                                                'Appid': self._app_id,
                                                'Bucket': self._bucket_name,
                                                'Key': cloud_path,
                                                'Region': self._region
                                            },
                                            CopyStatus='Replaced',
                                            Metadata=metadata)

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


