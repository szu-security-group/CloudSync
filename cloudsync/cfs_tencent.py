import os
import time
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client

import cos_config
import utils

"""
云操作方法汇总：
云端如按要求实现此接口，就可以直接进行导入同步功能类
"""

secret_id = cos_config.tencent['secret_id']
secret_key = cos_config.tencent['secret_key']
region = cos_config.tencent['region']
token = None
config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token)  # 获取配置对象
client = CosS3Client(config)

bucket = '{bucket_name}-{app_id}'.format(bucket_name=cos_config.tencent['bucket_name'],
                                         app_id=cos_config.tencent['app_id'])


# local_path = cos_config.tencent['local_path']
# cloud_path = cos_config.tencent['cloud_path']
# history_path = cos_config.tencent['history_path']


def upload(cloud_path, local_path):
    """
    上传文件
    要求附加自定义属性修改时间 mtime 、摘要 hash
    :param cloud_path:
    :param local_path:
    :return:
    """
    if local_path.endswith('/'):
        response = client.put_object(Bucket=bucket,
                                     Key=cloud_path,
                                     Body=b'',
                                     Metadata={
                                         'x-cos-meta-mtime': str(int(time.time()))
                                     })
    else:
        file_hash = utils.get_local_file_hash(local_path)
        with open(local_path, 'rb') as f:
            response = client.put_object(Bucket=bucket,
                                         Key=cloud_path,
                                         Body=f,
                                         ContentLength=str(os.path.getsize(local_path)),
                                         Metadata={
                                             'x-cos-meta-mtime': str(int(time.time())),
                                             'x-cos-meta-hash': file_hash
                                         })
    return response


def download(cloud_path, local_path):
    """
    下载文件
    如果下载的文件在云端没有附加属性摘要 hash ，要求计算后附加上去
    :param cloud_path:
    :param local_path:
    :return:
    """
    response = client.get_object(Bucket=bucket, Key=cloud_path)
    response['Body'].get_stream_to_file(local_path)
    if 'x-cos-meta-hash' not in response.keys():
        set_hash(cloud_path, utils.get_local_file_hash(local_path))


def delete(cloud_path):
    """
    删除文件
    :param cloud_path:
    :return:
    """
    client.delete_object(Bucket=bucket,
                         Key=cloud_path)


def update(cloud_path, local_path):
    """
    上传文件
    要求附加自定义属性修改时间 mtime 、摘要 hash
    :param cloud_path:
    :param local_path:
    :return:
    """
    if cloud_path.endswith('/') or not client.object_exists(Bucket=bucket, Key=cloud_path):
        return
    file_hash = utils.get_local_file_hash(local_path)
    with open(local_path, 'rb') as f:
        client.put_object(Bucket=bucket,
                          Key=cloud_path,
                          Body=f,
                          ContentLength=os.path.getsize(local_path),
                          Metadata={
                              'x-cos-meta-mtime': str(int(time.time())),
                              'x-cos-meta-hash': file_hash
                          })


def rename(old_cloud_path, new_cloud_path):
    """
    重命名文件或目录
    要求重命名完成后，设置最新的修改时间 mtime
    :param old_cloud_path:
    :param new_cloud_path:
    :return:
    """
    if not old_cloud_path.endswith('/'):
        response = client.copy_object(Bucket=bucket,
                                      Key=new_cloud_path,
                                      CopySource={
                                          'Appid': cos_config.tencent['app_id'],
                                          'Bucket': cos_config.tencent['bucket_name'],
                                          'Key': old_cloud_path,
                                          'Region': cos_config.tencent['region']
                                      },
                                      CopyStatus='Copy')
        delete(old_cloud_path)
        set_mtime(cloud_path=new_cloud_path, mtime=str(int(time.time())))
        return response
    else:
        for filename in list_files(old_cloud_path):
            rename(old_cloud_path + filename, new_cloud_path + filename)
        response = client.copy_object(Bucket=bucket,
                                      Key=new_cloud_path,
                                      CopySource={
                                          'Appid': cos_config.tencent['app_id'],
                                          'Bucket': cos_config.tencent['bucket_name'],
                                          'Key': old_cloud_path,
                                          'Region': cos_config.tencent['region']
                                      },
                                      CopyStatus='Copy')
        delete(old_cloud_path)
        set_mtime(cloud_path=new_cloud_path, mtime=str(int(time.time())))
        return response


def create_folder(cloud_path):
    """
    创建一个空目录
    要求附加最新修改时间 mtime
    :param cloud_path:
    :return:
    """
    # todo: 应当加入校验 cloud_path 结尾是不是 / , 否则上传之后不会表现为文件夹
    response = client.put_object(Bucket=bucket,
                                 Key=cloud_path,
                                 Body=b'',
                                 ContentLength='0',
                                 Metadata={
                                     'x-cos-meta-mtime': str(int(time.time()))
                                 })
    return response


def stat_file(cloud_path):
    """
    查询文件属性
    要求返回值中，key 至少包括 hash、mtime
    :param cloud_path:
    :return:
    """
    metadata = client.head_object(Bucket=bucket, Key=cloud_path)
    # todo: 我反而觉得这里需要加入检验 key 是否存在
    return {
        'hash': metadata['x-cos-meta-hash'],
        'mtime': metadata['x-cos-meta-mtime']
    }


def list_files(cloud_path):
    """
    查询子目录和文件
    要求返回值为子文件名、子目录名所组成的链表
    :param cloud_path:
    :return:
    """
    files = []
    listing = client.list_objects(Bucket=bucket, Prefix=cloud_path, Delimiter='/')
    # 添加目录名
    if 'CommonPrefixes' in listing.keys():
        dirs = [item['Prefix'] for item in listing['CommonPrefixes']]
        files += [filename.split('/')[-2] + '/' for filename in dirs if filename.endswith('/')]
    # 添加文件名 todo: 改个变量名
    listing = [item['Key'] for item in listing['Contents']]
    files += [filename.split('/')[-1] for filename in listing if not filename.endswith('/')]
    return files


def set_hash(cloud_path, hash_value):
    """
    设置文件摘要 hash
    :param cloud_path:
    :param hash_value:
    :return:
    """
    metadata = client.head_object(Bucket=bucket, Key=cloud_path)
    metadata = {key: value for key, value in metadata.items() if key.startswith('x-cos-meta-')}
    metadata['x-cos-meta-hash'] = str(hash_value)
    response = client.copy_object(Bucket=bucket,
                                  Key=cloud_path,
                                  CopySource={
                                      'Appid': cos_config.tencent['app_id'],
                                      'Bucket': cos_config.tencent['bucket_name'],
                                      'Key': cloud_path,
                                      'Region': cos_config.tencent['region']
                                  },
                                  CopyStatus='Replaced',
                                  Metadata=metadata)
    return response


def get_hash(cloud_path):
    """
    获取文件摘要 hash
    要求如果文件属性附加值不存在 hash 时，读取云文件并计算 hash 值，作为属性 hash 附加到云文件中，并返回此 hash 值
    :param cloud_path:
    :return:
    """
    metadata = client.head_object(Bucket=bucket, Key=cloud_path)
    if 'x-cos-meta-hash' in metadata.keys():
        hash_value = metadata['x-cos-meta-hash']
    else:
        # todo: 这里可能会重复计算hash: utils.get_cloud_file_hash -> cfs_tencent.download -> set_hash
        hash_value = utils.get_cloud_file_hash(cloud_path, download)
        set_hash(cloud_path, hash_value)
    return hash_value


def set_mtime(cloud_path, mtime):
    """
    设置最近修改时间
    一般为 new Date().getTime()/1000
    :param cloud_path:
    :param mtime:
    :return:
    """
    metadata = client.head_object(Bucket=bucket, Key=cloud_path)
    metadata = {key: value for key, value in metadata.items() if key.startswith('x-cos-meta-')}
    metadata['x-cos-meta-mtime'] = str(mtime)
    response = client.copy_object(Bucket=bucket,
                                  Key=cloud_path,
                                  CopySource={
                                      'Appid': cos_config.tencent['app_id'],
                                      'Bucket': cos_config.tencent['bucket_name'],
                                      'Key': cloud_path,
                                      'Region': cos_config.tencent['region']
                                  },
                                  CopyStatus='Replaced',
                                  Metadata=metadata)
    return response


def get_mtime(cloud_path):
    """
    获取文件/目录的最近修改时间
    如果不存在，则将当前时间作为属性附加值附加到云属性中
    :param cloud_path:
    :return:
    """
    metadata = client.head_object(Bucket=bucket, Key=cloud_path)
    if 'x-cos-meta-mtime' in metadata.keys():
        mtime = metadata['x-cos-meta-mtime']
    else:
        mtime = str(int(time.time()))
        set_mtime(cloud_path, mtime)
    return mtime