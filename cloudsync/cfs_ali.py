import os
import time
import oss2

import cos_config
import utils

"""
云操作方法汇总：
云端如按要求实现此接口，就可以直接进行导入同步功能类
"""

access_key_id = cos_config.ali['access_key_id']
access_key_secret = cos_config.ali['access_key_secret']
endpoint = cos_config.ali['endpoint']
auth = oss2.Auth(access_key_id, access_key_secret)  # 获取配置对象
bucket = oss2.Bucket(auth, endpoint, cos_config.ali['bucket_name'])
client = bucket

path_config = {
    'history_path': cos_config.ali['history_path'],
    'local_path': cos_config.ali['local_path'],
    'cloud_path': cos_config.ali['cloud_path']
}


def upload(cloud_path, local_path):
    """
    上传文件
    要求附加自定义属性修改时间 mtime 、摘要 hash
    :param cloud_path:
    :param local_path:
    :return:
    """
    if local_path.endswith('/'):
        response = client.put_object(key=cloud_path,
                                     data=b'',
                                     headers={
                                         'x-oss-meta-mtime': str(int(time.time()))
                                     })
    else:
        file_hash = utils.get_local_file_hash(local_path)
        with open(local_path, 'rb') as f:
            response = client.put_object(key=cloud_path,
                                         data=f,
                                         headers={
                                             'x-oss-meta-mtime': str(int(time.time())),
                                             'x-pss-meta-hash': file_hash
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
    import shutil
    response = client.get_object(key=cloud_path)
    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)
    if 'x-pss-meta-hash' not in response.headers:
        set_hash(cloud_path, utils.get_local_file_hash(local_path))


def delete(cloud_path):
    """
    删除文件
    :param cloud_path:
    :return:
    """
    client.delete_object(key=cloud_path)


def update(cloud_path, local_path):
    """
    上传文件
    要求附加自定义属性修改时间 mtime 、摘要 hash
    :param cloud_path:
    :param local_path:
    :return:
    """
    if cloud_path.endswith('/') or not client.object_exists(key=cloud_path):
        return
    file_hash = utils.get_local_file_hash(local_path)
    with open(local_path, 'rb') as f:
        client.put_object(key=cloud_path,
                          data=f,
                          headers={
                              'Content-Length': os.path.getsize(local_path),
                              'x-oss-meta-mtime': str(int(time.time())),
                              'x-pss-meta-hash': file_hash
                          })


def rename(old_cloud_path: str, new_cloud_path: str):
    """
    重命名文件或目录
    要求重命名完成后，设置最新的修改时间 mtime
    :param old_cloud_path:
    :param new_cloud_path:
    :return:
    """
    if not old_cloud_path.endswith('/'):
        response = client.copy_object(source_bucket_name=cos_config.ali['bucket_name'],
                                      source_key=old_cloud_path,
                                      target_key=new_cloud_path)
    else:
        for filename in list_files(old_cloud_path):
            rename(old_cloud_path + filename, new_cloud_path + filename)
        response = client.copy_object(source_bucket_name=cos_config.ali['bucket_name'],
                                      source_key=old_cloud_path,
                                      target_key=new_cloud_path)

    delete(old_cloud_path)
    set_mtime(cloud_path=new_cloud_path, mtime=str(int(time.time())))
    return response


def create_folder(cloud_path: str):
    """
    创建一个空目录
    要求附加最新修改时间 mtime
    :param cloud_path:
    :return:
    """
    # cloud_path 结尾若不是 / , 上传之后不会表现为文件夹
    if not cloud_path.endswith('/'):
        cloud_path += '/'
    response = client.put_object(key=cloud_path,
                                 data=b'',
                                 headers={
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
    metadata = client.head_object(key=cloud_path)
    return {
        'hash': metadata['x-oss-meta-hash'] if 'x-oss-meta-hash' in metadata.headers else get_hash(cloud_path),
        'mtime': metadata['x-oss-meta-mtime'] if 'x-oss-meta-mtime' in metadata.headers else get_mtime(cloud_path)
    }


def list_files(cloud_path):
    """
    查询子目录和文件
    要求返回值为子文件名、子目录名所组成的链表
    :param cloud_path:
    :return:
    """
    files = []
    items = oss2.ObjectIterator(bucket, prefix=cloud_path, delimiter='/')
    items = [item for item in items]
    # 添加目录名
    files += [item.key.split('/')[-2] + '/' for item in items if item.is_prefix()]
    # 添加文件名
    files += [item.key.split('/')[-1] for item in items if not item.key.endswith('/')]
    return files


def set_hash(cloud_path, hash_value):
    """
    设置文件摘要 hash
    :param cloud_path:
    :param hash_value:
    :return:
    """
    metadata = client.head_object(key=cloud_path)
    metadata = {key: value for key, value in metadata.headers if key.startswith('x-oss-meta-')}
    metadata['x-oss-meta-hash'] = str(hash_value)
    response = client.copy_object(source_bucket_name=cos_config.ali['bucket_name'],
                                  source_key=cloud_path,
                                  target_key=cloud_path,
                                  headers=metadata)
    return response


def get_hash(cloud_path):
    """
    获取文件摘要 hash
    要求如果文件属性附加值不存在 hash 时，读取云文件并计算 hash 值，作为属性 hash 附加到云文件中，并返回此 hash 值
    :param cloud_path:
    :return:
    """
    metadata = client.head_object(key=cloud_path)
    if 'x-oss-meta-hash' in metadata.headers:
        hash_value = metadata.headers['x-oss-meta-hash']
    else:
        # todo: 这里可能会重复计算hash: utils.get_cloud_file_hash -> cfs_ali.download -> set_hash
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
    metadata = client.head_object(key=cloud_path)
    metadata = {key: value for key, value in metadata.headers if key.startswith('x-oss-meta-')}
    metadata['x-oss-meta-mtime'] = str(mtime)
    response = client.copy_object(source_bucket_name=cos_config.ali['bucket_name'],
                                  source_key=cloud_path,
                                  target_key=cloud_path,
                                  headers=metadata)
    return response


def get_mtime(cloud_path):
    """
    获取文件/目录的最近修改时间
    如果不存在，则将当前时间作为属性附加值附加到云属性中
    :param cloud_path:
    :return:
    """
    metadata = client.head_object(key=cloud_path)
    if 'x-oss-meta-mtime' in metadata.headers:
        mtime = metadata.headers['x-oss-meta-mtime']
    else:
        mtime = str(int(time.time()))
        set_mtime(cloud_path, mtime)
    return mtime
