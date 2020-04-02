# 文件操作都对应常量标志
from enum import Enum, unique


@unique
class OP(Enum):
    # 创建云目录
    CREATE_CLOUD_FOLDER = 1
    # 创建本地目录
    CREATE_LOCAL_FOLDER = 2
    # 上传文件
    UPLOAD_FILE = 3
    # 删除云文件
    DELETE_CLOUD_FILE = 4
    # 删除本地文件
    DELETE_LOCAL_FILE = 5
    # 删除云目录
    DELETE_CLOUD_FOLDER = 6
    # 删除本地目录
    DELETE_LOCAL_FOLDER = 7
    # 更新云文件
    UPDATE_CLOUD_FILE = 8
    # 更新本地文件
    UPDATE_LOCAL_FILE = 9
    # 重命名云文件
    RENAME_CLOUD_FILE = 10
    # 重命名本地文件
    RENAME_LOCAL_FILE = 11
    # 下载文件
    DOWNLOAD_FILE = 12
    # 重命名云端目录
    RENAME_CLOUD_FOLDER = 13
    # 重命名本地目录
    RENAME_LOCAL_FOLDER = 14


# Ali properties
ali = {
    # client properties
    'access_key_id': '',
    'access_key_secret': '',
    'endpoint': '',
    'bucket_name': '',
    # synchronization properties
    'history_path': 'C:/cloudsync/ali.history',
    'local_path': 'C:/cloudsync/cloudsync-ali/',
    'cloud_path': 'cloudsync/'
}

# Tencent properties
tencent = {
    # client properties
    'app_id': '',
    'secret_id': '',
    'secret_key': '',
    'region': '',
    'bucket_name': '',
    # synchronization properties
    'history_path': 'C:/cloudsync/tencent.history',
    'local_path': 'C:/cloudsync/cloudsync-tencent/',
    'cloud_path': 'cloudsync/'
}
