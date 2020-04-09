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
        pass

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
        pass

    def download(self, cloud_path, local_path):
        """
        下载文件
        :param cloud_path: 云端文件路径
        :param local_path: 本地文件路径
        :return: None
        """
        pass

    def delete(self, cloud_path):
        """
        删除文件
        :param cloud_path: 云端文件路径
        :return: None
        """
        pass

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
        pass

    def rename(self, old_cloud_path: str, new_cloud_path: str):
        """
        重命名文件或目录
        如果是目录，需要对目录下的所有子文件都重命名
        要求重命名完成后，设置最新的修改时间 mtime
        :param old_cloud_path: 重命名前的云端文件路径
        :param new_cloud_path: 重命名后的云端文件路径
        :return: None
        """
        pass

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
        pass

    def list_files(self, cloud_path):
        """
        查询子目录和文件
        要求返回值为子目录名、子文件名所组成的数组
        :param cloud_path: 云端目录路径
        :return: 由子目录名和子文件名组成的数组
        """
        pass

    def stat_file(self, cloud_path):
        """
        查询并返回文件元信息
        若文件不存在，则返回 None
        若文件元信息存在空值，则设置该文件的此项的元信息
        :param cloud_path: 云端文件路径
        :return: None 或 含有文件元信息（包括 hash、mtime、uuid）的字典
        """
        pass

    def set_stat(self, cloud_path, stat):
        """
        修改文件元信息
        :param cloud_path: 云端文件路径
        :param stat: 文件元信息
        :return: None
        """
        pass

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
