from cos_config import ops_constants


class SynchronizeEventEmitter:
    """
    任务参数管理，中介对象
    存储任务需要的信息，以及通知相应任务
    作为可观察者 Observable
    """
    def __init__(self):
        self._observers = []
        self.task_index = 0
        self.from_path = ''
        self.to_path = ''

    def register(self, observer):
        if observer not in self._observers:
            self._observers.append(observer)

    def unregister(self, observer):
        if observer in self._observers:
            self._observers.remove(observer)

    def notify(self):
        for observer in self._observers:
            observer.update(self)

    def set_data(self, obj, task_index, *args):
        self.task_index = task_index
        # 删除文件、创建目录、下载文件、上传文件
        if task_index in [ops_constants['UPLOAD_FILE'],
                          ops_constants['DOWNLOAD_FILE'],
                          ops_constants['CREATE_CLOUD_FOLDER'],
                          ops_constants['RENAME_CLOUD_FOLDER'],
                          ops_constants['RENAME_LOCAL_FOLDER'],
                          ops_constants['CREATE_LOCAL_FOLDER']
                          ]:
            self.directory_list = obj
            self.from_path = args[0]
            self.to_path = args[1]
        if task_index in [ops_constants['DELETE_CLOUD_FILE'],
                          ops_constants['DELETE_CLOUD_FOLDER']]:
            self.directory_list = obj
            self.from_path = args[0]

        # 本地删除文件、目录和创建目录
        if task_index in [ops_constants['DELETE_LOCAL_FOLDER'],
                          ops_constants['DELETE_LOCAL_FILE']]:
            self.directory_list = obj
            self.from_path = args[0]

        # 重命名、更新文件
        if task_index in [ops_constants['RENAME_CLOUD_FILE'],
                          ops_constants['RENAME_LOCAL_FILE'],
                          ops_constants['UPDATE_CLOUD_FILE']]:
            self.stat_file = obj
            self.from_path = args[0]
            self.to_path = args[1]

        self.notify()
