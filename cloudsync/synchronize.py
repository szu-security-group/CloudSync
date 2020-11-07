import os
import pickle
import signal
import logging
import inspect
import time

from global_value import OP, hash_table_local, hash_table_cloud
from catalog import Catalog, DirectoryStatus, initialize_metatree_cloud, initialize_metatree_local
from cfs import CloudFileSystem
from synchronize_event_emitter import SynchronizeEventEmitter
from synchronize_event_handler import SynchronizeEventHandler


class Synchronize:
    def __init__(self, cfs: CloudFileSystem):
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        self.cfs = cfs
        self.history_path = cfs.config['history_path']
        self.local_path = cfs.config['local_path']
        self.cloud_path = cfs.config['cloud_path']
        logger.debug('history_path 的值为 {history_path}'.format(history_path=self.history_path))
        logger.debug('local_path 的值为 {local_path}'.format(local_path=self.local_path))
        logger.debug('cloud_path 的值为 {cloud_path}'.format(cloud_path=self.cloud_path))

        self.tasks = SynchronizeEventEmitter()
        self.metatree_cloud = None
        self.metatree_cloud_history = None
        self.metatree_local = None
        self.metatree_local_history = None
        self.close = True

        self.tasks.register(SynchronizeEventHandler(self.cfs))
        # 将工作目录切换成 local_path
        os.chdir(self.local_path)
        logger.info('工作目录切换到 {work_dir}'.format(work_dir=self.local_path))
        # 按下 Ctrl+C 之后停止同步程序
        signal.signal(signal.SIGINT, self.stop)

    def start(self):
        """
        启动同步功能，包括步骤初始化目录结构、同步 PULL 和 PUSH 算法、保存历史信息；
        可通过自定义输入提示关闭此同步功能
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        logger.info('云同步系统启动')
        print('输入 Ctrl + C ，即可离开同步系统')
        self.close = False
        self.initialize()
        while not self.close:
            self.synchronize()
            time.sleep(29)
        print('同步关闭')
        logger.info('云同步系统关闭')

    def stop(self, sig, frame):
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        self.close = True
        logger.debug('云同步系统关闭标志设置为 True')

    def initialize(self):
        """
        初始化历史记录、本地目录、云端目录的结构缓存的方法；如果历史记录文件不存在，
        则视为首次运行，历史记录结构为只有根目录的树；历史文件将会直接影响到文件的唯一状态信息。
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        logger.info('开始初始化云同步系统')
        # 获取最新树结构
        logger.info('获取最新的云端元信息树')
        self.metatree_cloud = initialize_metatree_cloud(self.cloud_path, self.cfs, hash_table_cloud)
        logger.debug('云端元信息树的值为 {metatree_cloud}'.format(metatree_cloud=self.metatree_cloud))
        logger.info('获取最新的本地元信息树')
        self.metatree_local = initialize_metatree_local(self.local_path, hash_table_local)
        logger.debug('本地元信息树的值为 {metatree_local}'.format(metatree_local=self.metatree_local))
        # 获取云端历史树结构
        cloud_history_path = self.history_path + '.cloud'
        if not os.path.exists(cloud_history_path):
            logger.info('未在磁盘找到云端历史元信息树，它将会被创建')
            open(cloud_history_path, 'w').close()  # create historical file
            logger.info('创建云端历史元信息树文件成功，文件名为 {}'.format(cloud_history_path))
        if os.path.getsize(cloud_history_path) > 0:
            logger.info('云端历史元信息树文件非空，尝试从中恢复云端历史元信息树')
            try:
                with open(cloud_history_path, 'rb') as f:
                    self.metatree_cloud_history = pickle.load(f)
                logger.info('成功通过本地磁盘文件恢复云端历史元信息树')
            except Exception as err:
                logger.exception('读取本地磁盘中的云端历史元信息树文件出现错误，错误信息为 {err}'.format(err=err))
        else:
            logger.info('云端历史元信息树文件为空')
            self.metatree_cloud_history = DirectoryStatus(self.cloud_path)
            logger.info('根据云端路径 {cloud_path} 创建云端历史元信息树'.format(cloud_path=self.cloud_path))
        # 获取本地历史树结构
        local_history_path = self.history_path + '.local'
        if not os.path.exists(local_history_path):
            logger.info('未在磁盘找到本地历史元信息树，它将会被创建')
            open(local_history_path, 'w').close()  # create historical file
            logger.info('创建本地历史元信息树文件成功，文件名为 {}'.format(local_history_path))
        if os.path.getsize(local_history_path) > 0:
            logger.info('本地历史元信息树文件非空，尝试从中恢复本地历史元信息树')
            try:
                with open(local_history_path, 'rb') as f:
                    self.metatree_local_history = pickle.load(f)
                logger.info('成功通过本地磁盘文件恢复本地历史元信息树')
            except Exception as err:
                logger.exception('读取本地磁盘中的历史元信息树文件出现错误，错误信息为 {err}'.format(err=err))
        else:
            logger.info('本地历史元信息树文件为空')
            self.metatree_local_history = DirectoryStatus(self.local_path)
            logger.info('根据本地路径 {local_path} 创建本地历史元信息树'.format(local_path=self.local_path))
        logger.info('云同步系统初始化完成')

    def synchronize(self):
        """
        同步本地和云端指定的目录 包括算法 AlgorithmPULL | AlgorithmPUSH
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))

        # Build Cloud Current Tree
        logger.info('获取最新的云端元信息树')
        self.metatree_cloud = initialize_metatree_cloud(self.cloud_path, self.cfs, hash_table_cloud)
        logger.debug('云端元信息树的值为 {metatree_cloud}'.format(metatree_cloud=self.metatree_cloud))
        # Run PULL Algorithm
        logger.info('开始运行 PULL 算法')
        self.algorithm_pull(self.metatree_cloud, self.metatree_cloud_history,
                            self.cloud_path, self.local_path)
        logger.info('PULL 算法运行结束')

        # Build Local Current Tree
        logger.info('获取最新的本地元信息树')
        self.metatree_local = initialize_metatree_local(self.local_path, hash_table_local)
        logger.debug('本地元信息树的值为 {metatree_local}'.format(metatree_local=self.metatree_local))
        # Run PUSH Algorithm
        logger.info('开始运行 PUSH 算法')
        self.algorithm_push(self.metatree_local, self.metatree_local_history,
                            self.cloud_path, self.local_path)
        logger.info('PUSH 算法运行结束')

        # Update and Save History Tree
        self.metatree_cloud_history = self.metatree_cloud
        logger.info('将云端元信息树赋值给云端历史元信息树')
        self.metatree_local_history = self.metatree_local
        logger.info('将本地元信息树赋值给本地历史元信息树')
        self.save_history()

    def save_history(self):
        """
        将云端历史树以及本地历史树保存到磁盘中
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        logger.info('尝试将本地历史元信息树写入本地磁盘')
        try:
            with open(self.history_path + '.local', 'wb') as f:
                pickle.dump(self.metatree_local_history, f)
            logger.info('本地历史元信息树文件写入成功')
        except Exception as err:
            logger.exception('本地历史元信息树文件写入失败，错误信息为 {err}'.format(err=err))
        logger.info('尝试将云端历史元信息树写入本地磁盘')
        try:
            with open(self.history_path + '.cloud', 'wb') as f:
                pickle.dump(self.metatree_cloud_history, f)
            logger.info('云端历史元信息树文件写入成功')
        except Exception as err:
            logger.exception('云端历史元信息树文件写入失败，错误信息为 {err}'.format(err=err))

    def algorithm_push(self, local, local_history, cloud_path, local_path):
        """
        历查找本地目录 metaTreeLocal ，对比历史记录和云端目录
        找出不一致的项，识别不一致的原因，并将任务提交给任务管理对象进行处理，任务包括：
        创建云端目录, 删除本地目录
        上传文件, 更新云端文件, 删除本地文件
        :param local: 本地元信息树
        :param local_history: 本地历史元信息树
        :param cloud_path: 云路径
        :param local_path: 本地路径
        :return:
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        # 新增、修改部分
        logger.info('遍历本地当前元信息树的每一项，判断是否需要进行同步操作')
        for catalog_local in local.children:
            filename = catalog_local.filename
            next_local_path = local_path + filename[len(local_path):]
            next_cloud_path = cloud_path + filename[len(local_path):]
            next_local_history = local_history.find_catalog(next_local_path) if local_history is not None else None
            logger.info('当前项 catalog_local 为 {}'.format(catalog_local))
            logger.debug('对应本地文件路径 next_local_path 为 {}'.format(next_local_path))
            logger.debug('对应云端文件路径 next_cloud_path 为 {}'.format(next_cloud_path))
            logger.debug('在本地历史树搜索的结果 next_local_history 为 {}'.format(next_local_history))
            if catalog_local.file_type == Catalog.IS_FOLDER:
                logger.debug('当前项为目录')
                # 如果历史树中存在，则直接递归进去目录处理
                if next_local_history is not None:
                    self.algorithm_push(catalog_local, next_local_history,
                                        next_cloud_path, next_local_path)
                # 在本地历史中利用文件名和文件ID都找不到记录，上传此云目录
                if next_local_history is None:
                    self.tasks.set_data(OP.CREATE_CLOUD_FOLDER,
                                        next_local_path, next_cloud_path)
            elif catalog_local.file_type == Catalog.IS_FILE:
                logger.debug('当前项为文件')
                # 在历史记录，名字和摘要都不存在，上传新文件
                if next_local_history is None:
                    self.tasks.set_data(OP.UPLOAD_FILE,
                                        next_local_path, next_cloud_path, file_id=catalog_local.file_id)
                # 历史记录中存在此文件名，此历史文件与本地文件摘要值不相同，且本地最新，则更新云端文件
                if next_local_history is not None \
                        and int(next_local_history.mtime) < int(catalog_local.mtime) \
                        and next_local_history.file_id != catalog_local.file_id:
                    self.tasks.set_data(OP.UPDATE_CLOUD_FILE,
                                        next_local_path, next_cloud_path, file_id=catalog_local.file_id)
            logger.info('当前项 {filename} 处理完毕'.format(filename=filename))
        # 删除部分
        if local_history is None:
            return
        logger.info('遍历本地历史元信息树的每一项，判断是否需要进行本地删除操作')
        for next_local_history in local_history.children:
            filename = next_local_history.filename
            next_local_path = local_path + filename[len(local_path):]
            next_cloud_path = cloud_path + filename[len(local_path):]
            catalog_local = local.find_catalog(next_local_path) if local is not None else None
            logger.info('当前项 next_local_history 为 {}'.format(next_local_history))
            logger.debug('对应本地文件路径 next_local_path 为 {}'.format(next_local_path))
            logger.debug('在本地树搜索的结果 catalog_local 为 {}'.format(catalog_local))
            if catalog_local is None:
                if next_local_history.file_type == Catalog.IS_FOLDER:
                    self.tasks.set_data(OP.DELETE_CLOUD_FOLDER, next_cloud_path)
                else:
                    self.tasks.set_data(OP.DELETE_CLOUD_FILE, next_cloud_path)
            logger.info('当前项 {filename} 处理完毕'.format(filename=filename))

    def algorithm_pull(self, cloud, cloud_history, cloud_path, local_path):
        """
        遍历查找本地目录 metaTreeCloud ，对比历史记录和本地目录
        找出不一致的项，识别不一致的原因，并将任务提交给任务管理对象进行处理，任务包括：
        创建本地目录, 删除云端目录
        下载文件, 更新本地文件, 删除云端文件
        :param cloud: 云端元信息树
        :param cloud_history: 云端历史元信息树
        :param cloud_path: 云路径
        :param local_path: 本地路径
        :return:
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        # 新增、修改部分
        logger.info('遍历云端当前元信息树的每一项，判断是否需要进行同步操作')
        for catalog_cloud in cloud.children:
            filename = catalog_cloud.filename
            next_local_path = local_path + filename[len(cloud_path):]
            next_cloud_path = cloud_path + filename[len(cloud_path):]
            next_cloud_history = cloud_history.find_catalog(next_cloud_path) if cloud_history is not None else None
            logger.info('当前项 catalog_cloud 为 {}'.format(catalog_cloud))
            logger.debug('对应本地文件路径 next_local_path 为 {}'.format(next_local_path))
            logger.debug('对应云端文件路径 next_cloud_path 为 {}'.format(next_cloud_path))
            logger.debug('在云端历史树搜索的结果 next_cloud_history 为 {}'.format(next_cloud_history))
            if catalog_cloud.file_type == Catalog.IS_FOLDER:
                logger.debug('当前项为目录')
                # 如果历史树中存在，则直接递归进去目录处理
                if next_cloud_history is not None:
                    self.algorithm_pull(catalog_cloud, next_cloud_history,
                                        next_cloud_path, next_local_path)
                # 在云端历史中利用文件名和文件ID都找不到记录，创建此本地目录
                if next_cloud_history is None:
                    self.tasks.set_data(OP.CREATE_LOCAL_FOLDER,
                                        next_cloud_path, next_local_path)
            elif catalog_cloud.file_type == Catalog.IS_FILE:
                logger.debug('当前项为文件')
                # 在历史记录，名字和摘要都不存在，下载新文件
                if next_cloud_history is None:
                    self.tasks.set_data(OP.DOWNLOAD_FILE,
                                        next_cloud_path, next_local_path, file_id=catalog_cloud.file_id)
                # 历史记录中存在此文件名，此历史文件与本地文件摘要值不相同，且云端最新，则更新本地文件
                elif next_cloud_history is not None \
                        and int(next_cloud_history.mtime) < int(catalog_cloud.mtime) \
                        and next_cloud_history.file_id != catalog_cloud.file_id:
                    self.tasks.set_data(OP.UPDATE_LOCAL_FILE,
                                        next_cloud_path, next_local_path, file_id=catalog_cloud.file_id)
            logger.info('当前项 {filename} 处理完毕'.format(filename=filename))
        # 删除部分
        if cloud_history is None:
            return
        logger.info('遍历云端历史元信息树的每一项，判断是否需要进行云端删除操作')
        for next_cloud_history in cloud_history.children:
            filename = next_cloud_history.filename
            next_local_path = local_path + filename[len(cloud_path):]
            next_cloud_path = cloud_path + filename[len(cloud_path):]
            catalog_cloud = cloud.find_catalog(next_cloud_path) if cloud is not None else None
            logger.info('当前项 next_cloud_history 为 {}'.format(next_cloud_history))
            logger.debug('对应云端文件路径 next_cloud_path 为 {}'.format(next_cloud_path))
            logger.debug('在云端树搜索的结果 catalog_cloud 为 {}'.format(catalog_cloud))
            if catalog_cloud is None:
                if next_cloud_history.file_type == Catalog.IS_FOLDER:
                    self.tasks.set_data(OP.DELETE_LOCAL_FOLDER, next_local_path)
                else:
                    self.tasks.set_data(OP.DELETE_LOCAL_FILE, next_local_path)
            logger.info('当前项 {filename} 处理完毕'.format(filename=filename))
