import os
import pickle
import signal
import logging
import inspect

import utils
from cos_config import ops_constants
from catalog import Catalog, DirectoryStatus, FileStatus, initialize_metatree_cloud, initialize_metatree_local
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
        启动同步功能，包括步骤初始化目录结构、同步Push和Pull算法、保存历史信息；
        可通过自定义输入提示关闭此同步功能
        :return:
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        logger.info('云同步系统启动')
        self.close = False
        before = after = False
        self.initialize()
        print('输入 Ctrl + C ，即可离开同步系统')
        while not self.close:
            self.synchronize()
            before = after
            after = self.update_and_validate()
            logger.debug('before 的值为 {before}, after 的值为 {after}'.format(before=before, after=after))
            if before != after and after is True:
                print('同步成功')
                logger.info('同步成功')
                self.save_history()
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
        :return:
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        logger.info('开始初始化云同步系统')
        # 获取最新树结构
        self.metatree_cloud = initialize_metatree_cloud(self.cloud_path, self.cfs)
        logger.info('获取最新的云端元信息树')
        logger.debug('云端元信息树的值为 {metatree_cloud}'.format(metatree_cloud=self.metatree_cloud))
        self.metatree_local = initialize_metatree_local(self.local_path)
        logger.info('获取最新的本地元信息树')
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
        # 计算最新树摘要
        utils.get_entire_cloud_directory_hash(self.metatree_cloud, self.cfs)
        logger.info('计算云端元信息树的哈希值')
        logger.debug('云端元信息树的散列值为 {hash}'.format(hash=self.metatree_cloud.hash_value))
        utils.get_entire_local_directory_hash(self.metatree_local)
        logger.info('计算本地元信息树的哈希值')
        logger.debug('本地元信息树的散列值为 {hash}'.format(hash=self.metatree_local.hash_value))
        logger.info('云同步系统初始化完成')

    def synchronize(self):
        """
        同步本地和云端指定的目录 包括算法 AlgorithmPUSH | AlgorithmPULL
        :return:
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        logger.info('开始运行 PULL 算法')
        self.algorithm_pull(self.metatree_cloud, self.metatree_cloud_history,
                            self.metatree_local, self.metatree_local_history,
                            self.cloud_path, self.local_path)
        logger.info('PULL 算法运行结束')
        # if self.update_and_validate():
        #     return
        logger.info('开始运行 PUSH 算法')
        self.algorithm_push(self.metatree_cloud, self.metatree_cloud_history,
                            self.metatree_local, self.metatree_local_history,
                            self.cloud_path, self.local_path)
        logger.info('PUSH 算法运行结束')

    def update_and_validate(self):
        """
        更新以及验证
        得到最新的目录结构，并计算节点摘要值，对比本地和云端的是否一致
        :return: Boolean
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        # 将当前的树结构保存到历史
        self.metatree_cloud_history = self.metatree_cloud
        logger.info('将云端元信息树赋值给云端历史元信息树')
        self.metatree_local_history = self.metatree_local
        logger.info('将本地元信息树赋值给本地历史元信息树')
        # 获取最新树结构
        self.metatree_cloud = initialize_metatree_cloud(self.cloud_path, self.cfs)
        logger.info('获取最新的云端元信息树')
        logger.debug('云端元信息树的值为 {metatree_cloud}'.format(metatree_cloud=self.metatree_cloud))
        self.metatree_local = initialize_metatree_local(self.local_path)
        logger.info('获取最新的本地元信息树')
        logger.debug('本地元信息树的值为 {metatree_local}'.format(metatree_local=self.metatree_local))
        # 计算树的摘要
        utils.get_entire_cloud_directory_hash(self.metatree_cloud, self.cfs)
        logger.info('计算云端元信息树的哈希值')
        logger.debug('云端元信息树的散列值为 {hash}'.format(hash=self.metatree_cloud.hash_value))
        utils.get_entire_local_directory_hash(self.metatree_local)
        logger.info('计算本地元信息树的哈希值')
        logger.debug('本地元信息树的散列值为 {hash}'.format(hash=self.metatree_local.hash_value))
        # 验证本地和云端的目录是否一致
        is_consistent = self.metatree_cloud.hash_value == self.metatree_local.hash_value
        if is_consistent:
            logger.info('本地数据和云端数据一致')
        else:
            logger.info('本地数据与云端数据不一致')
        return is_consistent

    def save_history(self):
        """
        历史记录存到磁盘中
        :return:
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

    def algorithm_push(self, cloud, cloud_history, local, local_history, cloud_path, local_path):
        """
        历查找本地目录 metaTreeLocal ，对比历史记录和云端目录
        找出不一致的项，识别不一致的原因，并将任务提交给任务管理对象进行处理，任务包括：
        1. 创建云端目录 2. 重命名云端目录 3. 删除本地目录
        4. 删除本地文件 5. 重命名云端文件 6. 上传文件 7. 更新云端文件
        :param cloud: 云端元信息树
        :param cloud_history: 云端历史元信息树
        :param local: 本地元信息树
        :param local_history: 本地历史元信息树
        :param cloud_path: 云路径
        :param local_path: 本地路径
        :return:
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        logger.debug('遍历本地元信息树的每一项，判断是否需要进行同步操作')
        for catalog_local in local.child:
            filename = catalog_local.filename
            hash_value = catalog_local.hash_value
            next_local_path = local_path + filename[len(local_path):]
            next_cloud_path = cloud_path + filename[len(local_path):]
            catalog_local_history = local_history.find_catalog(next_local_path) if local_history is not None else None
            catalog_cloud = cloud.find_catalog(next_cloud_path) if cloud is not None else None
            catalog_cloud_history = cloud_history.find_catalog(next_cloud_path) if cloud_history is not None else None
            logger.debug('当前项 catalog_local 为 {}'.format(catalog_local))
            logger.debug('对应本地文件路径 next_local_path 为 {}'.format(next_local_path))
            logger.debug('对应云端文件路径 next_cloud_path 为 {}'.format(next_cloud_path))
            logger.debug('在本地历史树搜索的结果 catalog_local_history 为 {}'.format(catalog_local_history))
            logger.debug('在云端树搜索的结果 catalog_cloud 为 {}'.format(catalog_cloud))
            logger.debug('在云端历史树搜索的结果 catalog_cloud_history 为 {}'.format(catalog_cloud_history))
            if catalog_local.file_type == Catalog.IS_FOLDER:
                logger.debug('当前项为目录')
                # 尝试在本地历史中利用文件的 inode 或 UUID 查找信息
                logger.debug('正尝试从本地历史中利用文件 ID 查找信息')
                if local_history is not None:
                    file_id_local_history = local_history.find_catalog(catalog_local.file_id)
                else:
                    file_id_local_history = None
                logger.debug('查找结果 file_id_local_history 为 {}'.format(file_id_local_history))
                if catalog_cloud_history is not None:
                    logger.debug('云端历史非空，正从云端中利用云端历史的文件 ID 查找信息')
                    file_id_cloud = cloud.find_catalog(catalog_cloud_history.file_id)
                    logger.debug('从云端查找的结果 file_id_cloud 为 {}'.format(file_id_cloud))
                else:
                    file_id_cloud = None
                    logger.debug('云端历史为空，file_id_cloud 设置为 None')

                # # 在云端存在且散列值相同，说明文件夹没变，跳过
                # if catalog_cloud is not None and catalog_cloud.hash_value == hash_value:
                #     continue
                # # 在云端存在但散列值不相同，说明文件夹里面有修改，递归进去处理
                # if catalog_cloud is not None and catalog_cloud.hash_value != hash_value:
                #     self.algorithm_push(catalog_cloud, catalog_cloud_history,
                #                         catalog_local, catalog_local_history,
                #                         next_cloud_path, next_local_path)
                # 不管发生什么，直接递归进去
                if catalog_cloud is not None:
                    self.algorithm_push(catalog_cloud, catalog_cloud_history,
                                        catalog_local, catalog_local_history,
                                        next_cloud_path, next_local_path)
                # # 在云端不存在，但在云端历史（和本地历史）中存在，说明云端删除了此目录，则删除这个本地目录
                # if catalog_cloud is None and catalog_cloud_history is not None and file_id_cloud is None:
                #     self.tasks.set_data(ops_constants['DELETE_LOCAL_FOLDER'],
                #                         next_local_path)
                # 在云端、云端历史、本地历史都找不到记录，上传此云目录
                if catalog_cloud is None and catalog_cloud_history is None and file_id_local_history is None:
                    self.tasks.set_data(ops_constants['CREATE_CLOUD_FOLDER'],
                                        next_local_path, next_cloud_path)
                # 在云端、云端历史找不到记录，但在本地历史中有记录，重命名此云目录
                if catalog_cloud is None and catalog_cloud_history is None and file_id_local_history is not None:
                    self.tasks.set_data(ops_constants['RENAME_CLOUD_FOLDER'],
                                        cloud_path + file_id_local_history.filename[len(local_path):], next_cloud_path)
            elif catalog_local.file_type == Catalog.IS_FILE:
                logger.debug('当前项为文件')
                # 在历史记录中，尝试利用摘要查找信息
                logger.debug('正尝试从本地历史中利用文件摘要查找信息')
                if local_history is not None:
                    file_id_local_history = local_history.find_catalog(catalog_local.file_id)
                else:
                    file_id_local_history = None
                logger.debug('查找结果 file_id_local_history 为 {}'.format(file_id_local_history))
                if catalog_local_history is None and file_id_local_history is None:
                    # 在历史记录，名字和摘要都不存在，上传新文件
                    self.tasks.set_data(ops_constants['UPLOAD_FILE'],
                                        next_local_path, next_cloud_path)
                elif catalog_local_history is None and file_id_local_history is not None:
                    # 历史记录中不存在此目录名，但存在相同摘要，且本地最新，则重命名云端文件
                    self.tasks.set_data(ops_constants['RENAME_CLOUD_FILE'],
                                        cloud_path + file_id_local_history.filename[len(local_path):], next_cloud_path)
                elif catalog_local_history is not None \
                        and catalog_local_history.hash_value != catalog_local.hash_value \
                        and catalog_local.mtime > catalog_local_history.mtime:
                    # 历史记录中存在此文件名，此历史文件与本地文件摘要值不相同，且本地最新，则更新云端文件
                    self.tasks.set_data(ops_constants['UPDATE_CLOUD_FILE'],
                                        next_local_path, next_cloud_path)
                # elif catalog_local_history is not None \
                #         and catalog_local_history.hash_value == catalog_local.hash_value \
                #         and catalog_cloud is None:
                #     # 历史记录中此文件名与摘要都与本地一致, 但在云端找不到此文件，删除此本地文件
                #     self.tasks.set_data(ops_constants['DELETE_LOCAL_FILE'],
                #                         next_local_path)
            logger.debug('当前项 {filename} 处理完毕'.format(filename=filename))
        if local_history is None:
            return
        logger.debug('遍历本地元信息树的每一项，判断是否需要进行本地删除操作')
        for catalog_local_history in local_history.child:
            filename = catalog_local_history.filename
            next_local_path = local_path + filename[len(local_path):]
            next_cloud_path = cloud_path + filename[len(local_path):]
            catalog_local = local.find_catalog(next_local_path) if local is not None else None
            logger.debug('当前项 catalog_local_history 为 {}'.format(catalog_local_history))
            logger.debug('对应本地文件路径 next_local_path 为 {}'.format(next_local_path))
            logger.debug('在本地树搜索的结果 catalog_local 为 {}'.format(catalog_local))
            # 在本地中，尝试利用文件 ID 查找信息
            logger.debug('正尝试从本地中利用文件摘要查找信息')
            file_id_local = local.find_catalog(catalog_local_history.file_id)
            logger.debug('查找结果 file_id_local 为 {}'.format(file_id_local))
            if catalog_local_history.file_type == Catalog.IS_FOLDER and catalog_local is None and file_id_local is None:
                self.tasks.set_data(ops_constants['DELETE_CLOUD_FOLDER'], next_cloud_path)
            elif catalog_local_history.file_type == Catalog.IS_FILE and catalog_local is None and file_id_local is None:
                self.tasks.set_data(ops_constants['DELETE_CLOUD_FILE'], next_cloud_path)
            logger.debug('当前项 {filename} 处理完毕'.format(filename=filename))

    def algorithm_pull(self, cloud, cloud_history, local, local_history, cloud_path, local_path):
        """
        遍历查找本地目录 metaTreeCloud ，对比历史记录和本地目录
        找出不一致的项，识别不一致的原因，并将任务提交给任务管理对象进行处理，任务包括：
        1. 创建本地目录 2. 重命名本地目录 3. 删除云端目录
        4. 删除云端文件 5. 重命名本地文件 6. 下载文件 7. 更新本地文件
        :param cloud: 云端元信息树
        :param cloud_history: 云端历史元信息树
        :param local: 本地元信息树
        :param local_history: 本地历史元信息树
        :param cloud_path: 云路径
        :param local_path: 本地路径
        :return:
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        logger.debug('遍历云端元信息树的每一项，判断是否需要进行同步操作')
        for catalog_cloud in cloud.child:
            filename = catalog_cloud.filename
            hash_value = catalog_cloud.hash_value
            next_local_path = local_path + filename[len(cloud_path):]
            next_cloud_path = cloud_path + filename[len(cloud_path):]
            catalog_cloud_history = cloud_history.find_catalog(next_cloud_path) if cloud_history is not None else None
            catalog_local = local.find_catalog(next_local_path) if local is not None else None
            catalog_local_history = local_history.find_catalog(next_local_path) if local_history is not None else None
            logger.debug('当前项 catalog_cloud 为 {}'.format(catalog_cloud))
            logger.debug('对应本地文件路径 next_local_path 为 {}'.format(next_local_path))
            logger.debug('对应云端文件路径 next_cloud_path 为 {}'.format(next_cloud_path))
            logger.debug('在云端历史树搜索的结果 catalog_cloud_history 为 {}'.format(catalog_cloud_history))
            logger.debug('在本地树搜索的结果 catalog_local 为 {}'.format(catalog_local))
            logger.debug('在本地历史树搜索的结果 catalog_local_history 为 {}'.format(catalog_local_history))
            if catalog_cloud.file_type == Catalog.IS_FOLDER:
                logger.debug('当前项为目录')
                # 尝试在云端历史中利用文件的 inode 或 UUID 查找信息
                logger.debug('正尝试从云端历史中利用文件 ID 查找信息')
                if cloud_history is not None:
                    file_id_cloud_history = cloud_history.find_catalog(catalog_cloud.file_id)
                else:
                    file_id_cloud_history = None
                logger.debug('查找结果 file_id_cloud_history 为 {}'.format(file_id_cloud_history))
                if catalog_local_history is not None:
                    logger.debug('本地历史非空，正从本地中利用本地历史的文件 ID 查找信息')
                    file_id_local = local.find_catalog(catalog_local_history.file_id)
                    logger.debug('从本地查找的结果 file_id_local 为 {}'.format(file_id_local))
                else:
                    file_id_local = None
                    logger.debug('本地历史为空，file_id_local 设置为 None')

                # # 在本地存在且散列值相同，说明文件夹没变，跳过
                # if catalog_local is not None and catalog_local.hash_value == hash_value:
                #     continue
                # # 在本地存在但散列值不相同，说明文件夹里面有修改，递归进去处理
                # if catalog_local is not None and catalog_local.hash_value != hash_value:
                #     self.algorithm_pull(catalog_cloud, catalog_cloud_history,
                #                         catalog_local, catalog_local_history,
                #                         next_cloud_path, next_local_path)
                # 不管发生什么，直接递归进去
                if catalog_local is not None:
                    self.algorithm_pull(catalog_cloud, catalog_cloud_history,
                                        catalog_local, catalog_local_history,
                                        next_cloud_path, next_local_path)
                # # 在本地不存在，但在本地历史（和云端历史）中存在，说明本地删除了此目录，则删除这个云端目录
                # if catalog_local is None and catalog_local_history is not None and file_id_local is None:
                #     self.tasks.set_data(ops_constants['DELETE_CLOUD_FOLDER'],
                #                         next_cloud_path)
                # 在本地、本地历史、云端历史都找不到记录，创建此本地目录
                if catalog_local is None and catalog_local_history is None and file_id_cloud_history is None:
                    self.tasks.set_data(ops_constants['CREATE_LOCAL_FOLDER'],
                                        next_cloud_path, next_local_path)
                # 在本地、本地历史找不到记录，但在云端历史中有记录，重命名此本地目录
                if catalog_local is None and catalog_local_history is None and file_id_cloud_history is not None:
                    self.tasks.set_data(ops_constants['RENAME_LOCAL_FOLDER'],
                                        local_path + file_id_cloud_history.filename[len(cloud_path):], next_local_path)
            elif catalog_cloud.file_type == Catalog.IS_FILE:
                logger.debug('当前项为文件')
                # 在历史记录中，尝试利用摘要查找信息
                logger.debug('正尝试从云端历史中利用文件摘要查找信息')
                if cloud_history is not None:
                    file_id_cloud_history = cloud_history.find_catalog(catalog_cloud.file_id)
                else:
                    file_id_cloud_history = None
                logger.debug('查找结果 file_id_cloud_history 为 {}'.format(file_id_cloud_history))
                if catalog_local_history is None and file_id_cloud_history is None:
                    # 在历史记录，名字和摘要都不存在，下载新文件
                    self.tasks.set_data(ops_constants['DOWNLOAD_FILE'],
                                        next_cloud_path, next_local_path)
                elif catalog_local_history is None and file_id_cloud_history is not None:
                    # 历史记录中不存在此目录名，但存在相同摘要，则且云端最新，则重命名本地文件
                    self.tasks.set_data(ops_constants['RENAME_LOCAL_FILE'],
                                        local_path + file_id_cloud_history.filename[len(cloud_path):], next_local_path)
                elif catalog_local_history is not None \
                        and catalog_local_history.hash_value != catalog_cloud.hash_value \
                        and catalog_cloud.mtime > catalog_local_history.mtime:
                    # 历史记录中存在此文件名，此历史文件与本地文件摘要值不相同，且云端最新，则更新本地文件
                    self.tasks.set_data(ops_constants['UPDATE_LOCAL_FILE'],
                                        next_cloud_path, next_local_path)
                # elif catalog_local_history is not None \
                #         and catalog_local_history.hash_value == catalog_cloud.hash_value \
                #         and catalog_local is None:
                #     # 历史记录中此文件名与摘要都一致，但在本地找不到此文件，删除此云端文件
                #     self.tasks.set_data(ops_constants['DELETE_CLOUD_FILE'],
                #                         next_cloud_path)
            logger.debug('当前项 {filename} 处理完毕'.format(filename=filename))
        if cloud_history is None:
            return
        logger.debug('遍历云端元信息树的每一项，判断是否需要进行云端删除操作')
        for catalog_cloud_history in cloud_history.child:
            filename = catalog_cloud_history.filename
            next_local_path = local_path + filename[len(cloud_path):]
            next_cloud_path = cloud_path + filename[len(cloud_path):]
            catalog_cloud = cloud.find_catalog(next_cloud_path) if cloud is not None else None
            logger.debug('当前项 catalog_cloud_history 为 {}'.format(catalog_cloud_history))
            logger.debug('对应云端文件路径 next_cloud_path 为 {}'.format(next_cloud_path))
            logger.debug('在云端树搜索的结果 catalog_cloud 为 {}'.format(catalog_cloud))
            # 在云端中，尝试利用文件 ID 查找信息
            logger.debug('正尝试从云端中利用文件摘要查找信息')
            file_id_cloud = cloud.find_catalog(catalog_cloud_history.file_id)
            logger.debug('查找结果 file_id_cloud 为 {}'.format(file_id_cloud))
            if catalog_cloud_history.file_type == Catalog.IS_FOLDER and catalog_cloud is None and file_id_cloud is None:
                self.tasks.set_data(ops_constants['DELETE_LOCAL_FOLDER'], next_local_path)
            elif catalog_cloud_history.file_type == Catalog.IS_FILE and catalog_cloud is None and file_id_cloud is None:
                self.tasks.set_data(ops_constants['DELETE_LOCAL_FILE'], next_local_path)
            logger.debug('当前项 {filename} 处理完毕'.format(filename=filename))

    @staticmethod
    def find_rename_file(history: DirectoryStatus, local: DirectoryStatus, hash_value: str):
        """
        在历史记录中，找到具有重命名规则的某个文件；
        重命名规则：本地一个文件名，历史不存在，但二者摘要值一样，
        而且这个文件不能是其他具有相同内容的文件（例如原文件的副本），必须是真正被重命名的文件；
        :param history: 历史目录
        :param local: 本地目录
        :param hash_value: 消息摘要
        :return: 重命名的子文件或者为空
        """
        number = 1
        exit_flag = 0
        hash_history = None
        while exit_flag != 1:
            hash_history = history.find_catalog(hash_value, number)
            number += 1
            if hash_history is None or (
                    not hash_history.filename.endswith('/') and local.find_catalog(hash_history.filename) is None
            ):
                exit_flag = 1
        return hash_history
