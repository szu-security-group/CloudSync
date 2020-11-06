import logging
import inspect


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
        self.kwargs = {}

    def register(self, observer):
        """
        将观察者注册到观察者列表中
        :param observer: 观察者
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        logger.info('观察者 {observer} 准备绑定到被观察者'.format(observer=observer))
        if observer not in self._observers:
            self._observers.append(observer)
            logger.info('观察者已成功添加到观察者列表')
        else:
            logger.warning('绑定失败！观察者 {observer} 已存在于观察者列表中'.format(observer=observer))

    def unregister(self, observer):
        """
        将观察者从观察者列表中移除
        :param observer: 观察者
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        logger.info('观察者 {observer} 准备从被观察者中解绑'.format(observer=observer))
        if observer in self._observers:
            self._observers.remove(observer)
            logger.info('观察者已从观察者列表中移除')
        else:
            logger.warning('解绑失败！观察者 {observer} 不存在于观察者列表中'.format(observer=observer))

    def notify(self):
        """
        通知观察者列表中的观察者
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        logger.debug('被观察者将通知所有已注册的观察者')
        for observer in self._observers:
            observer.update(self)
            logger.debug('已通知 {observer}'.format(observer=observer))
        logger.debug('通知完毕')

    def set_data(self, task_index, *args, **kwargs):
        """
        更新被观察者的数据，并调用 notify() 更新观察者
        :param task_index: 任务序号
        :param args: 观察者执行参数， 1~2个
        :return: None
        """
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__,
                                           function_name=inspect.stack()[0].function))
        logger.info('被观察者状态发生变化。')
        self.task_index = task_index
        self.from_path = args[0]
        try:
            self.to_path = args[1]
        except IndexError:
            self.to_path = ''
        self.kwargs = kwargs

        if self.to_path != '':
            logger.info('新值为: task_index={task_index}, from_path={from_path}, to_path={to_path}'
                        .format(task_index=self.task_index, from_path=self.from_path, to_path=self.to_path))
        else:
            logger.info('新值为: task_index={task_index}, from_path={from_path}'
                        .format(task_index=self.task_index, from_path=self.from_path))

        self.notify()
