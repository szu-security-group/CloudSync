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

    def register(self, observer):
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        logger.info('观察者 {observer} 准备绑定到被观察者'.format(observer=observer))
        if observer not in self._observers:
            self._observers.append(observer)
            logger.info('观察者已成功添加到观察者列表')
        else:
            logger.warning('绑定失败！观察者 {observer} 已存在于观察者列表中'.format(observer=observer))

    def unregister(self, observer):
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        logger.info('观察者 {observer} 准备从被观察者中解绑'.format(observer=observer))
        if observer in self._observers:
            self._observers.remove(observer)
            logger.info('观察者已从观察者列表中移除')
        else:
            logger.warning('解绑失败！观察者 {observer} 不存在于观察者列表中'.format(observer=observer))

    def notify(self):
        logger = logging.getLogger('{class_name} -> {function_name}'
                                   .format(class_name=__class__.__name__, function_name=inspect.stack()[0].function))
        logger.info('被观察者将通知所有已注册的观察者')
        for observer in self._observers:
            observer.update(self)
            logger.info('已通知 {observer}'.format(observer=observer))
        logger.info('通知完毕')

    def set_data(self, task_index, *args):
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

        if self.to_path != '':
            logger.debug('新值为: task_index={task_index}, from_path={from_path}, to_path={to_path}'
                         .format(task_index=self.task_index, from_path=self.from_path, to_path=self.to_path))
        else:
            logger.debug('新值为: task_index={task_index}, from_path={from_path}'
                         .format(task_index=self.task_index, from_path=self.from_path))

        self.notify()
