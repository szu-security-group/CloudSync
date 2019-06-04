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

    def set_data(self, task_index, *args):
        self.task_index = task_index
        self.from_path = args[0]
        try:
            self.to_path = args[1]
        except IndexError:
            self.to_path = ''

        self.notify()
