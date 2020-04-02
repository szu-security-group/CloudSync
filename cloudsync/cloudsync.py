import sys
import logging
import inspect

from cfs import CloudFileSystem
from synchronize import Synchronize


def start_sync(csp):
    """
    根据 csp 的名字启动对应的同步程序
    :param csp: COS Service Provider
    :return: None
    """
    logger = logging.getLogger(inspect.stack()[0].function)
    logger.info('COS 服务提供商为 {csp}'.format(csp=csp))
    logger.info('开始与 {csp} 进行同步...'.format(csp=sys.argv[2]))
    if csp == 'tencent':
        cfs = CloudFileSystem('tencent')
    elif csp == 'ali':
        cfs = CloudFileSystem('ali')
    else:
        logger.error("未知的 COS 服务提供商!")
        return

    Synchronize(cfs).start()


def print_help():
    logger = logging.getLogger(inspect.stack()[0].function)
    logger.info('输出帮助文档')
    print('''\
usage: python3 cloudsync.py [OPTION] ...
options:
  -s, --sync {tencent|ali}  指定数据同步到选定云
  -h, --help                输出此帮助文档
''')


if __name__ == '__main__':
    # 初始化 logging
    # 获取 root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # log 等级总开关
    # 定义 log 的输出格式
    # 创建一个用于写入日志文件的 handler
    file_handler = logging.FileHandler('cloudsync.log', encoding='utf-8')
    file_handler.setLevel(logging.WARNING)  # 输出到 file 的 log 等级的开关
    file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] '
                                  '%(filename)s -> %(name)s(line:%(lineno)d): %(message)s')
    file_handler.setFormatter(file_formatter)
    # 创建一个用于输出到控制台的 handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)  # 输出到 console 的 log 等级的开关
    console_formatter = logging.Formatter('[%(levelname)s] %(filename)s -> %(name)s(line:%(lineno)d): %(message)s')
    console_handler.setFormatter(console_formatter)
    # 将 handler 添加到 root handler
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    # 将引入模块的 log 等级提升到 warning
    for _ in ['urllib3', 'qcloud_cos']:
        logging.getLogger(_).setLevel(logging.WARNING)

    # 解析参数
    if len(sys.argv) == 3 and sys.argv[1] in ['-s', '--sync']:
        start_sync(csp=sys.argv[2])
    else:
        print_help()
