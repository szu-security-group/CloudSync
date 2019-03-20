import sys

from cfs import CloudFileSystem
from synchronize import Synchronize


def main():
    if len(sys.argv) == 3 and sys.argv[1] in ['-s', '--sync'] and sys.argv[2] in ['tencent', 'ali']:
        print('Start sync with {csp}...'.format(csp=sys.argv[2]))
        start_sync(csp=sys.argv[2])
    else:
        print_help()


def start_sync(csp):
    """
    根据 cosp 的名字启动对应的同步程序
    :param csp: COS Service Provider
    :return: None
    """
    if csp == 'tencent':
        cfs = CloudFileSystem('tencent')
    elif csp == 'ali':
        cfs = CloudFileSystem('ali')
    else:
        print("Unknown COS Service Provider!")
        return

    Synchronize(cfs).start()


def print_help():
    print('''\
usage: python3 cloudsync.py [OPTION] ...
options:
  -s, --sync {tencent|ali}  指定数据同步到选定云
  -h, --help                输出此帮助文档
''')


if __name__ == '__main__':
    main()
