import os


class CloudFileSystem:
    def __init__(self, csp_):
        self.csp = csp_
        if not os.path.exists('cfs_{csp}.py'.format(csp=self.csp)):
            # todo: 报个警
            print('error!')
            return
        cfs = __import__('cfs_{csp}'.format(csp=self.csp))
        self.upload = cfs.upload
        self.download = cfs.download
        self.delete = cfs.delete
        self.update = cfs.update
        self.rename = cfs.rename
        self.create_folder = cfs.create_folder
        self.stat_file = cfs.stat_file
        self.list_files = cfs.list_files
        self.set_hash = cfs.set_hash
        self.get_hash = cfs.get_hash
        self.set_mtime = cfs.set_mtime
        self.get_mtime = cfs.get_mtime
