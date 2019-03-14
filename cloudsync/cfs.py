import os


class CloudFileSystem:
    config = {
        'history_path': '',
        'local_path': '',
        'cloud_path': ''
    }

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
        self.config['history_path'] = cfs.path_config['history_path']
        self.config['local_path'] = cfs.path_config['local_path']
        self.config['cloud_path'] = cfs.path_config['cloud_path']
