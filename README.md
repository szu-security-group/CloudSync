# CloudSync

CloudSync is a synchronization tool that helps to sync files between local storage and cloud object storage.

This project achieves two-way, real-time synchronization.
It can synchronize files from local to cloud, or from cloud to local.
Therefore, it is able to sync user data for different sources by leveraging the cloud storage as a mediator.

![System Architecture](system_architecture.png)

CloudSync now supports Alibaba OSS and Tencent COS.
In theory, it supports any object storage that implements the standard read-write APIs.

Python 3 is used for development.

## Installation

1. Install requirements

    CloudSync installation requires [Python3](https://www.python.org/) and [pip](https://pypi.org/project/pip/).

    Please install them before using this code.

2. Clone the code

    Use git to clone source code of this project to the local.

    ```bash
    git clone https://github.com/szu-security-group/CloudSync.git
    ```

3. Install dependencies

    Run the following commands to install dependencies required by ClouSync.

    ```bash
    cd Cloudsync/
    pip install -r cloudsync/requirements.txt
    ```

## Usage

For the first run, one needs to rename `cloudsync/cos_config.example.py` to `cloudsync/cos_config.py`.
Then fill in the contents of this file with your personal computer local paths and cloud storage keys.

`cloudsync/cos_config.py` has two dictionaries. If you use OSS as cloud object storage, please modify the dictionary named `ali`. Instead, the dictionary named `tencent` should be modified.

After filling in the configuration file, run the following command to start the synchronization process.

```bash
python3 cloudsync/cloudsync.py -s <ali|tencent>
```

## Contributing

Please feel free to hack on CloudSync!
