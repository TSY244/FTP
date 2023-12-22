import ftplib
import multiprocessing
import os
import sys
import tarfile
import extract_data
import re
import time
from threading import Lock
# import logger
from loguru import *

logger.add(f"log/{os.path.basename(__file__)}/error.log", rotation="5 MB", encoding="utf-8",
           filter=lambda record: record["level"].name.upper() == "ERROR")
logger.add(f"log/{os.path.basename(__file__)}/critical.log", rotation="5 MB", encoding="utf-8",
           filter=lambda record: record["level"].name.upper() == "CRITICAL")

file_size = 0
file_name = ""
# 用于存放已经下载过的文件的hash值
file_name_list = []
# 文件名队列锁
locker = Lock()
# 用于标记是否解压完成
flag = False
# flag锁
flag_locker = Lock()

# 读取已经下载过的文件
if os.path.exists("Downloaded.txt"):
    with open("Downloaded.txt", "r") as f:
        for line in f.readlines():
            file_name_list.append(line.strip())



def check_args(arg) -> bool:
    '''
    检测传入的参数是否合格
    '''
    if len(arg) == 2:
        return True
    return False


def make_folder(folder_name):
    '''
    创建文件夹
    folder_name: 文件夹名
    '''
    # 检测文件夹是否存在
    if not os.path.exists(folder_name):
        os.mkdir(folder_name)


def ftp_download_file(ftp, path="/", username="", passwd="", queue: multiprocessing.Queue = None):
    '''
    下载ftp服务器上的文件
    ftp: ftp服务器地址
    username: ftp用户名
    passwd: ftp密码
    path: ftp路径
    '''
    try:
        global file_size
        global file_name
        # logger.debug(f"ftp:{ftp},path:{path},username:{username},passwd:{passwd}")
        ftp = ftplib.FTP(ftp)
        ftp.login()
        # 进入到路径
        ftp.cwd(path)
    except ftplib.error_reply as e:
        logger.error(f"ftp服务器连接失败：{e}")
        sys.exit(1)
    except ftplib.error_perm as e:
        logger.error(f"ftp用户名或密码错误：{e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"ftp连接失败\路径错误：{e}")
        sys.exit(1)
    make_folder("ftp_download")
    while True:
        # 遍历服务器上的文件
        for file in ftp.nlst():
            # 获取文件名，是否是2018-至今
            # logger.debug(f"文件名：{file}")
            if re.match(r"20(1[8-9]|[2-9][0-9])..\.tar\.gz", file) and file not in file_name_list:
                # print("正在下载文件：",file)
                logger.info(f"正在下载文件：{file}")
                # 下载文件
                ftp.retrbinary("RETR " + file, open("ftp_download/" + file, 'wb').write)
                # 下载完成后，将文件名存入列表
                file_name_list.append(file)
                logger.success(f"文件{file}下载完成")
                # 将文件名写入文件
                with open("Downloaded.txt", "a") as f:
                    f.write(file + "\n")
                extract_files("ftp_download/", file, queue)
        # 停止1小时
        time.sleep(3600)


def extract_files(path, filename, queue: multiprocessing.Queue = None):
    '''
    解压文件
    path: 文件路径
    filename: 文件名
    '''
    # 解压下载的文件
    make_folder("extract_files")
    with tarfile.open(path + filename, 'r:gz') as tar:  # r:gz 读取gz压缩包
        tar.extractall("extract_files")
        # 解压完成后，删除压缩包
        logger.success(f"{file_name} ""解压文件成功")
        # 给queue添加一个任务
        queue.put(file_name) # 通知任务添加者，已经添加了一个任务
    os.remove(path + filename)

# ftp_download_file(ftp="ftp.argo.org.cn", path="/pub/ARGO/global/core",username="", passwd="")
