from pymysqlpool.connection import *
from pymysqlpool import ConnectionPool
import configparser
import loguru
# 读取config

config = {
    'pool_name': 'test',
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'root',
    'database': 'argo',
    'pool_resize_boundary': 50,
    'enable_auto_resize': True,
    # 'max_pool_size': 10
}

# 创建数据库连接池
db_pool=ConnectionPool(**config)

with db_pool.cursor() as db:
    ret=db.execute("show tables")
    print(ret)
    print(db.fetchall())





# from .pymysqlpool.pool import PoolContainer





# import os
# import sys
# import tarfile
# import threading
# import time
# import logger
# import ftp_download_file

# flag=False
# flag_locker=threading.Lock()



# def extract_files(path, filename):
#     '''
#     解压文件
#     path: 文件路径
#     filename: 文件名
#     '''
#     if ftp_download_file.flag_locker.acquire():
#         global flag
#         ftp_download_file.flag_locker.release()
#     # 解压下载的文件
#     ftp_download_file.make_folder("extract_files")
#     with tarfile.open(path + filename, 'r:gz') as tar:  # r:gz 读取gz压缩包
#         tar.extractall("extract_files") 
#         # 解压完成后，删除压缩包
#         logger.info(f"{filename} 解压文件成功")
#         if ftp_download_file.flag_locker.acquire():
#             flag=True
#             ftp_download_file.flag_locker.release()
#     os.remove(path + filename)

# # def ftp_download_file(ftp, username, passwd, path):
# # 解压文件
# extract_files("ftp_download/", filename="201801.tar.gz")

# print(2+(12,))
# import ftplib
# import os
# import tarfile
# ftp=ftplib.FTP("ftp.argo.org.cn",user="",passwd="")
# ftp.login()
# ftp.cwd("/pub/ARGO/global/core")
# ftp.retrlines('LIST')
# file=input("请输入要下载的文件名：")
# # 创建文件夹
# if not os.path.exists("ftp_download"):
#     os.mkdir("ftp_download")
# # 下载文件
# ftp.retrbinary("RETR " + file, open("ftp_download/" + file, "wb").write)

# # 创建文件夹
# if not os.path.exists("extract_files"):
#     os.mkdir("extract_files")
# # 解压文件
# with tarfile.open("ftp_download/"+"202309.tar.gz", 'r:gz') as tar:  # r:gz 读取gz压缩包
#     tar.extractall("extract_files")
#     tar.close()