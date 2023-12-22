from ftp_download_file import *
import extract_data
import thread_pool
import threading
import configparser
import sys
import configparser
from ftp_download_file import ftp_download_file
import extract_data
import thread_pool
import loguru
import multiprocessing

'''
    主程序 用于启动程序（启动下载解压进程，启动线程池管理者，添加任务）
'''

queue=multiprocessing.Queue()
# wait_file_name_queue=multiprocessing.Queue()

# 初始化日志
loguru.logger.add(f"log/{os.path.basename(__file__)}/error.log",rotation="5 MB",encoding="utf-8",filter=lambda record: record["level"].name.upper() == "ERROR")
loguru.logger.add(f"log/{os.path.basename(__file__)}/critical.log",rotation="5 MB",encoding="utf-8",filter=lambda record: record["level"].name.upper() == "CRITICAL")

max_workers=15

# 创建下载解压进程，解决下载解压慢问题
def download_extract_process(ftps, username, passwd, path,queue: multiprocessing.Queue):
    '''
    ftps: ftp服务器地址
    username: ftp用户名
    passwd: ftp密码
    path: ftp路径
    '''
    # logger.debug(f"ftp:{ftps},path:{path},username:{username},passwd:{passwd}")
    # 创建进程
    p = multiprocessing.Process(target=ftp_download_file, args=(ftps,path, username, passwd,queue))
    p.start()
    logger.success("下载进程启动成功")




def main():
    try:
        # 读取配置文件
        config = configparser.ConfigParser()
        config.read("config.ini", encoding="utf-8")
        # 获取配置文件中的配置
        ftp_config = config["FTP"]
        # mysql_config = config["DB"]
    except Exception as e:
        logger.error(e)
        sys.exit(1)
    
    # 初始化线程池
    pool = thread_pool.ThreadPool(max_workers) # 最大线程数为10
    # pool.set_tasks(tasks=file_name_queue)
    # 启动一个线程用于管理线程池
    if extract_data.locker_for_file_name_queue.acquire():
        pool_thread = threading.Thread(target=pool.manager, args=(extract_data.file_name_queue,extract_data.extract_data))
        pool_thread.start()
        extract_data.locker_for_file_name_queue.release()
    logger.success("线程池启动成功")

    # 创建下载解压进程，解决下载解压慢问题
    download_extract_process(ftps=ftp_config["ftp"], username=ftp_config["user"], passwd=ftp_config["passwd"], path=ftp_config["path"],queue=queue)

    # 添加任务
    extract_data.traverse_folder("extract_files",queue)

if __name__ == '__main__':
    main()



    

    
    

