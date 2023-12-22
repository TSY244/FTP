from concurrent.futures import ThreadPoolExecutor
from sys import exit
from time import sleep
import threading 
from loguru import *
import os
from multiprocessing import Queue
'''
    线程池
'''



logger.add(f"log/{os.path.basename(__file__)}/error.log",rotation="5 MB",encoding="utf-8",filter=lambda record: record["level"].name.upper() == "ERROR")
logger.add(f"log/{os.path.basename(__file__)}/critical.log",rotation="5 MB",encoding="utf-8",filter=lambda record: record["level"].name.upper() == "CRITICAL")
# logger.add(f"log/{os.path.basename(__file__)}/info.log",rotation="1 MB",encoding="utf-8",filter=lambda record: record["level"].name.upper() == "INFO")

class ThreadPool():
    def __init__(self,max_workers:int):
        self.__max_workers=max_workers
        self.__tasks=None
        self.__thread_pool=None
        self.__task_lock=threading.Lock()
        # self.__worker=worker

    def __start(self):
        '''
            用于初始化线程池
        '''
        self.__thread_pool=ThreadPoolExecutor(self.__max_workers)
        


    def set_tasks(self,tasks:list):
        '''
            设置任务
        '''
        with self.__task_lock:
            self.__tasks=tasks

    def manager(self, file_name_queue:list,worker, *args):
        '''
            管理者，用于添加任务，检查任务队列，调度线程
        '''
        # 初始化线程池
        
        self.__start()
        # last_file_name=None
        while True:
            # 打印信息
            self.print_infomation()
            logger.info(f"现在一共有{len(file_name_queue)}个任务")
            with open("now_info.txt", "w") as f:
                f.write(f"现在一共有{len(file_name_queue)}个任务,现在一共有{len(self.__thread_pool._threads)}个线程,现在一共有{self.__thread_pool._work_queue.qsize()}个线程正在等待工作")
            # 添加锁
            if self.__task_lock.acquire():
                # 如果任务队列为空，就休眠
                if len(file_name_queue) == 0:
                    logger.info("任务队列为空，休眠")
                    self.__task_lock.release()
                    # wait_file_name_queue.put(1)
                    sleep(5)
                else:
                    # if self.__task_lock.acquire():
                    num_tasks = min(len(file_name_queue), 20)
                    task_batch = [file_name_queue.pop() for _ in range(num_tasks)]
                    self.__task_lock.release()
                    for _ in range(num_tasks):
                        self.__thread_pool.submit(worker, task_batch.pop(), *args)
                        # logger.info(f"添加任务{task}成功")
            # 休眠
            sleep(5)
            
    def print_infomation(self):
        '''
            打印信息
        '''
        logger.info(f"现在一共有{len(self.__thread_pool._threads)}个线程")
        # 工作线程
        logger.info(f"现在一共有{self.__thread_pool._work_queue.qsize()}个线程正在等待工作")

    def set_max_workers(self, max_workers: int):
        '''
            设置最大线程数
        '''
        self.__max_workers = max_workers
        # 修改线程池
        self.__thread_pool._max_workers=max_workers

    def add_task(self,task):
        '''
            添加任务
        '''
        self.__tasks.append(task)
        logger.info(f"添加任务{task}成功")

    def shutdown(self):
        '''
            关闭线程池
        '''
        self.__thread_pool.shutdown()
        

