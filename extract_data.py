import os
import re
import sys
import threading
import time
# import logger
import multiprocessing
from loguru import *
from pymysqlpool import ConnectionPool
import configparser

'''
    用于提取数据，将数据插入数据库
    任务添加到任务队列中
'''

logger.add(f"log/{os.path.basename(__file__)}/error.log", rotation="5 MB", encoding="utf-8",
           filter=lambda record: record["level"].name.upper() == "ERROR")
logger.add(f"log/{os.path.basename(__file__)}/critical.log", rotation="5 MB", encoding="utf-8",
           filter=lambda record: record["level"].name.upper() == "CRITICAL")
# logger.add(f"log/{os.path.basename(__file__)}/warning.log", rotation="5 MB", encoding="utf-8",
        #    filter=lambda record: record["level"].name.upper() == "WARNING")

# inster
insert_sql = "insert into {tablename}(device_id,cycle_num,report_time,lon,lat,pressure,p_flag,temperature,t_flag,salinity,s_flag) \
    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
# create table
create_table_sql = "create table if not exists {tablename} (\
    id int not null auto_increment,\
    device_id int not null,\
    cycle_num int not null,\
    report_time varchar(20) not null,\
    lon varchar(20) not null,\
    lat varchar(20) not null,\
    pressure varchar(20) not null,\
    p_flag varchar(20) not null,\
    temperature varchar(20) not null,\
    t_flag varchar(20) not null,\
    salinity varchar(20) not null,\
    s_flag varchar(20) not null,\
    primary key(id)\
)engine=InnoDB default charset=utf8;"
# use database
use_database = "use argo"
# 对 device_id, cycle_num, report_time, pressure 建一个组合索引，防止重复插入，注意检查是否存在联合索引
index_sql = "alter table {tablename} add index argo_index(device_id,cycle_num,report_time,pressure)"
# 联合查询一次
index_select_sql = "select * from {tablename} where device_id={device_id} and cycle_num={cycle_num} and report_time='{report_time}' and pressure='{pressure}'"
# 检查是否存在联合索引
inspect_index_sql = "select count(*) from information_schema.statistics where table_schema = 'argo' and table_name = '{tablename}' and index_name = 'argo_index'"

# 记录已经添加到任务队列中的文件或文件夹
extracted_files = [] # 防止已经提取却没有插入数据库的文件被重复提取

# 任务队列
file_name_queue = []
# 任务队列锁
locker_for_file_name_queue=threading.Lock()

config = {}
# 读取config
file = configparser.ConfigParser()
file.read("config.ini", encoding="utf-8")
for i in file["DB"]:
    if i == "port" or i == "max_pool_size" or i == "pool_resize_boundary":
        config[i] = int(file["DB"][i])
        continue
    config[i] = file["DB"][i]
    # logger.debug(f"config:{i}:{file['DB'][i]}")

# 创建数据库连接池
db_pool = ConnectionPool(**config)

# 创建文件夹extract_files
if not os.path.exists("extract_files"):
    os.mkdir("extract_files")

# 数据提取
def extract_data(args):
    '''
        提取数据，将数据插入数据库
    '''
    with db_pool.cursor() as db:
        # 获取参数
        try:
            path, filename = args[0], args[1]
        except Exception as e:
            logger.error(e)
            sys.exit(1)
        # use_db()
            
        try:

            # 创建数据表
            db.execute(create_table_sql.format(tablename="argo_" + path[-7:-1]))
            # 检查是否存在联合索引
            db.execute(inspect_index_sql.format(tablename="argo_" + path[-7:-1]))
            # 获取查询结果
            result = db.fetchone()
            if result["count(*)"] == 0:
                # 创建联合索引
                db.execute(index_sql.format(tablename="argo_" + path[-7:-1]))
                # db.commit()
        except Exception as e:
            logger.error(e)
            sys.exit(1)
        device_id, cycle_num = filename.split(".")[0].split("_")
        
     
        report_time = ""   # 报告时间   更新时间？创建时间？
        lon = ""           # 经度
        lat = ""           # 纬度 
        pressure = ""      # 压强
        p_flag = ""        # 压强标记
        temperature = ""   # 温度
        t_flag = ""        # 温度标记
        salinity = ""      # 盐度
        s_flag = ""        # 盐度标记

        # 对每一个文件进行数据提取
        with open(path + filename, "r") as f:
            flag = False # 用于标记是否到了数据部分
            for line in f.readlines():
                # 匹配分界线
                if re.match(r"============", line):
                    flag = True
                    continue
                if not flag:
                    if re.search(r"DATE UPDATE", line):
                        report_time = line.split(":")[1].strip()  # :20230918120040
                        # print(report_time)
                    elif re.search(r"LONGITUDE", line):
                        lon = line.split(":")[1].strip()
                    elif re.search(r"LATITUDE", line):
                        lat = line.split(":")[1].strip()
                else:
                    datas = line.split()
                    pressure = datas[1]
                    p_flag = datas[2]
                    temperature = datas[4]
                    t_flag = datas[5]
                    salinity = datas[7]
                    s_flag = datas[8]

                    # 获取完了数据 插入数据库
                    try:
                        #  device_id, cycle_num, report_time, pressure 做一次查询，没有记录再insert
                        # 先查询一遍

                        db.execute(index_select_sql.format(tablename="argo_" + path[-7:-1], device_id=device_id,
                                                           cycle_num=cycle_num, report_time=report_time,
                                                           pressure=pressure))

                        # 获取查询结果
                        result = db.fetchone()
                        if result:
                            logger.warning(f"数据已经存在：{result}")
                            continue
                        else:
                            # 插入数据
                            db.execute(insert_sql.format(tablename="argo_" + path[-7:-1]), (
                            device_id, cycle_num, report_time, lon, lat, pressure, p_flag, temperature, t_flag,
                            salinity, s_flag))
                            # logger.info(f"数据插入成功：{device_id, cycle_num, report_time, lon, lat,pressure, p_flag, temperature, t_flag, salinity, s_flag}")
                    except Exception as e:
                        logger.error(e)
                        sys.exit(1)
        try:                        
            # 删除文件
            os.remove(path + filename)
            logger.info(f"文件{path[-7:-1]}\{filename}已经提取完成,并删除")
            # 记录已经提取的文件
            with open("extracted_files.txt", "a") as f:
                f.write(path[-7:-1]+"\\"+filename + "\n") 
        except Exception as e:
            logger.error(e)
            sys.exit(1)
            


def traverse_folder(folder_name, decompression_queue: multiprocessing.Queue):
    '''
        遍历解压文件夹，将文件名添加到任务队列中
    '''
    # 定时遍历文件夹里里面的内容，如果有新的文件就添加到任务队列中
    logger.success("任务队列启动成功")
    while True:
        # 如果存在文件夹，就遍历文件夹
        if len(os.listdir(folder_name)) == 0:
            decompression_queue.get()  # 阻塞 只有当有新的文件下载完成之后才会继续执行

        # 获取当前文件的绝对路径 
        abspath = os.path.dirname(__file__) + "\\"
        path = abspath + folder_name + "\\"
        
        # 获取文件夹下的所有文件
        files = os.listdir(path)
        # 将文件从小到大排序
        files.reverse()
        # 遍历文件夹下的所有文件
        for file in files:
            # print(file)
            if file  in extracted_files:
                continue
            # 如果是文件夹，则进入文件夹继续遍历
            if os.path.isdir(path + file):
                # 如果文件夹为空，则删除文件夹
                if len(os.listdir(path+file))==0:
                    os.rmdir(path+file)
                    continue
                # 如果文件夹不为空，则将文件夹下的文件添加到任务队列中
                extracted_files.append(file)
                for f in os.listdir(path + file):
                    # if locker_for_file_name_queue.acquire():
                    file_name_queue.append((path + file + "\\", f))
                        # locker_for_file_name_queue.release()
            else:
                extracted_files.append(file)
                # if locker_for_file_name_queue.acquire():
                file_name_queue.append((path, file))
                    # locker_for_file_name_queue.release()
        time.sleep(10)

        # break

# extract_data(("D:\\code\\vsc\\dvid_Python\\FTP\\extract_files\\201802\\","1900728_368.dat"))
# extract_data(["extract_files//201801//","7900299_102.dat"])
# traverse_folder("extract_files", multiprocessing.Queue())
