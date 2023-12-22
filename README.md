# 简介

流程介绍：

首先组成为两个进程，姑且叫他们为下载解压进程和数据提取进程（主进程）

项目启动的时候，主进程启动，然后启动一个下载解压进程

## 主进程

会使用一个线程池用于管理分配任务，在主线程里会分出去一个manager()管理线程用于管理线程池，然后主线程会阻塞在任务添加处，直到下载解压完成之后，任务添加到任务队列，然后manager()会察觉到任务的添加，会根据任务的多少submit()到线程池

## 下载解压进程

这个进程 主要是用于下载和解压文件，为什么不给这个分一个线程的原因是，会导致解压速度极其的慢

会自动检测是否有新的文件出现，有的话就下载并且解压文件

# main.py

> 主程序   用于启动程序（启动下载解压进程，启动线程池管理者，添加任务）

# extract_data.py

> 用于将文件添加到任务队列中，执行数据提取，这里面的extract_data()也是线程池里面的worker()

## ftp_download_file.py

> 用于将文件从ftp当中下载下来，然后进行解压，把所有的文件解压完成之后通知添加者

# thread_pool.py

> 这个是manager()所在的文件，用于调度线程池工作

# 配置文件填写

## config.ini

```ini
[FTP]
ftp=ftp.argo.org.cn
path=/pub/ARGO/global/core
user=""
passwd=""


[DB]
; pool名称
pool_name=argo_pool 
host=localhost
user=root
passwd=root
; 数据库名
db=argo
; 数据库的端口
port=3306
; 下面默认
pool_resize_boundary=5 
enable_auto_resize=True 
; pool_max_size=20 # 连接池优先最大连接数
```

针对于FTP指定对应的服务器

针对于DB建议，只修改以下参数

> db=argo
>
> port=3306
>
> user=root
>
> passwd=root

# 使用方法

考虑到需要一直运行，就必须解决崩溃了重启，windows,linux采取不同的方式

## windows

1. 下载

   ```python
   pip install supervisor-win
   ```

2. 配置文件

   ```python
   [program:cancel]
   command=D:\\PYTHON\\python38\\python.exe D:\\code\\vsc\\dvid_Python\\FTP\\main.py
       
   [supervisord]
   nodaemon=true
    
   [supervisorctl]
   ```

3. 启动

   ```python
   supervisord -c D:\CODE\tdx_easytrader\supervisord.conf
   ```

## Linux

采用shell脚本，一直检测是否存在这个这个进程，如果发现这个进程没有了，就重新启动这个进程







