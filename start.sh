# !/bin/bash
# 使用python运行，并且得到运行的pid，然后检测pid是否存在，如果不存在则重新运行


while true
do
    # 检测进程是否存在
    pid=`ps -ef | grep "python3 main.py" | grep -v grep | awk '{print $2}'`
    if [ -z $pid ]; then
        echo "process not exist"
        # 重启进程
        python3 main.py &
    else
        echo "process exist"
    fi
    sleep 1
done