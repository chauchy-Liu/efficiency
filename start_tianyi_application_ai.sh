#!/bin/bash
JAR_PATH=/root/efficiency/tianyi-intelligent-ai/config/
APP_NAME=TianYiTemplateServerApplication.jar
CHECK_PROCESS="$APP_NAME"

# 停止应用
echo "停止应用: $APP_NAME"
pid=`ps -ef | grep java | grep -v grep | grep -E $CHECK_PROCESS |awk '{print $2}'`
if [ -n "$pid" ]
   then
   kill -9 $pid
   echo "$APP_NAME 已停止，进程号:${pid}"
fi

# 启动应用

cd $JAR_PATH

nohup java -Xms1024m -Xmx1024m -jar ${APP_NAME}  &
