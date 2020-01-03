#!/bin/bash
cd /Application/rep_preload


# file="/Application/bermuda/logs/refresh_result.log"
# NOW=$(date  '+%Y-%m-%d %T')
# mtime=`stat -c %Y $file`
# current=`date '+%s'`
# ((halt=$current - $mtime))


if ps -ef | grep receiverd | grep -v grep; then
     pid1=`ps -ef | grep receiverd | grep -v grep | head -1 | awk '{print $2}'`
     echo "receiverd is running!  main process id = $pid1"
else
    echo "receiverd is running ..."
    nohup ./bin/receiverd &
fi
