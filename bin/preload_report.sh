#!/bin/bash
cd /Application/rep_preload/.venv/


# file="/Application/bermuda/logs/refresh_result.log"
# NOW=$(date  '+%Y-%m-%d %T')
# mtime=`stat -c %Y $file`
# current=`date '+%s'`
# ((halt=$current - $mtime))


if ps -ef | grep preload_reportd | grep -v grep; then
    pid1=`ps -ef | grep preload_reportd | grep -v grep | head -1 | awk '{print $2}'`
    echo "routerd is running!  main process id = $pid1"
else
    echo "routerd is running ..."
    nohup ./bin/preload_reportd > /dev/null 2>&1 &
fi
