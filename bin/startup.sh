#!/bin/sh
cd /Application/rep_preload/


restart_receiver() {
    stop_receiver
    start_receiver
}


start_receiver() {
    echo "receiver is running ..."
    nohup ./bin/receiverd > /dev/null 2>&1 &
    sleep 2
    echo "ps aux | grep -v grep | grep -E 'PID|receiverd' --color"
    ps aux | grep -v grep | grep -E 'PID|receiverd' --color
}


stop_receiver() {
    echo "`date  '+%F %T'` killing receiver"
    pid=`ps -eo pid,cmd|grep receiverd|grep -v grep|awk '{print $1}'`
    echo kill -9 $pid
    kill -9 $pid
    sleep 2
    echo "ps aux | grep -v grep | grep -E 'PID|receiverd' --color"
    ps aux | grep -v grep | grep -E 'PID|receiverd' --color
}


restart_router() {
    stop_router
    start_router
}


start_router() {
    echo "router is running ..."
    nohup ./bin/preload_reportd > /dev/null 2>&1 &
    sleep 2
    echo "ps aux | grep -v grep | grep -E 'PID|preload_reportd' --color"
    ps aux | grep -v grep | grep -E 'PID|preload_reportd' --color
}


stop_router() {
    echo "`date  '+%F %T'` killing preload_reportd"
    pid=`ps -eo pid,cmd|grep preload_reportd|grep -v grep|awk '{print $1}'`
    echo kill -9 $pid
    kill -9 $pid
    sleep 2
    echo "ps aux | grep -v grep | grep -E 'PID|preload_reportd' --color"
    ps aux | grep -v grep | grep -E 'PID|preload_reportd' --color
}

restart_all() {
    restart_receiver
    restart_router
}


stop_all() {
    stop_receiver
    stop_router
}

start_all() {
    start_receiver
    start_router
}

case "$1" in
    all-start)
        start_all
    ;;

    all-stop)
        stop_all
    ;;

    all-restart)
        restart_all
    ;;

    receiver-restart)
        restart_receiver
    ;;

    receiver-start)
        start_receiver
    ;;

    receiver-stop)
        stop_receiver
    ;;

    router-restart)
        restart_router
    ;;

    router-start)
        start_router
    ;;

    router-stop)
        stop_router
    ;;
    *)
        echo "Usage: /Application/rep_preload/bin/startup.sh {receiver-[start|stop|restart], router-[start|stop|restart], all-[start|stop|restart]}."
        exit 1
    ;;
esac

exit 0
