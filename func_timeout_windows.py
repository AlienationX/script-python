#!/usr/bin/env python
# coding=utf-8

import multiprocessing
import time
import datetime


def time_monitor():
    """给task增加时间监控，超时也会结束task_main函数"""
    print("开始监控" + str(datetime.datetime.now()))
    time.sleep(3)
    print("结束监控" + str(datetime.datetime.now()))


def task_main():
    print("开始执行" + str(datetime.datetime.now()))
    time.sleep(5)
    print("结束执行" + str(datetime.datetime.now()))


def single_task():
    time_process = multiprocessing.Process(target=time_monitor)
    task_process = multiprocessing.Process(target=task_main)

    # 设置守护进程
    # 默认情况下，主进程会等待子进程执行结束，程序再退出，既主进程结束。
    # 设置守护进程，主进程结束，子进程被销毁。
    # 注意点：主进程里的子进程如果有一个没有设置守护，那么主进程会等待这个子进程执行结束，因为主进程没有结束，所以守护进程不会被销毁。
    # 注意点：设置守护进程( daemon )，要在start() 之前设置，否则会报错（RuntimeError: cannot set daemon status of active thread）

    time_process.daemon = True
    task_process.daemon = True

    time_process.start()
    task_process.start()

    while True:
        time.sleep(1)
        if not task_process.is_alive():
            print "任务正常结束"
            break
        if not time_process.is_alive():
            print "任务超时结束"
            break

    print datetime.datetime.now()


if __name__ == '__main__':
    single_task()
