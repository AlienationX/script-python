# coding=utf-8
# python 2.7

# 给函数增加执行时间限制，超时跑出异常，没超时会正常结束任务
# windows平台无效

import time
import datetime
import random
import traceback
from multiprocessing import Pool
import timeout_decorator


@timeout_decorator.timeout(3)
def mytest():
    print datetime.datetime.now()
    time.sleep(5)
    print datetime.datetime.now()


@timeout_decorator.timeout(5)
def exec_task(name):
    print 'Run task %s (%s)...' % (name, datetime.datetime.now())
    start = time.time()
    num = random.randint(1, 10)
    time.sleep(num)
    end = time.time()
    print 'Task %s end         %0.2f seconds.' % (name, (end - start))


def exec_task_catch(name):
    try:
        exec_task(name)
    except:
        print "{name} timeout".format(name=str(name))


def parallel_task():
    a = datetime.datetime.now()
    # output_msg = []
    p = Pool()  # 默认是4
    for i in range(10):
        p.apply_async(exec_task_catch, args=(i,))
    p.close()
    print 'Waiting for all subprocess done...'
    p.join()
    print datetime.datetime.now() - a


if __name__ == "__main__":
    try:
        mytest()
    except:
        print "test error"
        print traceback.format_exc()
    parallel_task()
