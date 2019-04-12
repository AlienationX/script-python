# coding=utf-8
# python 2.7

import pymysql
import pymssql
import datetime
import time
import logging
import traceback
import contextlib
import subprocess
from multiprocessing import Pool, Process

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

program_path = "C:/Python27/Program/"
log_name = "{program_path}/logs/data_sync_{date}.log".format(program_path=program_path, date=datetime.datetime.now().strftime("%Y-%m-%d"))
# logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s %(message)s', filename=log_name)
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s %(message)s')

######################################################################################################
_commit_num = 10000
_process_num = 6
_retry_time = 10
_timeout = 180


class TwoDB(object):
    def __init__(self, cursor_type='list'):
        mssql_db_config = {
            'host': 'localhost',
            'user': 'sa',
            'password': 'sa',
            'database': 'pms',
            'port': 1433,
            'charset': 'utf8',
        }
        mysql_db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'root',
            'database': 'pms',
            'port': 3306,
            'charset': 'utf8',
        }
        self.mssql_conn = pymssql.connect(**mssql_db_config)
        self.mysql_conn = pymysql.connect(**mysql_db_config)
        if cursor_type == 'list':
            self.mssql_cur = self.mssql_conn.cursor()
            self.mysql_cur = self.mysql_conn.cursor()
        elif cursor_type == 'dict':
            self.mssql_cur = self.mssql_conn.cursor(as_dict=True)
            self.mysql_cur = self.mysql_conn.cursor(cursor=pymysql.cursors.DictCursor)
        else:
            raise Exception('cursor type error !')

    def __enter__(self):
        logging.debug("exec __enter__")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.mssql_cur:
            self.mssql_cur.close()
        if self.mysql_cur:
            self.mysql_cur.close()
        if self.mssql_conn:
            # self.mssql_conn.commit()
            self.mssql_conn.close()
        if self.mysql_conn:
            self.mysql_conn.commit()
            self.mysql_conn.close()
        logging.debug("connect closed")


def db_conn_test():
    pass


def get_all_tables():
    sql = "select name from sys.tables where name not in ('SysLogs') order by name"
    with TwoDB() as twodb:
        twodb.mssql_cur.execute(sql)
        rows = twodb.mssql_cur.fetchall()
    all_tables = [x[0] for x in rows]
    logging.info(all_tables)
    return all_tables


def create_table(table_name):
    sql = """
    select t.name as table_name,
           s.name as column_name,
           case when s1.name in ('int','tinyint','bigint','date','datetime','text') then s1.name
                when s1.name in ('char','varchar') then s1.name + '(' + case when s.max_length<0 then '8000' else cast(s.max_length as varchar(10)) end +')'
                when s1.name in ('nchar','nvarchar') then substring(s1.name,2,len(s1.name)) + '(' + case when s.max_length<0 then '8000' else cast(s.max_length as varchar(10)) end +')'
                when s1.name='money' then 'decimal(18,2)'
                when s1.name='bit' then 'varchar(10)'
                when s1.name='timestamp' then 'blob'
                else 'varchar(255)' end as column_type,
            s1.name as raw_type,
            s.max_length as raw_length,
            s.precision as raw_precision,
            s.*
       from sys.tables t
       left join sys.columns s on t.object_id=s.object_id
       left join sys.types s1 on s.user_type_id=s1.user_type_id
      where t.type='U' and t.name='{table_name}'
      order by t.name,s.column_id
    """.format(table_name=table_name)
    drop_script = "drop table if exists `{}`".format(table_name)
    create_script = "create table if not exists `{table_name}` (".format(table_name=table_name)
    with TwoDB('dict') as twodb:
        twodb.mssql_cur.execute(sql)
        rows = twodb.mssql_cur.fetchall()
        for row in rows:
            create_script += "`{0}` {1},".format(row['column_name'], row['column_type'])
        create_script = create_script[:-1]
        create_script += ") ENGINE=MyISAM CHARSET=utf8;"
        create_script = create_script.lower()
        logging.info(drop_script)
        logging.info(create_script)
        twodb.mysql_cur.execute(drop_script)
        twodb.mysql_cur.execute(create_script)
    logging.info("{} has been created".format(table_name))


# def insert_date(self, table_name):
#     """一次insert全部数据，数据量大的话会很慢很慢，卡死状态"""
#     sql = "select * from {}".format(table_name)
#     self.mssql_cur_list.execute(sql)
#     rows = self.mssql_cur_list.fetchall()
#     rows_count = self.mssql_cur_list.rowcount
#     if rows_count == 0:
#         logging.info("{} no data, skip.".format(table_name))
#         return
#     logging.info("{} total rows: {}".format(table_name, rows_count))
#     cols_count = len(rows[0])
#     self.mysql_cur_dict.execute("truncate table {}".format(table_name))
#     effect_rows = self.mysql_cur_dict.executemany("insert into {} values ({})".format(table_name, ("%s," * cols_count)[:-1]), rows)
#     logging.info("insert into {} {} data succeeded".format(table_name, effect_rows))


def insert_data_batch(table_name):
    """
    串行 total：367311
    commit_num=1000    2:16
    commit_num=10000   2:04
    """
    sql = "select * from {}".format(table_name)
    with TwoDB() as twodb:
        twodb.mssql_cur.execute(sql)
        rows = twodb.mssql_cur.fetchall()
        rows_count = twodb.mssql_cur.rowcount
        if rows_count == 0:
            logging.info("{} no data, skip.".format(table_name))
            return
        logging.info("{} total rows: {}".format(table_name, rows_count))
        cols_count = len(rows[0])
        twodb.mysql_cur.execute("truncate table `{}`".format(table_name))
        # for i in range(0,rows_count,self.commit_num):
        #     j=i+self.commit_num
        #     if j > rows_count:
        #         j = rows_count
        #     effect_rows = self.mysql_cur_dict.executemany("insert into {} values ({})".format(table_name, ("%s," * cols_count)[:-1]), rows[i:j])
        #     logging.info("insert into {} {} data succeeded".format(table_name, effect_rows))
        i = 0
        while rows_count > i:
            j = i + _commit_num
            if j > rows_count:
                j = rows_count
            twodb.mysql_cur.executemany("insert into `{}` values ({})".format(table_name, ("%s," * cols_count)[:-1]), rows[i:j])
            twodb.mysql_conn.commit()
            logging.info("insert into {} {}({}%) data succeeded".format(table_name, j, '%0.0f' % (float(j) / rows_count * 100)))
            i += _commit_num


# """数据并行插入，有表锁，搁浅"""
# def get_rows(table_name):
#     sql = "select * from {}".format(table_name)
#     with TwoDB() as twodb:
#         twodb.mssql_cur.execute(sql)
#         rows = twodb.mssql_cur.fetchall()
#         rows_count = twodb.mssql_cur.rowcount
#         if rows_count == 0:
#             logging.info("{} no data, skip.".format(table_name))
#             return
#     logging.info("{} total rows: {}".format(table_name, rows_count))
#     return rows
#
#
# def insert_data_tmp(rows, rows_count, i):
#     cols_count = len(rows[0])
#     with TwoDB() as twodb:
#         j = i + _commit_num
#         if j > rows_count:
#             j = rows_count
#         twodb.mysql_cur.executemany("insert into {} values ({})".format(table_name, ("%s," * cols_count)[:-1]), rows[i:j])
#         twodb.mysql_conn.commit()
#         logging.info("insert into {} {}({}%) data succeeded".format(table_name, j, '%0.0f' % (float(j) / rows_count * 100)))
#
#
# def insert_data_batch_parallel(rows):
#     rows_count = len(rows)
#     p = Pool(_process_num)
#     for i in range(0, rows_count, _commit_num):
#         p.apply_async(func=insert_data_tmp, args=(rows, rows_count, i,))
#     p.close()
#     p.join()

def create_and_insert(table_name):
    try:
        create_table(table_name)
        insert_data_batch(table_name)
        return ""
    except:
        msg = str(traceback.format_exc())
        logging.error(table_name + "\n" + msg)
        return table_name


# datax 执行方式，因为也会出现任务卡死的bug（sqlserver服务端设置问题），废弃。
def exec_datax(table_name):
    cmd = """ python  {program_path}/datax/bin/datax.py {program_path}/mssql2mysql.json -p"-DtableName={table_name}" > {program_path}/logs/details/{table_name}.log """.format(program_path=program_path, table_name=table_name)
    try:
        subprocess.check_call(cmd, shell=True)
        logging.info(table_name + " successful")
        return ""
    except subprocess.CalledProcessError:
        msg = str(traceback.format_exc())
        logging.error(table_name + "\n" + msg)
        return table_name


def time_monitor():
    """给task增加时间监控，超时也会结束task_main函数"""
    time.sleep(_timeout)


def task_main(table_name):
    try:
        create_table(table_name)
        insert_data_batch(table_name)
    except:
        msg = str(traceback.format_exc())
        logging.error("task_main ----> " + table_name + "\n" + msg)


def single_task(table_name):
    result_flag = 0
    result_str = table_name
    try:
        for i in xrange(1, _retry_time + 1):
            s_time = insert_table_log(table_name)

            time_process = Process(target=time_monitor)
            task_process = Process(target=task_main, args=(table_name,))

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
                time.sleep(10)
                if not task_process.is_alive():
                    logging.info("{} normal end".format(table_name))
                    result_flag = 1
                    break
                if not time_process.is_alive():
                    logging.error("{} timeout".format(table_name))
                    break

            if result_flag == 1:
                update_table_log(table_name, s_time, i)
                result_str = ""
                break
        return result_str
    except:
        msg = str(traceback.format_exc())
        logging.error(table_name + "\n" + msg)
        return result_str


def create_log_table():
    create_sql = """
    create table if not exists data_sync_log (
        id int,
        dt char(10),
        start_time varchar(64),
        end_time varchar(64),
        exec_time varchar(64),
        message varchar(200),
        flag int,
        KEY idx_id (id)
    ) engine=innodb charset=utf8;
    
    create table if not exists data_sync_detail_log (
        id int,
        dt char(10),
        table_name varchar(64),
        start_time varchar(64),
        end_time varchar(64),
        exec_time varchar(64),
        flag int,
        PRIMARY KEY (id),
        UNIQUE KEY idx_unique (dt,table_name)
    ) engine=innodb charset=utf8;
    """
    with TwoDB() as twodb:
        twodb.mysql_cur.execute(create_sql)
        logging.info("log table created")


def insert_table_log(table_name):
    start_time = datetime.datetime.now()
    with TwoDB() as twodb:
        twodb.mysql_cur.execute("replace into data_sync_detail_log (dt,table_name,start_time) values('{}','{}','{}')".format(
            start_time.strftime("%Y-%m-%d"),
            table_name,
            start_time
        ))
    logging.info("insert {} log".format(table_name))
    return start_time


def update_table_log(table_name, start_time, i):
    end_time = datetime.datetime.now()
    exec_time = format_seconds(start_time, end_time)
    with TwoDB() as twodb:
        twodb.mysql_cur.execute("update data_sync_detail_log set end_time='{}',exec_time='{}',flag={} where dt='{}' and table_name='{}'".format(
            end_time,
            exec_time,
            i,
            end_time.strftime("%Y-%m-%d"),
            table_name
        ))
    logging.info("update {} log".format(table_name))


def insert_log():
    max_sql = "select max(id) from data_sync_log"
    with TwoDB() as twodb:
        twodb.mysql_cur.execute(max_sql)
        max_id = twodb.mysql_cur.fetchone()[0]
        if max_id is None:
            max_id = 1
        else:
            max_id += 1
        start_time = datetime.datetime.now()
        twodb.mysql_cur.execute("insert into data_sync_log (id,dt,start_time) values({},'{}','{}')".format(max_id, start_time.strftime("%Y-%m-%d"), start_time))
    logging.info("start insert data_sync_log")
    return max_id, start_time


def update_log(max_id, start_time):
    end_time = datetime.datetime.now()
    exec_time = format_seconds(start_time, end_time)
    with TwoDB() as twodb:
        twodb.mysql_cur.execute("update data_sync_log set end_time='{}',exec_time='{}',message='{}',flag={} where id={}".format(end_time, exec_time, "成功，详细日志所在路径" + log_name.replace('\\', '\\\\'), 1, max_id))
    logging.info("insert data_sync_log end")


def format_seconds(start_time, end_time):
    exec_time = (end_time - start_time).seconds
    m, s = divmod(exec_time, 60)
    h, m = divmod(m, 60)
    exec_time = "%02d:%02d:%02d" % (h, m, s)
    return exec_time


if __name__ == "__main__":
    create_log_table()
    max_id, start_time = insert_log()
    failed_tables = []

    # OperationalError: (20004, 'DB-Lib error message 20004, severity 9:\nRead from the server failed\nNet-Lib error during Unknown error (10054)
    # pymssql会卡住，2个小时后才会报如上错误，使用datax也会长时间卡住任务。可能是sqlserver服务器设置问题，需要增加timeout处理逻辑。

    # p = Pool(_process_num)
    # for table_name in get_all_tables():
    #     # err_table = p.apply_async(func=create_and_insert, args=(table_name,))
    #     # err_table = p.apply_async(func=exec_datax, args=(table_name,))
    #     # err_table = p.apply_async(func=single_task, args=(table_name,))
    #     failed_tables.append(err_table.get())
    # p.close()
    # p.join()

    for table_name in get_all_tables():
        # err_table = create_and_insert(table_name)
        # err_table = exec_datax(table_name)
        err_table = single_task(table_name)
        failed_tables.append(err_table)

    failed_tables = [x for x in failed_tables if x]
    if len(failed_tables) > 0:
        logging.error("--- Failed Task ---")
        for res in failed_tables:
            logging.error(res.strip())
    else:
        update_log(max_id, start_time)
    logging.info("--------------------------end--------------------------\n")
