#!/usr/bin/env python
# coding=utf-8

"""
HUE无法导出大批量的数据，最多5w数据左右
从hive导出数据到excel，但是excel只支持100w的数据
从hive导出数据到csv，需要处理逗号分隔符导致串列的问题
"""

import argparse
import logging
import pandas as pd
from sqlalchemy import create_engine, pool

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s %(message)s')


def get_sql(file):
    with open(file, "r") as f:
        sql_str = f.read()
    return sql_str


def get_data(sql):
    # sql = "select * from data_xiaonuan_final.hospital_base_information"
    con_str = "hive://{username}@{host}:{port}/default".format(username="lshu", host="10.15.1.16", port=10000)
    con = create_engine(con_str, poolclass=pool.NullPool)
    df = pd.read_sql(sql, con)
    print(df.head())
    return df


def batch_cast_sep(df):
    df_casted = df.apply(lambda x: x.replace(",", "，"))
    return df_casted


def to_xlsx(df):
    writer = pd.ExcelWriter('result.xlsx')
    df.to_excel(writer, 'Sheet1', index=False)
    writer.save()


def to_csv(df):
    cast_df = batch_cast_sep(df)
    cast_df.to_csv('result.csv', encoding='utf_8_sig', index=False)


if __name__ == "__main__":
    # logging.info("start")
    pars = argparse.ArgumentParser()
    pars.add_argument("-f", "--file", required=True, help="sql文件")
    args = pars.parse_args()
    print(args)
    sql_str = get_sql(args.file)
    df = get_data(sql_str)
    # to_xlsx(df)
    to_csv(df)
