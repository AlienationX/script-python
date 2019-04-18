#!/usr/bin/env python
# coding=utf-8

"""
读取一个Excel文件数据，按照某个字段拆分数据到多个sheet中
"""

import pandas as pd


def excel_split():
    df = pd.read_excel("/Users/MacBook/Downloads/账户押金余额数据_合并.xlsx")  #
    print(df.shape[0])  # 获取行数 shape[1]获取列数
    sheet_list = df["医院名称"].drop_duplicates(keep="first").tolist()
    print(sheet_list)

    writer = pd.ExcelWriter('/Users/MacBook/Downloads/账户押金余额数据_拆分.xlsx')  # 利用pd.ExcelWriter()存多张sheets

    for sheet in sheet_list:
        df[df["医院名称"] == sheet].to_excel(writer, sheet_name=str(sheet), index=False)  # 注意加上index=FALSE 去掉index列

    writer.save()


if __name__ == "__main__":
    excel_split()
