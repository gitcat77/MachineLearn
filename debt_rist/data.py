# -*- coding: UTF-8 -*-
import pymssql
import pandas as pd

conn= pymssql.connect('180.101.184.27', 'zhaiwu', 'datamind123', 'AnalysisData') #服务器名,账户,密码,数据库名

with conn:
    # print(conn.open)
    cur=conn.cursor()
    sql='select * from Lgd_Company_AssetsLiabilities'
    cur.execute(sql)
    des=cur.description
    print("表的描述",des)


def yue():
    print("约你")
    print("约她")
    return
    print("约谁") #这句话不会被执行
yue()