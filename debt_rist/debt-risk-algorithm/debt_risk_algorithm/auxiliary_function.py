# -*- coding: UTF-8 -*-
import pandas as pd
from sklearn.externals import joblib
import psycopg2
import pymssql

def panda_read_sql(db_config, sql):
    logger.info(sql)
    psycopg2模块操作PostgreSQL
    conn = psycopg2.connect(database=db_config['database'], user=db_config['user'],
                            password=base64.b64decode(db_config['password']).decode(),
                            host=db_config['host'], port=db_config['port'])

    try:
        df = pd.read_sql(sql, conn)
        df = df.fillna(0)
        return df
    except BaseException as be:
        logger.error(be)
    finally:
        try:
            conn.close()
        except BaseException as be:
            logger.error(be)

#数据[0,1]标准化
def MaxMinNormalization(x):
    """[0,1]normalization"""
    x=(x-np.min(x))/(np.max(x)-np.min(x))
    return x


#数据[0,1]反向标准化
def MinMaxNormalization(x):
    """[0,1]normalization"""
    x=(np.max(x)-x)/(np.max(x)-np.min(x))
    return x


def load_model(path):
    """加载模型函数"""
    if os.path.isfile(os.path.dirname(os.path.realpath(sys.executable)) + "/" + path):
        return joblib.load(os.path.dirname(os.path.realpath(sys.executable)) + "/" + path)
    else:
        return joblib.load(sys.path[0] + "/" + path)






def main():
    pass
if __name__ == "__main__":
    main()