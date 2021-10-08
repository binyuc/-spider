import pymysql
import pandas as pd
import datetime
from loguru import logger
from config import *


class MysqlReader(object):
    '''
    desc: mysql封装读取工具
    '''

    def __init__(self, target=None):
        if not target:
            self.host = db_host
            self.port = db_port
            self.user = db_user
            self.passwd = db_passwd
        elif target == 'localhost':
            self.host = local_host
            self.port = local_port
            self.user = local_user
            self.passwd = local_passwd

    def read_sql(self, sql: str, database):
        '''
        desc: 执行读取sql脚本，返回dataframe
        :param sql: sql脚本
        :param database:
        :return: dataframe
        '''
        begin = datetime.datetime.now()
        conn = pymysql.Connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            db=database,
            charset='utf8'
        )
        results = None
        try:
            cursor = conn.cursor()
            # 执行SQL语句
            cursor.execute(sql)
            # 获取所有记录列表
            results = cursor.fetchall()
            logger.info(f'Successfully read table. ')
        except Exception as e:
            logger.debug(e)
        finally:
            cursor.close()
            conn.close()
            logger.info(f'Connection closed')
        if results:
            df = pd.DataFrame(results, columns=[i[0] for i in cursor.description], )
            logger.info(f'Read sql ended,time spent {datetime.datetime.now() - begin}, num of rows is {len(results)}')
            return df
        return logger.info(f'Read sql ended, time spent {datetime.datetime.now() - begin}')
