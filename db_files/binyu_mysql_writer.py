import pymysql
import pandas as pd
import datetime
from loguru import logger
from sqlalchemy import types, create_engine
from sqlalchemy.types import *
from config import *

class MysqlWriter(object):
    '''
    desc: mysql封装写入工具
    '''

    def __init__(self, database_name, target=None):
        if not target:
            self.host = db_host
            self.port = db_port
            self.user = db_user
            self.passwd = db_passwd
            charset = 'utf8'
            self.database_name = database_name
            self.conn = create_engine(
                f"mysql+pymysql://{self.user}:{self.passwd}@{self.host}:{self.port}/{self.database_name}",
                encoding='utf-8', convert_unicode=True)
            self.conn.autocommit = True
        elif target == 'localhost':
            self.host = local_host
            self.port = local_port
            self.user = local_user
            self.passwd = local_passwd
            charset = 'utf8'
            self.database_name = database_name
            self.conn = create_engine(
                f"mysql+pymysql://{self.user}:{self.passwd}@{self.host}:{self.port}/{self.database_name}",
                encoding='utf-8', convert_unicode=True)
            self.conn.autocommit = True

    def write_df(self, table_name, new_df, method='append', rep_dict=None):
        '''
        desc: 执行读取sql脚本，返回dataframe
        :param: new_df pd.DataFrame
        :param: table_name 等待被插入的表名（大小写不用区分）
        :param: method_name 默认为append， 如果需要replace 记得替换参数
        :return: None
        '''
        logger.info(f'开始写入dateframe，方法为{method}')
        start_time = datetime.datetime.now()
        conn = pymysql.Connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            db=self.database_name,
            charset='utf8'
        )
        for key, value in dict(new_df.dtypes).items():
            # 如果不对object的columns进行一次astype（str）就会报错
            if value == 'object':
                new_df.loc[:, '{}'.format(key)] = new_df.loc[:, '{}'.format(key)].astype(str)

        def set_d_type_dict(df, rep_dict = rep_dict):
            type_dict = {}
            for i, j in zip(df.columns, df.dtypes):
                if "object" in str(j):
                    type_dict.update({i: VARCHAR(255)})
                if "float" in str(j):
                    type_dict.update({i: FLOAT})
                if "int" in str(j):
                    type_dict.update({i: INT})
            if rep_dict:
                type_dict.update(rep_dict)
            return type_dict

        logger.info('Processing... Writing {} rows into database'.format(len(new_df)))
        d_type = set_d_type_dict(new_df)
        success = True
        try:
            new_df.to_sql(name=table_name.lower(), con=self.conn, if_exists=method, index=False, dtype=d_type,
                          chunksize=2000)
        except Exception as e:
            logger.error(e)
            success = False
        finally:
            end_time = datetime.datetime.now()
            consum_time = end_time - start_time
            if success:
                return logger.info(f'write dataframe successfully !!! time spent {consum_time}')
            else:
                return logger.info(f'write dataframe failed... time spent {consum_time}')
