import requests
import execjs
import sys
import json
import pandas as pd

sys.path.append(r'D:\Jupyter\tt_spider\tiantian-spider')
from utils.get_headers import get_header
from config import *
from db_files.binyu_mysql_reader import MysqlReader
from db_files.binyu_mysql_writer import MysqlWriter


def get_fundcode():
    header = {}
    header['user-agent'] = get_header()
    res = requests.get('http://fund.eastmoney.com/js/fundcode_search.js', headers=header)
    a = res.text.replace('var r = ', '').replace(';', '')
    fund_list = json.loads(a)
    final = []
    for i in fund_list:
        temp = {}
        temp['fund_code'] = i[0]
        temp['fund_name_en'] = i[1]
        temp['fund_name_cn'] = i[2]
        temp['fund_type'] = i[3]
        final.append(temp)
    final = pd.DataFrame(final)
    print(final.info())
    MysqlWriter(target=None, database_name='bond_db').write_df(table_name='')

if __name__ == '__main__':
    get_fundcode()
