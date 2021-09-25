import requests
import execjs
import sys
import json

sys.path.append(r'D:\Jupyter\tt_spider\tiantian-spider')
from utils.get_headers import get_header
import config


def get_fundcode():
    header = {}
    header['user-agent'] = get_header()
    res = requests.get('http://fund.eastmoney.com/js/fundcode_search.js', headers=header)
    a = res.text.replace('var r = ', '').replace(';', '')
    fund_list = json.loads(a)
    final = []
    for i in fund_list[:10]:
        temp = {}
        temp['fund_code'] = i[0]
        temp['fund_name_en'] = i[1]
        temp['fund_name_cn'] = i[2]
        temp['fund_type'] = i[3]
        final.append(temp)
    return final


if __name__ == '__main__':
    get_fundcode()
