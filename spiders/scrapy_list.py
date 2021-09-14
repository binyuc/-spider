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
    js_content = execjs.compile(res)
    print(type(js_content))
    # r = js_content.eval("r")
    # print(js_content)
    print(js_content.eval('r'))


if __name__ == '__main__':
    get_fundcode()
