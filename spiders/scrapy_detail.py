import execjs
import requests
from loguru import logger
import sys
import pandas as pd


class DetailSpider(object):
    def __init__(self):
        logger.remove()
        logger.add(sys.stderr, backtrace=False, diagnose=False)

        self.final = []

    def solo_spider(self, url: str):
        '''
        单线程爬虫程序
        :param url:
        :return:
        '''
        # todo adding retries
        res = requests.get(url)
        js_content = execjs.compile(res.text)
        parse_list={
            'fund_name_cn' : 'fS_name',
            'fund_code' : 'fS_code',
            'fund_original_rate' : 'fund_sourceRate',
            'fund_current_rate' : 'fund_Rate',
            'mini_buy_amount' : 'fund_minsg',
            'hold_stocks_list' : 'stockCodes',
            'hold_bond_list' :'zqCodes', #基金持仓债券代码
            '':'', #基金持仓股票代码(新市场号),
            '':'',
        }
        final = {}
        for key, name in parse_list.items():
            temp = {}
            value = js_content.eval(name)
            temp[key] = value
            final.update(temp)
        # print(pd.DataFrame(final))
        print(final)

    def saver(self):
        pass

    def cleaner(self):
        pass

    def run_spider(self, url):
        self.solo_spider(url=url)


if __name__ == '__main__':
    url = 'http://fund.eastmoney.com/pingzhongdata/000001.js'
    DetailSpider().run_spider(url)


