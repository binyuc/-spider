import threading

import execjs
import requests
from loguru import logger
import sys
import pandas as pd
import json
from db_files.binyu_mysql_reader import MysqlReader
from db_files.binyu_mysql_writer import MysqlWriter
import random


class DetailSpider(object):
    def __init__(self):
        self.user_agent = [
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729; InfoPath.3; rv:11.0) like Gecko",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
            "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
            "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
            "MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
            "Opera/9.80 (Android 2.3.4; Linux; Opera Mobi/build-1107180945; U; en-GB) Presto/2.8.149 Version/11.10",
            "Mozilla/5.0 (BlackBerry; U; BlackBerry 9800; en) AppleWebKit/534.1+ (KHTML, like Gecko) Version/6.0.0.337 Mobile Safari/534.1+",
            "Mozilla/5.0 (hp-tablet; Linux; hpwOS/3.0.0; U; en-US) AppleWebKit/534.6 (KHTML, like Gecko) wOSBrowser/233.70 Safari/534.6 TouchPad/1.0",
            "Mozilla/5.0 (SymbianOS/9.4; Series60/5.0 NokiaN97-1/20.0.019; Profile/MIDP-2.1 Configuration/CLDC-1.1) AppleWebKit/525 (KHTML, like Gecko) BrowserNG/7.1.18124",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0; HTC; Titan)",
            "UCWEB7.0.2.37/28/999",
            "NOKIA5700/ UCWEB7.0.2.37/28/999",
            "Openwave/ UCWEB7.0.2.37/28/999",
            "Mozilla/4.0 (compatible; MSIE 6.0; ) Opera/UCWEB7.0.2.37/28/999",
        ]
        logger.remove()
        logger.add(sys.stderr, backtrace=False, diagnose=False)

        self.final = []
        self.bond_info_df = []
        self.bond_value_df = []

    def get_url_list(self):
        ''':param
        通过读取bond_db的数据，生成url_list
        '''
        sql = '''
        select * from bond_db.bond_list
        '''
        url_list = []
        df = MysqlReader(target=None).read_sql(sql=sql, database='bond_db')
        for i in df['fund_code'].to_list():
            url = f'http://fund.eastmoney.com/pingzhongdata/{i}.js'
            url_list.append(url)
        logger.info(f'url_list length is {len(url_list)}')
        return url_list

    def solo_spider(self, url: str):
        '''
        单线程爬虫程序
        :param url:
        :return: None
        '''
        # todo adding retries
        headers = {}
        headers['User-Agent'] = random.choice(self.user_agent)
        try_times = 0
        while try_times <= 3:
            try:
                res = requests.get(url, headers=headers, timeout=10)
                break
            except Exception as e:
                try_times += 1
                logger.warning(f'子线程出错，retring 第{try_times}次')
                if try_times >= 3:
                    logger.error(e)
        try:
            bond_info = self.parser(res)
            self.bond_info_df.append(bond_info)
            logger.debug(f'successfully get bond detail current  result length is {len(self.bond_info_df)}')
        except Exception as e:
            logger.warning(f'cannot parse bond details, url is {url}')
            pass

        try:
            bond_value = self.parser2(res)
            self.bond_value_df.append(bond_value)
            logger.debug(f'successfully get bond value current result length is {len(self.bond_value_df)}')
        except Exception as e:
            logger.warning(f'cannot parse bond value , url is {url}')
            pass
        # print(pd.DataFrame(self.bond_value_df, index=[0]))
        return

    def parser(self, res):
        '''
        解析函数1，返回基金信息
        :return： 某一个基金的info dict
        '''
        js_content = execjs.compile(res.text)
        parse_list = {
            'fund_name_cn': 'fS_name',
            'fund_code': 'fS_code',
            'fund_original_rate': 'fund_sourceRate',  # 原费率
            'fund_current_rate': 'fund_Rate',  # 现费率
            'mini_buy_amount': 'fund_minsg',  # 最小申购金额
            'hold_stocks_list': 'stockCodes',
            'hold_bond_list': 'zqCodes',  # 基金持仓债券代码
            'new_hold_stocks_list': 'stockCodesNew',  # 基金持仓股票代码(新市场号)
            'new_hold_bond_list': 'zqCodesNew',  # 基金持仓债券代码（新市场号）
            'syl_1_year': 'syl_1n',  # 近一年收益率
            'syl_6_month': 'syl_6y',  # 近6月收益率
            'syl_3_month': 'syl_3y',  # 近三月收益率
            'syl_1_month': 'syl_1y',  # 近一月收益率
            'Data_fluctuationScale': 'Data_fluctuationScale',  # 规模变动 mom-较上期环比
            'Data_holderStructure': 'Data_holderStructure',  # 持有人结构
            'Data_assetAllocation': 'Data_assetAllocation',  # 资产配置
            'Data_performanceEvaluation': 'Data_performanceEvaluation',  # *业绩评价 ['选股能力', '收益率', '抗风险', '稳定性','择时能力']
            'Data_currentFundManager': 'Data_currentFundManager',  # 现任基金经理
            'Data_buySedemption': 'Data_buySedemption',  # 申购赎回
            'swithSameType': 'swithSameType',  # 同类型基金涨幅榜
        }
        bond_info = {}
        for key, name in parse_list.items():
            temp = {}
            try:
                value = js_content.eval(name)
                temp[key] = value
            except:
                temp[key] = ''
            bond_info.update(temp)

        bond_info['hold_stocks_list'] = json.dumps(bond_info['hold_stocks_list'], ensure_ascii=False)
        bond_info['hold_bond_list'] = json.dumps(bond_info['hold_bond_list'], ensure_ascii=False)
        bond_info['new_hold_stocks_list'] = json.dumps(bond_info['new_hold_stocks_list'], ensure_ascii=False)
        bond_info['new_hold_bond_list'] = json.dumps(bond_info['new_hold_bond_list'], ensure_ascii=False)
        bond_info['Data_fluctuationScale'] = json.dumps(bond_info['Data_fluctuationScale'], ensure_ascii=False)
        bond_info['Data_holderStructure'] = json.dumps(bond_info['Data_holderStructure'], ensure_ascii=False)
        bond_info['Data_assetAllocation'] = json.dumps(bond_info['Data_assetAllocation'], ensure_ascii=False)
        bond_info['Data_performanceEvaluation'] = json.dumps(bond_info['Data_performanceEvaluation'],
                                                             ensure_ascii=False)
        bond_info['Data_currentFundManager'] = json.dumps(bond_info['Data_currentFundManager'], ensure_ascii=False)
        bond_info['Data_buySedemption'] = json.dumps(bond_info['Data_buySedemption'], ensure_ascii=False)
        bond_info['swithSameType'] = json.dumps(bond_info['swithSameType'], ensure_ascii=False)
        return bond_info

    def parser2(self, res):
        '''第二个解析程序，用来返回基金金额的'''
        parse_list2 = {
            'fund_name_cn': 'fS_name',
            'fund_code': 'fS_code',
            'Data_netWorthTrend': 'Data_netWorthTrend',  # 单位净值走势
            'Data_ACWorthTrend': 'Data_ACWorthTrend',  # 累计净值走势
            'Data_grandTotal': 'Data_grandTotal',  # 累计收益率走势
            'Data_rateInSimilarType': 'Data_rateInSimilarType',  # 同类排名走势
            'Data_rateInSimilarPersent': 'Data_rateInSimilarPersent',  # 同类排名百分比
        }
        bond_value = {}
        js_content = execjs.compile(res.text)
        for key, name in parse_list2.items():
            temp = {}
            try:
                value = js_content.eval(name)
                temp[key] = value
            except:
                temp[key] = ''
            bond_value.update(temp)

        bond_value['Data_netWorthTrend'] = json.dumps(bond_value['Data_netWorthTrend'], ensure_ascii=False)
        bond_value['Data_ACWorthTrend'] = json.dumps(bond_value['Data_ACWorthTrend'], ensure_ascii=False)
        bond_value['Data_grandTotal'] = json.dumps(bond_value['Data_grandTotal'], ensure_ascii=False)
        bond_value['Data_rateInSimilarType'] = json.dumps(bond_value['Data_rateInSimilarType'], ensure_ascii=False)
        bond_value['Data_rateInSimilarPersent'] = json.dumps(bond_value['Data_rateInSimilarPersent'],
                                                             ensure_ascii=False)

        return bond_value

    def multi_thread_func(self, url_list):
        '''desc: 多线程爬虫包裹'''
        global pool_sema
        logger.debug('-----------------------begin-----------------------')
        threads = []
        max_connections = 8
        pool_sema = threading.BoundedSemaphore(max_connections)
        for url in url_list:
            threads.append(
                threading.Thread(target=self.solo_spider, args=(url,))
            )
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        logger.debug('-----------------------end-----------------------')

    def saver(self):
        pass

    def split_list_by_n(self, target_list, n=100):
        '''将目标切成按100的长度切分'''
        url_list_collection = []
        for i in range(0, len(target_list), n):
            url_list_collection.append(target_list[i: i + n])
        return url_list_collection

    def run_spider(self):
        '''执行程序'''
        url_list = self.get_url_list()
        url_list_collection = self.split_list_by_n(url_list)
        # todo 记得修改url_list_collection的起始位置
        for mark, url_list in enumerate(url_list_collection[9:]):
            self.multi_thread_func(url_list)
            bond_info_df = pd.DataFrame(self.bond_info_df)
            bond_value_df = pd.DataFrame(self.bond_value_df)
            MysqlWriter(target=None, database_name='bond_db').write_df(table_name='bond_info', new_df=bond_info_df,
                                                                       method='append')
            MysqlWriter(target=None, database_name='bond_db').write_df(table_name='bond_value',
                                                                       new_df=bond_value_df,
                                                                       method='append')
            self.bond_info_df = []
            self.bond_value_df = []
            logger.info(f'current collection is NO.{mark}')


if __name__ == '__main__':
    url = 'http://fund.eastmoney.com/pingzhongdata/000001.js'
    # DetailSpider().solo_spider(url)
    DetailSpider().run_spider()
