import threading

import execjs
import requests
from loguru import logger
import sys
import pandas as pd
import json
from db_files.binyu_mysql_reader import MysqlReader
from db_files.binyu_mysql_writer import MysqlWriter


class DetailSpider(object):
    def __init__(self):
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
        res = requests.get(url)
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
            logger.warning(f'cannot parse bond details, url is {url}')
            pass

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
            value = js_content.eval(name)
            temp[key] = value
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
            value = js_content.eval(name)
            temp[key] = value
            bond_value.update(temp)

        bond_value['hold_stocks_list'] = json.dumps(bond_value['Data_netWorthTrend'], ensure_ascii=False)
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
        max_connections = 16
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

    def split_list_by_n(self, target_list, n=500):
        '''将目标切成按1000的长度切分'''
        url_list_collection = []
        for i in range(0, len(target_list), n):
            url_list_collection.append(target_list[i: i + n])
        return url_list_collection

    def run_spider(self):
        '''执行程序'''
        url_list = self.get_url_list()
        url_list_collection = self.split_list_by_n(url_list)
        for mark, url_list in enumerate(url_list_collection[:2]):
            self.multi_thread_func(url_list)
            MysqlWriter(target=None, database_name='bond_db').write_df(table_name='bond_info', new_df=self.bond_info_df,
                                                                       method='append')
            self.bond_info_df = []
            logger.info(f'current collection is NO.{mark}')
        bond_info_df = pd.DataFrame(self.bond_info_df)
        print(bond_info_df)
        #


if __name__ == '__main__':
    url = 'http://fund.eastmoney.com/pingzhongdata/000001.js'
    DetailSpider().run_spider()
