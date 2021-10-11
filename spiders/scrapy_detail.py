import threading

import execjs
import requests
from loguru import logger
import sys
import pandas as pd
import json
from db_files.binyu_mysql_reader import MysqlReader
from db_files.binyu_mysql_writer import MysqlWriter


# pd.options.display.max_columns = 100
# pd.options.display.max_rows = 3


class DetailSpider(object):
    def __init__(self):
        logger.remove()
        logger.add(sys.stderr, backtrace=False, diagnose=False)

        self.final = []

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
        :return:
        '''
        # todo adding retries
        res = requests.get(url)
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
        parse_list2 = {
            'fund_name_cn': 'fS_name',
            'fund_code': 'fS_code',
            'Data_netWorthTrend': 'Data_netWorthTrend',  # 单位净值走势
            'Data_ACWorthTrend': 'Data_ACWorthTrend',  # 累计净值走势
            'Data_grandTotal': 'Data_grandTotal',  # 累计收益率走势
            'Data_rateInSimilarType': 'Data_rateInSimilarType',  # 同类排名走势
            'Data_rateInSimilarPersent': 'Data_rateInSimilarPersent',  # 同类排名百分比
        }
        bond_info = {}
        bond_value = {}
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

        for key, name in parse_list2.items():
            temp = {}
            value = js_content.eval(name)
            temp[key] = value
            bond_value.update(temp)

        return bond_info

    def saver(self, df):
        pass

    def cleaner(self):
        pass

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

    def run_spider(self):
        self.bond_info_df = []
        url_list = self.get_url_list()
        for mark, url in enumerate(url_list[:10]):
            try:
                self.bond_info_df.append(self.solo_spider(url=url))
                logger.debug(
                    f'successfully get bond detail current bond is {mark}, result length is {len(self.bond_info_df)}')
            except Exception as e:
                logger.warning(f'cannot parse bond details, url is {url}, mark is {mark}')
        bond_info_df = pd.DataFrame(self.bond_info_df)
        MysqlWriter(target=None, database_name='bond_db').write_df(table_name='bond_info', new_df=bond_info_df, method='append')


if __name__ == '__main__':
    url = 'http://fund.eastmoney.com/pingzhongdata/000001.js'
    DetailSpider().run_spider()
