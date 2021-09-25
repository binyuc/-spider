import execjs
import requests

url='http://fund.eastmoney.com/pingzhongdata/000001.js'
res = requests.get(url)

js_content = execjs.compile(res.text)
date_map = {
            "source_rate": "fund_sourceRate",
            "rate": "fund_Rate",
            "minimum_purchase_amount": "fund_minsg",
            "stock_codes": "stockCodes",
            "zq_codes": "zqCodes",
            "new_stock_codes": "stockCodesNew",
            "new_zq_codes": "zqCodesNew",
            "annual_income": "syl_1n", # 1年
            "half_year_income": "syl_6y", #
            "quarterly_revenue": "syl_3y",
            "monthly_income": "syl_1y",
            "position_calculation_chart": "Data_fundSharesPositions",
            "net_worth_trend": "Data_netWorthTrend",
            # "cumulative_net_worth_trend": "Data_ACWorthTrend",
            "cumulative_rate_of_return_trend": "Data_grandTotal", # 累计收益率走势
            "rate_in_similar_type": "Data_rateInSimilarType", # 同类走势排名
            "rate_in_similar_persent": "Data_rateInSimilarPersent",
            "fluctuation_scale": "Data_fluctuationScale", # 规模变化
            "holder_structure": "Data_holderStructure", # 持有人变化
            "asset_allocation": "Data_assetAllocation", # 资产配置
            "performance_evaluation": "Data_performanceEvaluation",
            "current_fund_manager": "Data_currentFundManager", # 现任基金经理
            "buy_sedemption": "Data_buySedemption", # 申购赎回
            "swith_same_type": "swithSameType", # 同类型基金涨幅榜
            "million_copies_income": "Data_millionCopiesIncome",
            "seven_days_year_income": "Data_sevenDaysYearIncome",
            "asset_allocation_currency": "Data_assetAllocationCurrency",
        }
for key, name in date_map.items():
    value = js_content.eval(name)
    print(value)
# print(res.text)

