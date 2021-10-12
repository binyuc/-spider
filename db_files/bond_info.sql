create table if not exists bond_db.bond_info(
    ID INT  PRIMARY KEY AUTO_INCREMENT,
    fund_code varchar(255) comment '基金代码',
    fund_name_cn varchar(255) comment '基金中文名称',
    fund_original_rate varchar(255) comment '原费率',
    fund_current_rate varchar(255) comment '现费率',
    mini_buy_amount varchar(255)  comment '最小申购金额',
    hold_stocks_list varchar(1000) comment '持仓股票列表',
    hold_bond_list varchar(1000) comment '基金持仓债券代码',
    new_hold_stocks_list varchar(1000) comment '基金持仓股票代码(新市场号)',
    new_hold_bond_list varchar(1000) comment '基金持仓债券代码（新市场号）',
    syl_1_year varchar(255) comment '近一年收益率',
    syl_6_month varchar(255) comment '近6月收益率',
    syl_3_month varchar(255) comment '近三月收益率',
    syl_1_month varchar(255) comment '近一月收益率',
    Data_fluctuationScale json comment '规模变动',
    Data_holderStructure json comment '持有人结构',
    Data_assetAllocation json comment '资产配置',
    Data_performanceEvaluation json comment "业绩评价 ['选股能力', '收益率', '抗风险', '稳定性','择时能力']",
    Data_currentFundManager json comment '现任基金经理',
    Data_buySedemption json comment '申购赎回',
    swithSameType json comment '同类型基金涨幅榜'
)
;
select * from bond_db.bond_info





