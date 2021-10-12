create table if not exists bond_db.bond_value(
    ID INT  PRIMARY KEY AUTO_INCREMENT,
    fund_code varchar(255) comment '基金代码',
    fund_name_cn varchar(255) comment '基金中文名称',
    Data_netWorthTrend json comment '单位净值走势',
    Data_ACWorthTrend json comment '累计净值走势',
    Data_grandTotal    json comment '累计收益率走势',
    Data_rateInSimilarType   json comment '同类排名走势',
    Data_rateInSimilarPersent  json comment '同类排名百分比'
)
;
select * from bond_db.bond_value





