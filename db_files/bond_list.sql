create table if not exists bond_db.bond_list(
    fund_code varchar(255) comment '基金代码',
    fund_name_en varchar(255) comment '基金英文名称',
    fund_name_cn varchar(255) comment '基金中文名称',
    fund_type varchar(255) comment '基金类型'
)
;

select * from bond_db.bond_list
