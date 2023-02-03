select platform, eventtype, countrycode from user_groupondw.m_raw_click WHERE eventdate = '2017-12-23' limit 1;

select * from user_groupondw.m_raw_click where eventdate = '2020-01-15' limit 5;


select * from user_groupondw.gbl_fact_transactions_na limit 1;

DESCRIBE user_groupondw.gbl_fact_transactions_na;

DESCRIBE user_groupondw.m_raw_click;

SELECT * from cerebro_metadata.table_metadata;