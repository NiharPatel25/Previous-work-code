select top 100 * from user_edwprod.agg_gbl_financials order by report_date;


SELECT TOP 10 * FROM user_edwprod.agg_gbl_traffic_financials;

SELECT TOP 10 * FROM user_edwprod.agg_gbl_financials;


/*QUESTION 1: FOR GPR*/

SELECT YEAR(report_date),SUM(nob_loc) from user_edwprod.agg_gbl_financials WHERE YEAR(report_date) = 2017 group by YEAR(report_date);

/*Question 2: FOR GPR*/

SELECT YEAR(report_date) as year_, platform, SUM(nob_loc) as nob
from user_edwprod.agg_gbl_financials 
group by YEAR(report_date), platform order by year_, nob;

/*Question 3: line of business dominant by country. Lets do it first by NOB and for 2017 only... order by country_code, year_ */


SELECT YEAR(report_date) as year_, country_code, grt_l2_cat_name as category, SUM(nob_loc) as total, RANK() OVER(PARTITION BY year_, country_code ORDER BY total desc) 
from user_edwprod.agg_gbl_financials 
group by YEAR(report_date), country_code, grt_l2_cat_name;
