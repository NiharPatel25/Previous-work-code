
------PUSH CANDIDATES

drop table sandbox.nvp_no_show_push;
create multiset table sandbox.nvp_no_show_push
(
merchant_uuid varchar(100) character set unicode, 
country_cd varchar(100) character set unicode,
preferred_lang varchar(100) character set unicode, 
push varchar(100) character set unicode
);

drop table sandbox.nvp_no_show_tableau;
create multiset table sandbox.nvp_no_show_tableau as
(select 
	a.merchant_uuid,
	a.country_id,
	a.appointment_date, 
	count(vsc_code) total_appointments, 
	sum(redemption_status) total_redemptions
from sandbox.ab_chkins_final a 
	join sandbox.nvp_no_show_push b on a.merchant_uuid = b.merchant_uuid
	where a.book_state = 'confirmed'
	and a.appointment_date >= '2020-06-01'
	and a.fgt_flag = 1
	and a.refund_flag = 0
	group by a.merchant_uuid, a.country_id, a.appointment_date) with data

select count(*) from sandbox.nvp_no_show_push;	

/*case when checked_in = 'no-show' and redemption_flag = 0 end as no_show,
count(vsc_code)
where
book_state = 'confirmed'
and appointment_date >= '2020-07-06'
and fgt_flag = 1
and refund_flag = 0

*cast(SUBSTR(ack_at, 0,8) as TIMESTAMP(0) FORMAT 'MM/DD/YY') ack_at, */

 
-------ACKNOWLEDGED PUSH



drop table sandbox.nvp_acknowledged_push;
create multiset table sandbox.nvp_acknowledged_push
(
merchant_uuid varchar(100) character set unicode, 
country_cd varchar(100) character set unicode,
ack_at varchar(100) character set unicode, 
ack_method varchar(100) character set unicode
);



drop table sandbox.nvp_acknowledged_push_lev2;
create multiset table sandbox.nvp_acknowledged_push_lev2 as 
(select 
	merchant_uuid, 
	country_cd, 
	cast(ack_at as date) ack_at,
	trunc(cast(ack_at as date),'iw')+6 ack_week,
	ack_method
from
(select * from sandbox.nvp_acknowledged_push where ack_at is not null) as fin) with data;

select * from sandbox.nvp_acknowledged_push_lev2;

------EMAIL AND INTERCEPT

drop table sandbox.nvp_merchant_email_inter;
create multiset table sandbox.nvp_merchant_email_inter
(
merchant_uuid varchar(100) character set unicode, 
country_cd varchar(100) character set unicode,
gross_appt float,
no_show float,
read_email varchar(100) character set unicode, 
ack_intercept varchar(100) character set unicode, 
is_live_deal varchar(100) character set unicode
);

drop table sandbox.nvp_merchant_email_inter_lev2;
create multiset table sandbox.nvp_merchant_email_inter_lev2 as
(
select merchant_uuid, 
	country_cd, 
	gross_appt, 
	no_show,
	case read_email when 'YES' then 'yes'
		when '#N/A' then NULL END read_email, 
	case ack_intercept when 'YES' then 'yes'
		when '#N/A' then NULL END ack_intercept,
	is_live_deal
from sandbox.nvp_merchant_email_inter) with data;

select * from sandbox.nvp_merchant_email_inter_lev2;

------SALES FORCE

drop table sandbox.nvp_sf_import;
create multiset table sandbox.nvp_sf_import
(
merchant_uuid varchar(100) character set unicode, 
country_cd varchar(100) character set unicode,
open_at varchar(100) character set unicode 
);

create multiset table sandbox.nvp_sf_import_lev2 as
(select merchant_uuid, country_cd, cast(open_at as date) open_at from sandbox.nvp_sf_import) with data;

select * from sandbox.nvp_sf_import_lev2;
-----Getting all merchant data
SELECT * FROM sandbox.nvp_sf_import WHERE country_cd = 'GB'

drop table sandbox.nvp_all_merchant_no_show;
create multiset table sandbox.nvp_all_merchant_no_show as
(
select * from 
	(select merchant_uuid, case when country_cd = 'GB' then 'UK' else country_cd end country_cd from sandbox.nvp_merchant_email_inter_lev2
	union
	select merchant_uuid, case when country_cd = 'GB' then 'UK' else country_cd end country_cd from sandbox.nvp_sf_import_lev2
	union
	select merchant_uuid, case when country_cd = 'GB' then 'UK' else country_cd end country_cd  from sandbox.nvp_no_show_push
	union
	select merchant_uuid, case when country_cd = 'GB' then 'UK' else country_cd end country_cd  from sandbox.nvp_acknowledged_push_lev2) fin group by merchant_uuid, country_cd
) with data;


------
select * from sandbox.nvp_all_merchant_min_bt;
create multiset table sandbox.nvp_all_merchant_min_bt as
(select 
mp.merchant_uuid,
min(load_date) min_load_date
from 
(select deal_uuid, min(load_date) load_date from sandbox.sh_bt_active_deals_log where is_bookable = 1 and partner_inactive_flag = 0 and product_is_active_flag = 1 group by deal_uuid) as bo
left join 
(select 
	merchant_uuid, 
	product_uuid product_uuid
	from user_edwprod.dim_offer_ext  where inv_product_uuid <> '-1' and contract_sell_price > 0 and groupon_value <> 0
) as mp on bo.deal_uuid = mp.product_uuid
group by mp.merchant_uuid
) with data;


select * from sandbox.nvp_all_merchant_flag_no_show;
drop table sandbox.nvp_all_merchant_flag_no_show;
create multiset table sandbox.nvp_all_merchant_flag_no_show as
(select 
	a.merchant_uuid,
	a.country_cd,
	case when b.merchant_uuid is not null then 1 else 0 end push_test_group, 
	case when c.merchant_uuid is not null then ack_at else null end ack_at_week, 
	case when d.merchant_uuid is not null then 1 else 0 end read_email,
	case when e.merchant_uuid is not null then 1 else 0 end ack_intercept,
	case when f.merchant_uuid is not null then open_at else null end sf_call, 
	g.min_load_date
from sandbox.nvp_all_merchant_no_show as a
left join sandbox.nvp_no_show_push as b on a.merchant_uuid = b.merchant_uuid and a.country_cd = b.country_cd
left join (select merchant_uuid, case when country_cd = 'GB' then 'UK' else country_cd end country_cd , min(ack_at) ack_at from sandbox.nvp_acknowledged_push_lev2 group by merchant_uuid, country_cd) as c on a.merchant_uuid = c.merchant_uuid and a.country_cd = c.country_cd
left join (select merchant_uuid, case when country_cd = 'GB' then 'UK' else country_cd end country_cd from sandbox.nvp_merchant_email_inter_lev2 where read_email = 'yes' and is_live_deal = 'Y' group by merchant_uuid, country_cd) as d on a.merchant_uuid = d.merchant_uuid and a.country_cd = d.country_cd
left join (select merchant_uuid, case when country_cd = 'GB' then 'UK' else country_cd end country_cd from sandbox.nvp_merchant_email_inter_lev2 where ack_intercept = 'yes'and is_live_deal = 'Y'group by merchant_uuid, country_cd) as e on a.merchant_uuid = e.merchant_uuid and a.country_cd = e.country_cd
left join (select merchant_uuid, case when country_cd = 'GB' then 'UK' else country_cd end country_cd, min(open_at) open_at from sandbox.nvp_sf_import_lev2 group by merchant_uuid, country_cd) as f on a.merchant_uuid = f.merchant_uuid and a.country_cd = f.country_cd
left join (select merchant_uuid, min_load_date from sandbox.nvp_all_merchant_min_bt) as g on a.merchant_uuid = g.merchant_uuid
) with data;


select * from sandbox.nvp_all_merchant_flag_no_show;

------TABLE 
select * from sandbox.nvp_no_show_tableau2 where country_cd = 'US';

drop table sandbox.nvp_no_show_tableau2;

select * from sandbox.nvp_no_show_tableau2;

create multiset table sandbox.nvp_no_show_tableau2 as
(select 
	a.merchant_uuid,
	a.country_cd,
	a.push_test_group, 
	a.ack_at_week,
	a.read_email,
	a.ack_intercept,
	a.sf_call,
	a.min_load_date,
	b.appt_week,
	count(b.vsc_code) total_appointments, 
	sum(b.redemption_status) total_redemptions
from sandbox.nvp_all_merchant_flag_no_show as a
left join
(select 
	merchant_uuid, 
	country_id, 
	appointment_date,
	vsc_code,
	redemption_status, 
	trunc(cast(appointment_date as date),'iw')+6 appt_week
from sandbox.ab_chkins_final 
where book_state = 'confirmed'
	and appointment_date >= '2020-05-01'
	and fgt_flag = 1
	and refund_flag = 0
) as b on a.merchant_uuid = b.merchant_uuid and a.country_cd = b.country_id
group by 
a.merchant_uuid,
a.country_cd,
a.push_test_group, 
a.ack_at_week,
a.read_email,
a.ack_intercept,
a.sf_call,
b.appt_week, a.min_load_date) with data;



------Old table

drop table sandbox.nvp_no_show_tableau;

create multiset table sandbox.nvp_no_show_tableau as
(select 
	merchant_uuid, 
	country_id, 
	appointment_date, 
	appt_week,
	ack_intercept,
	read_mail,
	acknowledged_week,
	count(vsc_code) total_appointments, 
	sum(redemption_status) total_redemptions
from
(select 
	a.merchant_uuid,
	a.country_id,
	a.appointment_date,
	a.vsc_code,
	a.redemption_status,
	a.appt_week,
	c.read_email read_mail, 
	c.ack_intercept,
	case when d.merchant_uuid is not null then 1 else 0 end acknowledged_week, 
	d.ack_week
from 
	(select merchant_uuid, 
			country_id, 
			appointment_date,
			vsc_code,
			redemption_status, 
			trunc(cast(appointment_date as date),'iw')+6 appt_week
		from sandbox.ab_chkins_final 
		where book_state = 'confirmed'
		and appointment_date >= '2020-06-01'
		and fgt_flag = 1
		and refund_flag = 0) as a
	join sandbox.nvp_no_show as b on a.merchant_uuid = b.merchant_uuid
	left join sandbox.nvp_merchant_email_inter_fin as c on a.merchant_uuid = c.merchant_uuid
	left join (select distinct ack_week, merchant_uuid from sandbox.nvp_acknowledged_push_fin) d on a.merchant_uuid = d.merchant_uuid and a.appt_week = d.ack_week
	) fin  
	group by merchant_uuid, country_id, appointment_date, appt_week, ack_intercept, acknowledged_week, read_mail) with data






------REDEMPTION TABLEAU 
drop table sandbox.nvp_merc_red_tableau_agg;

create multiset table sandbox.nvp_merc_red_tableau_agg as
(select 
	grt_l2_cat_name, 
	country_id,
	redemption_date - appointment_date date_diff,
	trunc(appointment_date,'iw')+6 as appt_week,
	trunc(appointment_date,'mm') as appt_mnth,
	trunc(appointment_date,'Q') as appt_qrtr,
	YEAR(appointment_date) as appt_year,
	sum(redemption_status) total_redemptions, 
	count(vsc_code) total_appointments
from sandbox.nvp_chkins_final a 
	where a.book_state = 'confirmed'
	and a.fgt_flag = 1
	and a.refund_flag = 0
group by 
	grt_l2_cat_name, 
	country_id,
	redemption_date - appointment_date,
	trunc(appointment_date,'iw')+6,
	trunc(appointment_date,'mm'),
	trunc(appointment_date,'Q'),
	YEAR(appointment_date)
	) with data;



drop table sandbox.nvp_merc_red_tableau;
create multiset table sandbox.nvp_merc_red_tableau as
(select 
	grt_l2_cat_name, 
	country_id,
	vsc_code,
	appointment_date,
	redemption_date,
	redemption_status
from sandbox.nvp_chkins_final a 
	where a.book_state = 'confirmed'
	and a.fgt_flag = 1
	and a.refund_flag = 0) with data;
