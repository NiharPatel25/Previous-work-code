
select row_rank, account_id, account_name, live_720, live_today, svcs_720, svcs_now from sandbox.avb_to_top_accts_hbw order by row_rank;

select * from sandbox.avb_to_top_accts_hbw order by row_rank;

select * from sandbox.avb_to_top_accts_hbw where unres_now = 2 order by row_rank;

------
drop hbw_owners;
create volatile multiset table hbw_owners as (
sel
account_manager ownerid,
account_id_18 account_id,
ros.rep as account_owner,
ros.m1,
name
from dwh_base_sec_view.sf_account sfa
left join user_groupondw.dim_sf_person sfp on sfp.person_id = sfa.ownerid
left join (
	select
	 roster_date,
	 emplid,
	 team,
	 title_rw,
	 rep,
	 m1,
	 m2,
	 m3
	from sandbox.ops_roster_all
	where roster_date = current_date - 1
	qualify row_number() over(partition by roster_date,emplid order by start_date,month_end_date,length(title_rw) desc) = 1
	) ros on ros.emplid = sfp.ultipro_id
) with data primary index (account_id) on commit preserve rows;
collect stats on hbw_owners column(account_id);

select * from sandbox.ops_roster_all where roster_date = current_date - 1;

select count(*) from hbw_owners;

drop table hbw_pds;
create volatile multiset table hbw_pds as (
sel
account_id,
a.deal_id,
p.grt_l3_cat_description as l3,
p.pds_cat_name as pds
from (
	select
	f.deal_id,
	f.account_id,
	sum(units) units,
	row_number() over(partition by f.account_id order by sum(units) desc) as units_rank
	from sandbox.rev_mgmt_deal_funnel f
	join sandbox.avb_to_t600_hbw h on f.account_id = h.account_id
	where
		report_date between '2019-10-01' and '2020-02-28'
	group by 1,2
) a 
join user_edwprod.dim_gbl_deal_lob l on a.deal_id = l.deal_id
join user_dw.v_dim_pds_grt_map p on p.pds_cat_id = l.pds_cat_id
where units_rank = 1
group by 1,2,3,4)
with data primary index (account_id) on commit preserve rows;
collect stats abautista.hbw_pds column(account_id);



drop table hbw_pop_svcs;
create volatile multiset table hbw_pop_svcs as (
sel
pds_cat_id,
grt_l5_cat_name
from user_dw.v_dim_pds_grt_map
where grt_l5_cat_name in (
'L5 - Injection - Wrinkle',
'L5 - Laser Hair Removal',
'L5 - Fat/ Cellulite Reduction Procedure',
'L5 - Spa Package',
'L5 - Saunas & Steam Rooms',
'L5 - Specialty Facial',
'L5 - Laser Eye Surgery',
'L5 - Spa - Day Pass',
'L5 - Chiropractic / Osteopathy',
'L5 - Floating - Isolation Tank / Sensory Deprivation',
'L5 - Eyebrow & Eyelash Care',
'L5 - Medical Care',
'L5 - Teeth Whitening',
'L5 - Cryotherapy',
'L5 - Dental Checkup/ Cleaning',
'L5 - Salt Cave',
'L5 - Weight Loss & Nutrition',
'L5 - Other Alternative Therapies',
'L5 - Acupuncture',
'L5 - Injection - B12',
'L5 - Massage',
'L5 - Haircut/Color',
'L5 - Facial',
'L5 - Eyelash Extensions',
'L5 - Nail Services',
'L5 - Waxing',
'L5 - Facial- Photo',
'L5 - Facial- Rejuvenation',
'L5 - Tanning',
'L5 - Straightening Treatment',
'L5 - Reflexology',
'L5 - Blow Dry',
'L5 - Beauty Package - Hair Salon',
'L5 - Threading',
'L5 - Sugaring',
'L5 - Eyelash Tinting',
'L5 - Hair Styling',
'L5 - Haircut - Men',
'L5 - Hair Conditioning Treatment',
'L5 - Cupping',
'L5 - Yoga',
'L5 - Boxing / Kickboxing',
'L5 - Boot Camp',
'L5 - Pilates',
'L5 - Fitness Studio',
'L5 - Gym',
'L5 - Spinning / Indoor Cycling',
'L5 - Barre',
'L5 - Cross Fit',
'L5 - Sexy Fitness',
'L5 - Gymnastics',
'L5 - Personal Trainer',
'L5 - Fitness Conditioning',
'L5 - Dance Class',
'L5 - Aerial Fitness',
'L5 - Zumba',
'L5 - Personalized Fitness Program',
'L5 - Aerobics',
'L5 - Circuit Training',
'L5 - Gym / Fitness Center - In Spa')
) with data primary index (pds_cat_id) on commit preserve rows;
collect stats on hbw_pop_svcs column (pds_cat_id);


-----


drop table hbw_720;
create volatile multiset table hbw_720 as (
	select
	o.account_id,
	count(distinct m.grt_l5_cat_name) as services_live,
	count(distinct case when p.pds_cat_id is not null then m.grt_l5_cat_name end) pop_services,
	count(distinct case when deals_live = 1 then pds_name end) as pds_live,
	count(distinct ad.inventory_id) as options_live
	from sandbox.sup_analytics_deal_counts_final o
	join user_dw.v_dim_pds_grt_map m on o.pds_cat_id = m.pds_cat_id
	join user_groupondw.fact_active_deals ad on o.deal_uuid = ad.deal_uuid and sold_out = 'false' and load_date = '2020-07-19'
	join sandbox.avb_to_t600_hbw a on o.account_id = a.account_id
	left join hbw_pop_svcs p on m.pds_cat_id = p.pds_cat_id
	where report_date = '2020-07-19' and accts_live = 1 and deals_live = 1
	group by 1
) with data primary index (account_id) on commit preserve rows;
collect stats on hbw_720 column(account_id);

select * from hbw_720 order by account_id;
---

create volatile multiset table hbw_now as (
	select
	o.account_id, 
	count(distinct m.grt_l5_cat_name) as services_live,
	count(distinct case when p.pds_cat_id is not null then m.grt_l5_cat_name end) pop_services,
	count(distinct case when deals_live = 1 then pds_name end) as pds_live,
	count(distinct ad.inventory_id) as options_live
	from sandbox.sup_analytics_deal_counts_final o
	join user_dw.v_dim_pds_grt_map m on o.pds_cat_id = m.pds_cat_id
	join user_groupondw.fact_active_deals ad on o.deal_uuid = ad.deal_uuid and sold_out = 'false' and load_date = current_date - 2
	join sandbox.avb_to_t600_hbw a on o.account_id = a.account_id
	left join hbw_pop_svcs p on m.pds_cat_id = p.pds_cat_id
	where report_date = current_date - 2 and accts_live = 1 and deals_live = 1
	group by 1
) with data primary index (account_id) on commit preserve rows;
collect stats on hbw_now column(account_id);

select * from hbw_now order by account_id;

----

drop table hbw_new_svcs;
create volatile multiset table hbw_new_svcs as (
	sel
		a.account_id,
		count(distinct a.services_live) as new_svcs_live,
		count(distinct case when c.grt_l5_cat_name is not null then c.grt_l5_cat_name end) new_pop_svcs_live
	from (
		select distinct
		o.account_id,
		m.grt_l5_cat_name as services_live
		from sandbox.sup_analytics_deal_counts_final o
		join user_dw.v_dim_pds_grt_map m on o.pds_cat_id = m.pds_cat_id
		join sandbox.avb_to_t600_hbw a on o.account_id = a.account_id
		where report_date = current_date - 2 and accts_live = 1 and deals_live = 1
		) a
	left join (	
		select distinct
		o.account_id,
		m.grt_l5_cat_name as services_live
		from sandbox.sup_analytics_deal_counts_final o
		join user_dw.v_dim_pds_grt_map m on o.pds_cat_id = m.pds_cat_id
		join sandbox.avb_to_t600_hbw a on o.account_id = a.account_id
		where report_date = '2020-07-19' and accts_live = 1 and deals_live = 1
		) b
	on a.account_id = b.account_id and a.services_live = b.services_live
	left join hbw_pop_svcs c on a.services_live = c.grt_l5_cat_name
	where
		b.account_id is null and b.services_live is null
	group by 1
) with data primary index (account_id) on commit preserve rows;
collect stats on hbw_new_svcs column(account_id);

select count(*) from hbw_booking_720;

drop table hbw_booking_720;
create volatile multiset table hbw_booking_720 as (
	select
		distinct salesforce_account_id as account_id
	from sandbox.sh_bt_active_deals_log a
	join user_edwprod.dim_offer o on a.deal_uuid = o.product_uuid
	join user_edwprod.dim_merchant m on m.merchant_uuid = o.merchant_uuid
	where
		(load_date = '2020-07-19' and is_bookable = 1 and partner_inactive_flag = 0 and product_is_active_flag = 1)
	UNION ALL
	sel
		distinct account_id
	from sandbox.hbw_deals_bookings_flags m
	where 
		mbo_flag = 1
		and m.deal_uuid <> '88610046-a287-48e7-8fd3-e5092b5926f1' -- permalink booking-test-co-5'
		and m.report_date = '2020-07-19'
) with data primary index (account_id) on commit preserve rows;
collect stats on hbw_booking_720 column(account_id);

drop table hbw_booking_now;
create volatile multiset table hbw_booking_now as (
	select
		distinct salesforce_account_id as account_id
	from sandbox.sh_bt_active_deals_log a
	join user_edwprod.dim_offer o on a.deal_uuid = o.product_uuid
	join user_edwprod.dim_merchant m on m.merchant_uuid = o.merchant_uuid
	where
		(load_date = current_date - 2 and is_bookable = 1 and partner_inactive_flag = 0 and product_is_active_flag = 1)
	UNION ALL
	sel
		distinct account_id
	from sandbox.hbw_deals_bookings_flags m
	where 
		mbo_flag = 1
		and m.deal_uuid <> '88610046-a287-48e7-8fd3-e5092b5926f1' -- permalink booking-test-co-5'
		and m.report_date = (sel max(report_date) from sandbox.hbw_deals_bookings_flags)
) with data primary index (account_id) on commit preserve rows;
collect stats on hbw_booking_now column(account_id);




create volatile multiset table hbw_tiers_deals as (
sel
	account_id,
	count(distinct case when option_tier = 1 then option_uuid end) as offers,
	count(distinct case when option_tier = 2 then option_uuid end) as deals
from sandbox.deals_options_tier_audited_daily_snapshot
where
	report_date = (sel max(report_date) from sandbox.deals_options_tier_audited_daily_snapshot) 
	and op_close_date >= '2020-07-19'
	and deal_tier in (1,3)
group by 1
) with data primary index (account_id) on commit preserve rows;
collect stats on hbw_tiers_deals column(account_id);


select count(*) from hbw_tiers_deals;