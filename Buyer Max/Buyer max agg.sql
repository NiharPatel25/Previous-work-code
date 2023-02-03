---OUTPUT
select
	ltwo, 
	buyer_max_hit, 
	sum(count_udv) count_udv, 
	sum(count_bcook_udv)
from 
(select 
a.user_uuid, 
a.deal_uuid, 
a.buyer_max_hit, 
b.ltwo,
b.lthree,
b.count_udv, 
b.count_bcook_udv
from 
grp_gdoop_bizops_db.nvp_buyermax_allinfo as a
left join 
grp_gdoop_bizops_db.nvp_buyermax_udv_deal2 as b on a.user_uuid = b.user_uuid and a.buyer_max_hit = b.buyer_max_hit and a.deal_uuid = b.deal_uuid) fin_ 
where buyer_max_hit = 1 group by buyer_max_hit, ltwo order by ltwo
;

select lthree, buyer_max_hit, sum(count_udv) count_udv, sum(count_bcook_udv)
from grp_gdoop_bizops_db.nvp_buyermax_udv_deal 
where ltwo = 'L2 - Health / Beauty / Wellness' group by lthree, buyer_max_hit having buyer_max_hit = 1 order by count_udv desc;

select * from grp_gdoop_bizops_db.nvp_buyermax_udv_deal2 limit 5;

select * from grp_gdoop_bizops_db.nvp_buyermax_udv_deal2;
select buyer_max_hit, count(user_uuid), count(distinct user_uuid) from grp_gdoop_bizops_db.nvp_buyermax_allinfo group by buyer_max_hit order by ltwo;

select buyer_max_hit, sum(count_udv), sum(count_bcook_udv) from grp_gdoop_bizops_db.nvp_buyermax_udv group by buyer_max_hit;
----

select count(*), min(buyer_max), max(buyer_max) from grp_gdoop_bizops_db.nvp_buyermax_info where buyer_max is not null;



-----
select * from grp_gdoop_bizops_db.nvp_user_max_topm_deals;