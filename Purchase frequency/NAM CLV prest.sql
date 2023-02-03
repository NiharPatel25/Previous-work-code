select * from grp_gdoop_bizops_db.rt_bt_txns;

select 
   booked, 
   count(distinct user_uuid) distinct_users,
   sum(total_units) all_units, 
   sum(total_nob) all_nob,
   sum(total_nor) all_nor,
   var_pop(total_units) units_var, 
   var_pop(total_nob) nob_var,
   var_pop(total_nor) nor_var
   from
(select 
    user_uuid,
    max(bookable) bookable,
    max(booked) booked,
    max(redeemed) redeemed,
    count(distinct parent_order_uuid) total_orders, 
    sum(units) total_units, 
    sum(case when bookable = 1 then units end) bookable_units,
    sum(nob) total_nob, 
    sum(nor) total_nor,
    count(distinct user_uuid) total_users
    from 
    grp_gdoop_bizops_db.nvp_purch_clv_nam
    group by 
      user_uuid)
      as fin 
    group by booked
    order by booked
    ;