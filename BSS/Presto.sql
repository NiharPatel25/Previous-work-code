select * from grp_gdoop_bizops_db.nvp_bss_funnel;

select * from grp_gdoop_bizops_db.sh_bt_questionnaires;

select merchant_uuid, groupon_real_deal_uuid, count(1) from grp_gdoop_bizops_db.nvp_bss_funnel group by 1,2;

select * from grp_gdoop_bizops_db.nvp_bss_funnel;

select * from grp_gdoop_bizops_db.nvp_bss_funnel
where merchant_uuid = '60dce150-97e2-44d3-b96c-c6d9f19d0509' and groupon_real_deal_uuid = 'bfafb4f4-b24f-4a5d-af90-3d3ec47b96fa';

select count(distinct concat(merchant_uuid, deal_uuid) ) from grp_gdoop_bizops_db.nvp_bss_funnel;


select 
    deals, 
    count(distinct merchant_uuid)
from 
(select merchant_uuid, count(distinct groupon_real_deal_uuid) deals from grp_gdoop_bizops_db.nvp_bss_funnel group by 1)
group by 1;




{"questionnaire":
   [{"stepId":"businessHours",
     "isPassed":true,
     "isCurrent":false,
     "isChanged":false,
     "questions":
        [{"locationsHours":
              [{"id":"ad092afc-6050-b4f3-b550-01749478711b",
                "name":"serenity hair salon",
                "address":"14545 South Military Trail, Delray Beach, US 33484",
                "location":
                    {"street_address":"14545 South Military Trail",
                    "locality":"Delray Beach",
                    "region":"FL",
                    "postcode":"33484",
                    "country":"US",
                    "lon_lat":{"lon":-80.12479720000002,"lat":26.4601337},
                    "time_zone":"America/New_York","neighborhood":"Fountains at Delray Beach"},
                "weekDays":
                    [{"dayOfWeek":7,
                      "isClosed":true,
                      "hours":[{}]},
                      {"dayOfWeek":1,
                       "isClosed":false,
                        "hours":[{"from":"10:00","until":"18:00"}]},
                                 {"dayOfWeek":2,
                                  "isClosed":false,
                                   "hours":[{"from":"10:00","until":"18:00"}]},
                                  {"dayOfWeek":3,"isClosed":false,
                                   "hours":[{"from":"10:00","until":"18:00"}]},
                                   {"dayOfWeek":4,"isClosed":false,"hours":[{"from":"10:00","until":"18:00"}]},
                                   {"dayOfWeek":5,"isClosed":false,"hours":[{"from":"10:00","until":"18:00"}]},
                                   {"dayOfWeek":6,"isClosed":false,"hours":[{"from":"09:00","until":"17:00"}]}]
                   },
                   
                {"id":"4779ceff-75f7-b099-c72b-017494787193",
                 "name":"serenity hair salon",
                 "address":"14545c South Military Trail, Delray Beach, US 33484",
                 "location":{"street_address":"14545c South Military Trail","locality":"Delray Beach","region":"FL","postcode":"33484","country":"US","lon_lat":{"lon":-80.12479720000002,"lat":26.4601337},"time_zone":"America/New_York","neighborhood":"Fountains at Delray Beach"},
                 "weekDays":[{"dayOfWeek":7,"isClosed":true,"hours":[{}]},{"dayOfWeek":1,"isClosed":true,"hours":[{}]},{"dayOfWeek":2,"isClosed":true,"hours":[{}]},{"dayOfWeek":3,"isClosed":true,"hours":[{}]},{"dayOfWeek":4,"isClosed":true,"hours":[{}]},{"dayOfWeek":5,"isClosed":true,"hours":[{}]},{"dayOfWeek":6,"isClosed":true,"hours":[{}]}]}]}]},
                 {"stepId":"staff","isPassed":true,"isCurrent":false,"isChanged":false,
                 "questions":[{"staffValues":[{"staffLocation":"serenity hair salon, 14545 South Military Trail, Delray Beach, US 33484","id":"ad092afc-6050-b4f3-b550-01749478711b","staffAmount":1},{"staffLocation":"serenity hair salon, 14545c South Military Trail, Delray Beach, US 33484","id":"4779ceff-75f7-b099-c72b-017494787193","staffAmount":1}]}]},{"stepId":"bookingCampaignsSelection","isPassed":false,"isCurrent":true,"isChanged":false,"questions":[{"deals":[]}]},{"stepId":"bookingsCampaignsContextPage","isPassed":false,"isCurrent":false,"isChanged":false,"questions":[]},{"stepId":"bookingCampaignsDealSetup","isPassed":false,"isCurrent":false,"isChanged":false,"questions":[{"dealDuration":{"hours":"0","minutes":"15"}},{"dealBlockedTime":{}},{"dealCustomersAmount":1},{"dealVisits":{"id":"singleVisit","isChanged":false,"amount":1}},{"dealLocations":[]},{"dealOpeningHours":{"optionId":"anyTime","openingHours":[]}}]},{"stepId":"bookingCampaignsDealSetupReview","isPassed":false,"isCurrent":false,"isChanged":false,"questions":[{"completedCampaign":{}}]},{"stepId":"grouponAvailability","isPassed":false,"isCurrent":false,"isChanged":false,"questions":[{"grouponAvailability":[]}]},{"stepId":"bookingPolicies","isPassed":false,"isCurrent":false,"isChanged":false,"questions":[{},{"bookingPolicy":2},{"cancellationPolicy":4}]},{"stepId":"bookingNotifications","isPassed":false,"isCurrent":false,"isChanged":false,"questions":[{"emails":[""]},{"notificationFrequency":""}]},{"stepId":"review","isPassed":false,"isCurrent":false,"isChanged":false,"questions":[]}]}
                 