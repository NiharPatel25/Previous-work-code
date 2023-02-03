/* What % of app traffic lands on the notifications tab during their session?*/

SELECT distinct clicktype FROM user_groupondw.m_raw_click where eventdate = '2020-01-01';

describe FORMATTED user_groupondw.m_raw_pageview;

SELECT distinct parenteventid, pageid FROM user_groupondw.m_raw_pageview where eventdate = '2020-01-01';