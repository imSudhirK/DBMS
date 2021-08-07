#q1 done 
(select *
from venues 
where venues.venueid = 1)
union 
(select *
from venues 
where venues.venueid =5);


#q2
select userid, familyname, givenname, dateofjoining 
from users
where not users.dateofjoining < to_timestamp('2012-09-01 00:00:00' , 'YYYY-MM-DD HH24:MI:SS');

#q3 done 
select venueid, name, usercost, monthlymaintenance
from venues
where usercost < 1.0*monthlymaintenance/50.0 and usercost >0;

#q4  done 
select distinct familyname
from users
order by familyname asc limit 10;

#q5 done 
with table1(c1, c2) as(
	select distinct venues.name as c1, users.familyname as c2
	from users, venues
	)
select distinct c1 || c2 as familyname
from table1;


#q6 done 
select starttime 
from reservations, users
where reservations.userid = users.userid and 
	users.familyname = 'Patel' and users.givenname = 'Ram';

#q7 done 
with table1(value) as 
	(select distinct recommender 
	from users)
select userid, familyname, givenname, pincode, phone , recommender, dateofjoining
from users,table1
where table1.value = users.userid
order by familyname, givenname;

#q8 done 
with table1(userid, venueid) as 
	(select distinct userid, reservations.venueid
	from reservations, venues
	where venues.name like '%Tennis%' and 
		venues.venueid = reservations.venueid
	)
select distinct (familyname || ' ' || givenname) as u_name, venues.name
from table1, venues, users
where table1.userid = users.userid and 
	table1.venueid = venues.venueid;



#q9



#q10
with table1(freq, venueid) as(
	select distinct count(starttime) as freq, venueid 
	from reservations
	group by venueid
	)
select table1.venueid, name, freq*1.5 as hours_reserved
from table1, venues
where table1.venueid = venues.venueid 
order by venueid asc;

#q11 done 
with table1 (value) as 
	(select  all recommender
	from users)
select distinct value, count(value) as freq
from table1
where table1.value is not null
group by table1.value
order by table1.value;

#q12 done 
with table1(venueid, uc) as (
	select venueid, count(reservations.userid) as uc
	from reservations, users
	where reservations.userid = users.userid and users.familyname <> 'GUEST'
	group by venueid),
	table2(venueid, ug) as (
	select venueid, count(reservations.userid) as ug
	from reservations, users
	where reservations.userid = users.userid and users.familyname = 'GUEST'
	group by venueid)
select venues.name, (table1.uc*venues.usercost + table2.ug*venues.guestcost) as revenue
from table1, table2, venues
where venues.venueid = table2.venueid and 
	venues.venueid = table1.venueid 
order by revenue desc;
