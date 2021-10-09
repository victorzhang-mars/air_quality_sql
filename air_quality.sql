--  Author: Youchen (Victor) Zhang

--.	There is a chance that the tables that we are trying to create might already exist.  
--      If they exist, delete airnow, state, county, and site tables to move forward.
drop table if exists airnow, state, county, site;

--.	Create tables called airnow, state, county, and site using the four provided csv files.
------•	site_id in airnow.csv refers to site_id in site.csv.
------•	state_code in airnow.csv refers to state_code in state.csv
------•	county_code in airnow.csv refers to county_code in county.csv
------•	Load the data from airnow.csv, state.csv, county.csv and site.csv.
create table airnow(
	date date,
	site_id integer,
	site_latitude real,
	site_longtitude real,
	daily_mean_pm10_concentration integer,
	daily_aqi_value integer,
	daily_obs_count integer,
	percent_complete integer,
	state_code integer,
	county_code integer
);

create table site(
	site_id integer,
	site_name varchar(100)
);

create table state(
	state_code integer,
	state varchar(50)
);

create table county(
	county_code integer,
	county varchar(20)
);

copy site from './site.csv' csv header;
copy state from './state.csv' csv header;
copy county from './county.csv' csv header;
copy airnow from './airnow.csv' csv header;

--.	Write a query to find all the rows and columns where the month in the date column is greater (>) than 8.
select *
from airnow
where cast(to_char(airnow.date,'MM') as integer) > 8;
---.	Write a query with EXPLAIN ANALYZE VERBOSE.
explain analyze verbose select * from airnow where cast(to_char(airnow.date,'MM') as integer)>8;
---.	Design and add an index to improve the query performance.
CREATE INDEX air_btree ON airnow USING btree(date);
CLUSTER airnow USING air_btree;
---.	Write a query with EXPLAIN ANALYZE VERBOSE.
explain analyze verbose select * from airnow where cast(to_char(airnow.date,'MM') as integer)>8;
---.	Drop the created index.
drop index air_btree;

--.	Return date, site_id, and daily_aqi_value for the rows which site_name is ‘Azusa'.
---.	Write a query without EXPLAIN ANALYZE VERBOSE and show the output.
select airnow_join_site.date, airnow_join_site.site_id, airnow_join_site.daily_aqi_value
from 
(
	select * 
	from airnow
	join site using(site_id)
) as airnow_join_site
where airnow_join_site.site_name = 'Azusa';

---.	Write a query with EXPLAIN ANALYZE VERBOSE.
explain analyze verbose
select airnow_join_site.date, airnow_join_site.site_id, airnow_join_site.daily_aqi_value
from 
(
	select * 
	from airnow
	join site using(site_id)
) as airnow_join_site
where airnow_join_site.site_name = 'Azusa';
---.	Design and add an index to improve the query performance.
create index site_btree on site using btree(site_id);
cluster site using site_btree;
---.	Write a query with EXPLAIN ANALYZE VERBOSE.
explain analyze verbose
select airnow_join_site.date, airnow_join_site.site_id, airnow_join_site.daily_aqi_value
from 
(
	select * 
	from airnow
	join site using(site_id)
) as airnow_join_site
where airnow_join_site.site_name = 'Azusa';
---.	Drop the created index.
drop index site_btree;


--.		Return all date, state, county, site_name and daily_mean_pm10_concentration 
----	where the month in the date column is 1 ordered by date, county_code, and site_id (all in ascending order).
---.	Write a query without EXPLAIN ANALYZE VERBOSE and show the output.
select airnow_state_county_site.date, airnow_state_county_site.state, airnow_state_county_site.county, airnow_state_county_site.site_name, airnow_state_county_site.daily_mean_pm10_concentration
from 
(
	select *
	from
	(
		select *
		from
		(
			select * 
			from airnow
			join state using (state_code)
		) as airnow_join_state
		join county using (county_code)
	) as airnow_join_state_join_county
	join site using (site_id)
) airnow_state_county_site
where cast(to_char(airnow_state_county_site.date,'MM') as integer)=1
order by airnow_state_county_site.date asc, airnow_state_county_site.county_code asc, airnow_state_county_site.site_id asc;
---.	Write a query with EXPLAIN ANALYZE VERBOSE.
explain analyze verbose
select airnow_state_county_site.date, airnow_state_county_site.state, airnow_state_county_site.county, airnow_state_county_site.site_name, airnow_state_county_site.daily_mean_pm10_concentration
from 
(
	select *
	from
	(
		select *
		from
		(
			select * 
			from airnow
			join state using (state_code)
		) as airnow_join_state
		join county using (county_code)
	) as airnow_join_state_join_county
	join site using (site_id)
) airnow_state_county_site
where cast(to_char(airnow_state_county_site.date,'MM') as integer)=1
order by airnow_state_county_site.date asc, airnow_state_county_site.county_code asc, airnow_state_county_site.site_id asc;
---.	Design and add an index to improve the query performance.
create index state_btree on state using btree(state_code);
cluster state using state_btree;
---.	Write a query with EXPLAIN ANALYZE VERBOSE.
explain analyze verbose
select airnow_state_county_site.date, airnow_state_county_site.state, airnow_state_county_site.county, airnow_state_county_site.site_name, airnow_state_county_site.daily_mean_pm10_concentration
from 
(
	select *
	from
	(
		select *
		from
		(
			select * 
			from airnow
			join state using (state_code)
		) as airnow_join_state
		join county using (county_code)
	) as airnow_join_state_join_county
	join site using (site_id)
) airnow_state_county_site
where cast(to_char(airnow_state_county_site.date,'MM') as integer)=1
order by airnow_state_county_site.date asc, airnow_state_county_site.county_code asc, airnow_state_county_site.site_id asc;
---.	Drop the created index.
drop index state_btree;


--.	Return all the columns ordered by date and site_id (both in ascending order).
---.	Write a query without EXPLAIN ANALYZE VERBOSE and show the output.
select *
from airnow
order by airnow.date asc, airnow.site_id asc;
---.	Write a query with EXPLAIN ANALYZE VERBOSE.
explain analyze verbose
select *
from airnow
order by airnow.date asc, airnow.site_id asc;
---.	Design and add an index to improve the query performance.
create index date_site_id_btree on airnow using btree(date, site_id);
cluster airnow using date_site_id_btree;
---.	Write a query with EXPLAIN ANALYZE VERBOSE.
explain analyze verbose
select *
from airnow
order by airnow.date asc, airnow.site_id asc;
---.	Drop the created index.
drop index date_site_id_btree;

