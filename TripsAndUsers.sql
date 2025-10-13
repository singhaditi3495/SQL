-- Databricks notebook source
-- MAGIC %md
-- MAGIC # **TRIPS AND USERS**
-- MAGIC
-- MAGIC write a sql query to find 
-- MAGIC
-- MAGIC 1. The Cancellation rate of requests with banned users (both client and driver must not be banned) 
-- MAGIC each day between "2023-10-01" and "2023-10-03"
-- MAGIC Round Cancellation Rate to two decimal points
-- MAGIC
-- MAGIC 2. The Cancellation rate is computed by dividing the number of canceled (by driver or client) requests with unbanned users by the total number of requests with unbanned users on that day

-- COMMAND ----------

Drop table if exists Trips;
Create table  Trips (id int, client_id int, driver_id int, city_id int, status varchar(50), request_at varchar(50));

Drop table if exists Users;
Create table Users (users_id int, banned varchar(50), role varchar(50));

insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('1', '1', '10', '1', 'completed', '2013-10-01');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('2', '2', '11', '1', 'cancelled_by_driver', '2013-10-01');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('3', '3', '12', '6', 'completed', '2013-10-01');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('4', '4', '13', '6', 'cancelled_by_client', '2013-10-01');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('5', '1', '10', '1', 'completed', '2013-10-02');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('6', '2', '11', '6', 'completed', '2013-10-02');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('7', '3', '12', '6', 'completed', '2013-10-02');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('8', '2', '12', '12', 'completed', '2013-10-03');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('9', '3', '10', '12', 'completed', '2013-10-03');
insert into Trips (id, client_id, driver_id, city_id, status, request_at) values ('10', '4', '13', '12', 'cancelled_by_driver', '2013-10-03');


insert into Users (users_id, banned, role) values ('1', 'No', 'client');
insert into Users (users_id, banned, role) values ('2', 'Yes', 'client');
insert into Users (users_id, banned, role) values ('3', 'No', 'client');
insert into Users (users_id, banned, role) values ('4', 'No', 'client');
insert into Users (users_id, banned, role) values ('10', 'No', 'driver');
insert into Users (users_id, banned, role) values ('11', 'No', 'driver');
insert into Users (users_id, banned, role) values ('12', 'No', 'driver');
insert into Users (users_id, banned, role) values ('13', 'No', 'driver');

-- COMMAND ----------

select * from Trips;

-- COMMAND ----------

select * from Users;

-- COMMAND ----------


select 
*
from Trips t
join Users c
on t.client_id = c.users_id
join Users d
on t.driver_id = d.users_id
where c.banned = 'No'
and d.banned = 'No'


-- COMMAND ----------

Here in case statement we are using null values because it will ignore all the other values apart from the one mentioned in the then condition

-- COMMAND ----------

select 
request_at,
count(request_at) as total_count,
count(case when status in ("cancelled_by_driver","cancelled_by_client") then 1 else null end ) as cancelled_count,
round(count(case when status in ("cancelled_by_driver","cancelled_by_client") then 1 else null end )/count(request_at)*100,2) as cancellation
from Trips t
join Users c
on t.client_id = c.users_id
join Users d
on t.driver_id = d.users_id
where c.banned = 'No'
and d.banned = 'No'
group by t.request_at
order by request_at 

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

