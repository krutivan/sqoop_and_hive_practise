use practical_exercise_1;

CREATE TABLE if NOT EXISTS user_total
(time_ran timestamp, total_users INT, users_added INT);


INSERT INTO TABLE user_total 
select from_unixtime(unix_timestamp()) as time_ran, cast(uc.total_users as INT) as total_users, 
cast((uc.total_users- coalesce(prev.prev_total,0)) as INT) as users_added from
(select count(*) as total_users from user) uc
join 
(select max(total_users) as prev_total from user_total) prev;

drop TABLE if exists user_report;
CREATE TABLE user_report as 
select 
u.id as user_id,
COALESCE(totals.total_updates,0) as total_updates,
COALESCE(totals.total_inserts,0) as total_inserts,
COALESCE(totals.total_deletes,0) as total_deletes,
last_activity.last_activity_type,
COALESCE(uploads.upload_count,0) as upload_count,
CASE when active.is_active=1 then cast(1 as boolean) else cast(0 as boolean) end as is_active
from
user u 
left join
(
    select user_id, 
    sum(case when type = 'UPDATE' then 1 else 0 end) as total_updates,
    sum(case when type = 'DELETE' then 1 else 0 end) as total_deletes,
    sum(case when type = 'INSERT' then 1 else 0 end) as total_inserts
    from activitylog group by user_id
) totals 
on u.id = totals.user_id
left join
(
    select user_id, type as last_activity_type from 
    (select user_id, type, `timestamp`, rank() over (partition by user_id order by `timestamp` desc) as rank from activitylog) ranked_types
    WHERE ranked_types.rank = 1
) last_activity 
on u.id = last_activity.user_id
left join
(
    select user_id,count(*) as upload_count from user_upload_dump group by user_id
) uploads
on u.id = uploads.user_id
left join
(
    select user_id,
    max(case when datediff(from_unixtime(unix_timestamp()), (from_unixtime(`timestamp`),'yyyy-MM-dd HH:mm:ss'))<=2 then 1 else 0 end) as is_active
    from activitylog
    group by user_id
) active
on u.id = active.user_id;

