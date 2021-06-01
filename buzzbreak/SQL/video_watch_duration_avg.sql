with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ('ID')),

    video_watch as (select account_id, safe_cast(json_extract_scalar(data, '$.duration_in_seconds') as numeric) as duration_in_seconds, extract(date from created_at) as date from stream_events.video_watch where created_at >= '2021-05-01' and created_at < '2021-06-01'),

    account_video_watch as (select country_code, account_id, duration_in_seconds, date from video_watch inner join accounts on account_id = id),

    account_video_watch_group as (select country_code, date, sum(duration_in_seconds) as total_duration from account_video_watch group by country_code, date),

    account_video_watch_user_group as (select country_code, date, count(distinct account_id) as user_num from account_video_watch group by country_code, date)

    select g.country_code as country_code, g.date as date, total_duration, user_num, round(total_duration/user_num, 4) as avg_duration from account_video_watch_group as g inner join account_video_watch_user_group as ug on g.country_code = ug.country_code and g.date = ug.date order by country_code, date