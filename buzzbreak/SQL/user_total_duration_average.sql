with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),
    
    user_time as (select distinct account_id, safe_cast(json_extract_scalar(data, '$.duration_in_seconds') as numeric) as duration, extract(date from created_at) as date from stream_events.user_time where created_at >= '{start_time}' and created_at < '{end_time}' and json_extract_scalar(data, '$.page') = 'app'),
    
    account_user_time_total as (select country_code, date, sum(duration) as sum_duration from user_time inner join accounts on account_id = id group by country_code, date order by sum_duration desc),

    account_user_time_group as (select country_code, date, count(distinct account_id) as user_num from user_time inner join accounts on account_id = id group by country_code, date order by user_num desc)

    select t.country_code as country_code, t.date as date, sum_duration, user_num, round(sum_duration / user_num, 4) as avg_duration from account_user_time_total as t inner join account_user_time_group as g on t.country_code = g.country_code and t.date = g. order by country_code, date