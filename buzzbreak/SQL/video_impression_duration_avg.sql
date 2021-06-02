with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    user_time as (select account_id, safe_cast(json_extract_scalar(data, '$.duration_in_seconds') as numeric) as duration_in_seconds, extract(date from created_at) as date from stream_events.user_time where created_at >= '{start_time}' and created_at < '{end_time}' and json_extract_scalar(data, '$.page') like 'videos%'),

    account_user_time as (select country_code, account_id, duration_in_seconds, date from user_time inner join accounts on account_id = id),

    account_user_time_group as (select country_code, date, sum(duration_in_seconds) as total_duration from account_user_time group by country_code, date),

    account_user_time_user_group as (select country_code, date, count(distinct account_id) as user_num from account_user_time group by country_code, date)

    select g.country_code as country_code, g.date as date, total_duration, user_num, round(total_duration/user_num, 4) as avg_duration from account_user_time_group as g inner join account_user_time_user_group as ug on g.country_code = ug.country_code and g.date = ug.date order by country_code, date