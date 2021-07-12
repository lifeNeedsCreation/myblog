with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    user_time_info as (select country_code, account_id, page, duration_in_seconds, date from (select account_id, json_extract_scalar(data, '$.page') as page, safe_cast(json_extract_scalar(data, '$.duration_in_seconds') as numeric) as duration_in_seconds, extract(date from created_at) as date from stream_events.user_time where created_at >= '{start_time}' and created_at < '{end_time}') inner join accounts on account_id = id where page != 'app'),

    user_time_time_group as (select country_code, date, page, sum(duration_in_seconds) as total_time from user_time_info group by country_code, date, page),

    user_time_people_group as (select country_code, date, page, count(distinct account_id) as user_count from user_time_info group by country_code, date, page)

    select p.country_code as country_code, p.date as date, p.page as page, total_time, user_count, (case when user_count = 0 then 0 else round(total_time/user_count, 4) end) as avg_time from user_time_time_group as t inner join user_time_people_group as p on t.country_code = p.country_code and t.date = p.date and t.page = p.page order by country_code, date, avg_time desc