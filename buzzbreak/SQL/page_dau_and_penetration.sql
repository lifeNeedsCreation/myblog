with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    user_time_info as (select country_code, account_id, page, date from (select distinct account_id, json_extract_scalar(data, '$.page') as page, extract(date from created_at) as date from stream_events.user_time where created_at >= '{start_time}' and created_at < '{end_time}') inner join accounts on account_id = id where page != 'app'),

    app_open_info as (select country_code, account_id, date from (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= '{start_time}' and created_at < '{end_time}') inner join accounts on account_id = id),

    user_time_group as (select country_code, date, page, count(account_id) as page_count from user_time_info group by country_code, date, page),

    app_open_group as (select country_code, date, count(account_id) as app_open_count from app_open_info group by country_code, date)

    select a.country_code as country_code, a.date as date, page, ifnull(page_count, 0) as page_count, app_open_count, round(ifnull(page_count, 0)/app_open_count, 4) as penetration_rate from app_open_group as a left join user_time_group as u on a.country_code = u.country_code and a.date = u.date order by penetration_rate desc