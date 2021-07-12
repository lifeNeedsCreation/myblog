with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    uesr_time_end as (select country_code, account_id, page, date as retention_date from (select distinct account_id, json_extract_scalar(data, '$.page') as page, extract(date from created_at) as date from stream_events.user_time where created_at >= '{start_time}' and created_at < '{end_time}') inner join accounts on account_id = id),

    uesr_time_start as (select country_code, account_id, page, date as initial_date from (select distinct account_id, json_extract_scalar(data, '$.page') as page, extract(date from created_at) as date from stream_events.user_time where created_at >= timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 1 day)) inner join accounts on account_id = id),

    user_time_retention as (select s.country_code as country_code, s.account_id as account_id, s.page as page, initial_date, retention_date, date_diff(retention_date, initial_date, day) as date_diff from uesr_time_start as s inner join uesr_time_end as e on s.country_code = e.country_code and s.account_id = e.account_id and s.page = e.page),

    uesr_time_start_group as (select country_code, initial_date, page, count(account_id) as initial_count from uesr_time_start group by country_code, initial_date, page),

    uesr_time_retention_group as (select country_code, initial_date, retention_date, date_diff, page, count(account_id) as retention_count from user_time_retention group by country_code, initial_date, retention_date, date_diff, page)

    select s.country_code as country_code, s.page as page, s.initial_date as initial_date, retention_date, date_diff, initial_count, ifnull(retention_count, 0) as retention_count, round(ifnull(retention_count, 0)/initial_count, 4) as retention_rate from uesr_time_start_group as s left join uesr_time_retention_group as r on s.country_code = r.country_code and s.page = r.page and s.initial_date = r.initial_date where retention_date is not null