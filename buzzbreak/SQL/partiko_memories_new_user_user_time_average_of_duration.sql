with
    accounts as (select id, country_code from input.accounts where name is not null and created_at >= "{start_time}" and created_at <"{end_time}"),

    user_time as (select account_id, extract(date from created_at) as created_date, json_extract_scalar(data, "$.page") as page, safe_cast(json_extract_scalar(data, "$.duration_in_seconds") as numeric) as duration_in_seconds from stream_events.user_time where created_at >= "{start_time}" and created_at < "{end_time}"),

    memories as (select account_id, key, value, extract(date from updated_at) as update_date from partiko.memories where value in ({indicator_dimension})),

    app_open as (select distinct account_id, extract(date from created_at) as created_date from stream_events.app_open where created_at >= "{start_time}" and created_at < "{end_time}"),

    experiment_accounts as (select id, country_code, key, value, update_date from accounts inner join memories on id = account_id),
    
    experiment_accounts_user_time as (select id, country_code, key, value, page, duration_in_seconds, created_date from experiment_accounts as a inner join user_time on id = account_id  where created_date >= update_date),

    experiment_account_app_open as (select distinct id, country_code, key, value, created_date from experiment_accounts as a inner join app_open on id = account_id  where created_date >= update_date),

    page_duration_sum as (select country_code, key, value, page, created_date, sum(duration_in_seconds) as duration_sum from experiment_accounts_user_time as u group by country_code, key, value, page, created_date),

    app_open_count as (select country_code, key, value, created_date, count(distinct id) as user_count from experiment_account_app_open as a group by country_code, key, value, created_date)

    select a.country_code as country_code, a.key as treatment_name, a.value as indicator_dimension, a.created_date as created_date, page, user_count, round(ifnull(duration_sum, 0)/user_count, 4) as duration_avg from app_open_count as a left join page_duration_sum as p on a.country_code = p.country_code and a.key = p.key and a.value = p.value and a.created_date = p.created_date