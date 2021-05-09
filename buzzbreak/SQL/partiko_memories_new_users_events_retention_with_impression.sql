with
    accounts as (select * from input.accounts where name is not null and country_code in ({country_code})),

    account_profiles as (select * from partiko.account_profiles where mac_address is not null),

    memories as (select * from partiko.memories where key like 'experiment%' and value in ({indicator_dimension})),

    app_open as (select * from stream_events.app_open where created_at > timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}'),

    initial_app_open as (select * from input.accounts where name is not null and created_at > timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}'),

    tab_impression as (select * from stream_events.tab_impression where created_at > timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}'),

    account as (select distinct id, country_code, key, value, extract(date from created_at) as created_date from (select id, country_code, created_at from (select distinct id, country_code, created_at from accounts) inner join (select distinct account_id from (select mac_address, min(created_at) as created_at from account_profiles group by mac_address) as a inner join (select account_id, mac_address,created_at from account_profiles) as b on a.mac_address = b.mac_address and a.created_at = b.created_at) on id = account_id) inner join (select distinct account_id, key, value, updated_at from memories) on id = account_id where extract(date from created_at) = extract(date from updated_at)),

    news_impression as (select distinct account_id, extract(date from created_at) as date from stream_events.news_impression where created_at > timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}'),

    video_impression as (select distinct account_id, extract(date from created_at) as date from stream_events.video_impression where created_at > timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}'),

    news_video_impression as (select * from news_impression union distinct select * from video_impression),

    account_news_video_impression as (select id, country_code, key, value, created_date from account inner join news_video_impression on id = account_id and created_date = date),

    events_target_time as (select * from ((select distinct account_id,extract(date from created_at) as date, json_extract_scalar(data, '$.tab') as event from tab_impression where created_at > '{start_time}' and created_at < '{end_time}') union all (select distinct account_id, extract(date from created_at) as date, 'app_open' as event from app_open where created_at > '{start_time}' and created_at < '{end_time}'))),

    events_one_month as (select * from ((select distinct account_id,extract(date from created_at) as date, json_extract_scalar(data, '$.tab') as event from tab_impression) union all (select distinct id as account_id, extract(date from created_at) as date, 'app_open' as event from initial_app_open))),

    initial_events as (select distinct id, country_code, key, value, created_date as initial_date, event from account_news_video_impression inner join events_one_month on id = account_id where created_date = date),

    retention_events as (select distinct id,country_code,key,value,initial_date, date as retention_date, initial_events.event as initial_event, events_target_time.event as retention_event, date_diff(date, initial_date, day) as date_diff from initial_events inner join events_target_time on id = account_id),

    initial_event_count as (select count(distinct id) as initial_users,country_code, key, value, initial_date, event as initial_event from initial_events group by country_code, initial_date, initial_event, key, value),

    retention_event_count as (select count(distinct id) as retention_users, country_code, key, value, initial_date, retention_date, initial_event, retention_event, date_diff from retention_events group by country_code, initial_date, retention_date, initial_event, retention_event, date_diff, key, value)

    select i.country_code as country_code, i.key as treatment_name, i.value as value, i.initial_date as initial_date, retention_date, date_diff, i.initial_event as initial_event, retention_event, initial_users, ifnull(retention_users,0) as retention_users, round(ifnull(retention_users,0)/initial_users, 4) as retention_rate from initial_event_count as i left join retention_event_count as r on i.country_code = r.country_code and i.initial_date = r.initial_date and i.initial_event = r.initial_event and i.key = r.key and i.value = r.value where date_diff is not null and date_diff > 0 and i.initial_event = retention_event