with
    accounts as (select id, country_code, extract(date from created_at) as date from input.accounts where name is not null and country_code in ({country_code}) and created_at >= timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}'),

    account as (select accounts.* from accounts inner join (select distinct account_id from (select mac_address, min(created_at) as created_at from partiko.account_profiles group by mac_address) as a inner join (select account_id, mac_address, created_at from partiko.account_profiles) as b on a.mac_address = b.mac_address and a.created_at = b.created_at) on id = account_id),

    new_user_total as (select country_code, date, count(distinct id) as new_user_total_num from account group by country_code, date),

    user_time as (select account_id, json_extract_scalar(data, '$.page') as page, safe_cast(json_extract_scalar(data, '$.duration_in_seconds') as numeric) as duration_in_seconds, extract(date from created_at) as date from stream_events.user_time where created_at >= timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}' and json_extract_scalar(data, '$.page') in ('home_tab_for_you', 'home_tab_story', 'videos_tab_popular')),

    user_time_group as (select distinct account_id, date from (select account_id, page, date, sum(duration_in_seconds) as page_time from user_time group by account_id, page, date) where page_time >= 30 and page_time < 3*60),

    immersive_user_time as (select account_id, json_extract_scalar(data, '$.page') as page, safe_cast(json_extract_scalar(data, '$.duration_in_seconds') as numeric) as duration_in_seconds, extract(date from created_at) as date from stream_events.user_time where created_at >= timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}' and json_extract_scalar(data, '$.page') like 'immersive%'),

    immersive_user_time_group as (select distinct account_id, date from (select account_id, date, sum(duration_in_seconds) as page_time from immersive_user_time group by account_id, date) where page_time >= 30 and page_time < 3*60),

    news_read as (select account_id, safe_cast(json_extract_scalar('duration_in_seconds') as numeric) as duration_in_seconds, extract(date from created_at) as date from stream_events.news_read where created_at >= timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}'),

    news_read_group as (select distinct account_id, date from (select account_id, date, sum(duration_in_seconds) as news_read_time from news_read group by account_id, date) where news_read_time >= 30 and news_read_time < 3*60),

    video_watch as (select account_id, safe_cast(json_extract_scalar('duration_in_seconds') as numeric) as duration_in_seconds, extract(date from created_at) as date from stream_events.video_watch where created_at >= timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}'),

    video_watch_group as (select distinct account_id, date from (select account_id, date, sum(duration_in_seconds) as video_watch_time from video_watch group by account_id, date) where video_watch_time >= 30 and video_watch_time < 3*60),

    user_time_all_group as (select * from user_time_group union distinct select * from immersive_user_time_group union distinct select * from news_read_group union distinct select * from video_watch_group),

    account_filter as (select a.* from account as a inner join user_time_group as u on id = account_id and a.date = u.date),

    app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= '{start_time}' and created_at < '{end_time}'),

    one_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 1 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 1 day)),

    three_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 3 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 3 day)),

    seven_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 7 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 7 day)),

    fourty_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 14 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 14 day)),

    thirty_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 30 day)),

    app_open_all as (select * from app_open union distinct select * from one_day_app_open union distinct select * from three_day_app_open union distinct select * from seven_day_app_open union distinct select * from fourty_day_app_open union distinct select * from thirty_day_app_open),

    account_app_open_all as (select id as account_id, country_code, a.date as date from account_filter as f inner join app_open_all as a on id = account_id and f.date = a.date),

    retention_day_event as (select account_id, date as retention_date from app_open_all where date = extract(date from timestamp'{start_time}')),

    initial_event as (select country_code, account_id, date as initial_date from account_app_open_all where date != extract(date from timestamp'{start_time}')),

    retention_event as (select country_code, r.account_id as account_id, initial_date, retention_date, date_diff(retention_date, initial_date, day) as date_diff from retention_day_event as r inner join initial_event as i on r.account_id = i.account_id),

    initial_count as (select country_code, initial_date, count(distinct account_id) as initial_num from initial_event group by country_code, initial_date),

    retention_count as (select country_code, initial_date, retention_date, date_diff, count(distinct account_id) as retention_num from retention_event group by country_code, initial_date, retention_date, date_diff),

    a as (select i.country_code as country_code, i.initial_date as initial_date, retention_date, date_diff, initial_num, retention_num, round(retention_num/initial_num, 4) as retention_rate from initial_count as i inner join retention_count as r on i.country_code = r.country_code and i.initial_date = r.initial_date order by country_code, initial_date, date_diff)

    select a.country_code as country_code, initial_date, retention_date, date_diff, initial_num, new_user_total_num, round(initial_num/new_user_total_num, 4) as ratio, retention_num, retention_rate from a inner join new_user_total as n on a.country_code = n.country_code and date = initial_date