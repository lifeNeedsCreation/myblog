with
    accounts as (select id, country_code, extract(date from created_at) as date from input.accounts where name is not null and country_code in ({country_code})),

    account as (select accounts.* from (select distinct account_id from (select mac_address, min(updated_at) as updated_at from partiko.account_profiles group by mac_address) as a inner join (select account_id, mac_address, updated_at from partiko.account_profiles) as b on a.mac_address = b.mac_address and a.updated_at = b.updated_at) inner join accounts on account_id = id),

    account_memories as (select country_code, account_id, key, value, date, updated_at from account as a inner join partiko.memories as m on id = account_id and date <= extract(date from updated_at) and key = 'experiment_treatment_group_new_invite' and value in ('control', 'treatment')),

    referrals as (select referrer_account_id, referee_account_id, created_at from partiko.referrals),

    account_memories_referrals as (select referee_account_id as account_id, key, value, updated_at from account_memories inner join referrals on account_id = referrer_account_id and updated_at < created_at),

    users_info as (select country_code, account_id, key, value, date from account_memories_referrals inner join account on account_id = id and date >= extract(date from updated_at)),

    app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= '{start_time}' and created_at < '{end_time}'),

    one_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 1 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 1 day)),

    three_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 3 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 3 day)),

    seven_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 7 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 7 day)),

    fourteen_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 14 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 14 day)),

    thirty_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 30 day)),

    app_open_all as (select * from app_open union distinct select * from one_day_app_open union distinct select * from three_day_app_open union distinct select * from fourteen_day_app_open union distinct select * from thirty_day_app_open),

    account_app_open_all as (select country_code, u.account_id as account_id, key, value, u.date as date from users_info as u inner join app_open_all as a on u.account_id = a.account_id and u.date = a.date),

    retention_day_event as (select account_id, date as retention_date from app_open_all where date = extract(date from timestamp'{start_time}')),

    initial_event as (select country_code, account_id, date as initial_date, key, value from account_app_open_all where date != extract(date from timestamp'{start_time}')),

    retention_event as (select country_code, r.account_id as account_id, initial_date, retention_date, date_diff(retention_date, initial_date, day) as date_diff, key, value from retention_day_event as r inner join initial_event as i on r.account_id = i.account_id),

    initial_count as (select country_code, initial_date, key, value, count(distinct account_id) as initial_num from initial_event group by country_code, initial_date, key, value),

    retention_count as (select country_code, initial_date, retention_date, date_diff, key, value, count(distinct account_id) as retention_num from retention_event group by country_code, initial_date, retention_date, date_diff, key, value)
    
    select i.country_code as country_code, i.initial_date as initial_date, retention_date, date_diff, i.key as key, i.value as value, initial_num, retention_num, round(retention_num/initial_num, 4) as retention_rate from initial_count as i inner join retention_count as r on i.country_code = r.country_code and i.initial_date = r.initial_date and i.key = r.key and i.value = r.value order by country_code, initial_date, date_diff, key, value