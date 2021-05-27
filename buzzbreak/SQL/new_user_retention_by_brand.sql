with
    accounts as (select id, country_code, extract(date from created_at) as date from input.accounts where name is not null and country_code in ({country_code})),

    device_model_info as (select a.account_id as account_id, device_model from (select account_id, max(updated_at) as updated_at from partiko.sessions group by account_id) as a inner join (select account_id, device_model, updated_at from partiko.sessions) as b on a.account_id = b.account_id and a.updated_at = b.updated_at),

    brands_info as (select device_model, brand from analytics.brands),

    users_info as (select account_id, country_code, date, ifnull(brand, 'undefined') as brand from (select account_id, country_code, date, device_model from accounts inner join device_model_info on id = account_id) as a left join brands_info as b on a.device_model = b.device_model),

    app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= '{start_time}' and created_at < '{end_time}'),

    one_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 1 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 1 day)),

    three_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 3 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 3 day)),

    seven_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 7 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 7 day)),

    fourteen_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 14 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 14 day)),

    thirty_day_app_open as (select distinct account_id, extract(date from created_at) as date from stream_events.app_open where created_at >= timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 30 day)),

    app_open_all as (select * from app_open union distinct select * from one_day_app_open union distinct select * from three_day_app_open union distinct select * from fourteen_day_app_open union distinct select * from thirty_day_app_open),

    account_app_open_all as (select country_code, u.account_id as account_id, brand, u.date as created_date, a.date as date from users_info as u inner join app_open_all as a on u.account_id = a.account_id and u.date = a.date),

    retention_day_event as (select account_id, date as retention_date from app_open_all where date = extract(date from timestamp'{start_time}')),

    initial_event as (select country_code, account_id, date as initial_date, brand from account_app_open_all where created_date = date and date != extract(date from timestamp'{start_time}')),

    retention_event as (select country_code, r.account_id as account_id, initial_date, retention_date, date_diff(retention_date, initial_date, day) as date_diff, brand from retention_day_event as r inner join initial_event as i on r.account_id = i.account_id),

    initial_count as (select country_code, initial_date, brand, count(distinct account_id) as initial_num from initial_event group by country_code, initial_date, brand),

    retention_count as (select country_code, initial_date, retention_date, date_diff, brand, count(distinct account_id) as retention_num from retention_event group by country_code, initial_date, retention_date, date_diff, brand)
    
    select i.country_code as country_code, i.initial_date as initial_date, retention_date, date_diff, i.brand as brand, initial_num, retention_num, round(retention_num/initial_num, 4) as retention_rate from initial_count as i inner join retention_count as r on i.country_code = r.country_code and i.initial_date = r.initial_date and i.brand = r.brand order by country_code, initial_date, date_diff, brand