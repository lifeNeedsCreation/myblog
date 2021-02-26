with
    accounts as (select * from input.accounts where name is not null),

    account_profiles as (select * from partiko.account_profiles where mac_address is not null),

    account as ((select id, country_code, created_at from 
                    (select distinct id, country_code, created_at from accounts) 
                    inner join 
                    (select distinct account_id from 
                        (select mac_address, min(created_at) as created_at from account_profiles group by mac_address) as a 
                        inner join 
                        (select account_id, mac_address, created_at from account_profiles) as b 
                        on a.mac_address = b.mac_address and a.created_at = b.created_at) 
                    on id = account_id)),

    immersive_event as (select * from stream_events.video_impression where created_at >= timestamp_sub(timestamp"{start_time}", interval 30 day) and created_at < "{end_time}" and json_extract_scalar(data, "$.placement") like "immersive%" and json_extract_scalar(data, "$.meta_tag") is not null),

    impression_event_info as (select account_id, json_extract_scalar(data, "$.placement") as placement, json_extract_array(meta_tag, "$.experiment") as experiments, json_extract_array(meta_tag, "$.recall_strategy") as strategies, created_at from (select *, json_extract_scalar(data, "$.meta_tag") as meta_tag from immersive_event)),

    impression_event_infos as (select distinct account_id, placement, replace(experiment, '"', '') as experiment, replace(strategy, '"', '') as strategy, extract(date from created_at) as date from impression_event_info as i 
    cross join unnest(i.experiments) as experiment 
    cross join unnest(i.strategies) as strategy),

    impression_event_list as (select * from impression_event_infos where experiment in ("video_recall", "immersive_video_recall", "short_video_recall", "cold_start_video_recall")),

    impression_event_list_update as (select account_id, experiment, strategy, date, (case when placement in ("immersive_videos_tab_popular", "immersive_videos_tab_home", "immersive_videos_tab_home_tab_home_video", "immersive_videos_tab_news_detail_activity") then "immersive_videos_tab_popular" else placement end) as placement from impression_event_list),

    event_target_time as (select * from impression_event_list_update where date = "{start_time}"),

    event_one_month as (select * from impression_event_list_update),
    
    initial_events as (select account_id, country_code, placement, experiment, strategy, date as initial_date from account inner join event_one_month as e on account.id = e.account_id),

    retention_events as (select i.account_id as account_id, i.country_code as country_code, i.placement as placement, i.experiment as experiment, i.strategy as strategy, initial_date, date as retention_date, date_diff(date, initial_date, day) as date_diff from initial_events as i inner join event_target_time as e on i.account_id = e.account_id and i.placement = e.placement and i.experiment = e.experiment and i.strategy = e.strategy),

    initial_events_count as (select country_code, placement, experiment, strategy, initial_date, count(account_id) as initial_num from initial_events group by country_code, placement, experiment, strategy, initial_date),

    retention_events_count as (select country_code, placement, experiment, strategy, initial_date, retention_date, date_diff, count(account_id) as retention_num from retention_events group by country_code, placement, experiment, strategy, initial_date, retention_date, date_diff)

    select i.country_code as country_code, i.placement as placement, i.experiment as experiment, i.strategy as strategy, i.initial_date as initial_date, retention_date, date_diff, initial_num, ifnull(retention_num, 0) as retention_num, round(ifnull(retention_num, 0) / initial_num, 4) as retention_rate from initial_events_count as i left join retention_events_count as r on i.country_code = r.country_code and i.placement = r.placement and i.experiment = r.experiment and i.strategy = r.strategy and i.initial_date = r.initial_date where retention_date is not null and date_diff > 0 order by date_diff, country_code, placement, experiment, strategy, initial_date, retention_date