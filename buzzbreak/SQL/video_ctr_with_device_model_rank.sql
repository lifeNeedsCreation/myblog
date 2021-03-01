with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    sessions as (select * from `partiko.sessions`),
    
    device_model_info as (select a.account_id as account_id, device_model from (select account_id, max(updated_at) as max_updated_at from sessions group by account_id) as a inner join sessions on a.account_id = sessions.account_id and max_updated_at = updated_at),

    accounts_info as (select id, country_code, device_model from accounts inner join device_model_info on id = account_id),

    video_click_info as (select account_id, json_extract_scalar(data, "$.placement") as placement, json_extract_scalar(data, "$.id") as video_id, json_extract_array(meta_tag, "$.experiment") as experiments, json_extract_array(meta_tag, "$.ranking_strategy") as strategies from (select *, json_extract_scalar(data, "$.meta_tag") as meta_tag from stream_events.video_click where created_at >= "{start_time}" and created_at < "{end_time}")),

    video_click as (select distinct account_id, placement, video_id, replace(experiment, '"', '') as experiment, replace(strategy, '"', '') as strategy from video_click_info as v 
    cross join unnest(v.experiments) as experiment 
    cross join unnest(v.strategies) as strategy),

    accounts_video_click as (select account_id, device_model, country_code, placement, video_id, experiment, strategy from video_click inner join accounts_info on account_id = id where experiment in ({experiments})),

    video_click_group as (select country_code, placement, experiment, strategy, device_model, count(*) as click_num from accounts_video_click group by country_code, placement, experiment, strategy, device_model),

    video_impression_info as (select account_id, json_extract_scalar(data, "$.placement") as placement, json_extract_scalar(data, "$.id") as video_id, json_extract_array(meta_tag, "$.experiment") as experiments, json_extract_array(meta_tag, "$.ranking_strategy") as strategies from (select *, json_extract_scalar(data, "$.meta_tag") as meta_tag from stream_events.video_impression where created_at >= "{start_time}" and created_at < "{end_time}" and json_extract_scalar(data, "$.placement") not like "immersive%")),

    video_impression as (select distinct account_id, placement, video_id, replace(experiment, '"', '') as experiment, replace(strategy, '"', '') as strategy from video_impression_info as v 
    cross join unnest(v.experiments) as experiment 
    cross join unnest(v.strategies) as strategy),

    accounts_video_impression as (select account_id, device_model, country_code, placement, video_id, experiment, strategy from video_impression inner join accounts_info on account_id = id where experiment in ({experiments})),

    video_impression_group as (select country_code, placement, experiment, strategy, device_model, count(*) as impression_num from accounts_video_impression group by country_code, placement, experiment, strategy, device_model),

    video_impression_union as (select * from video_click union distinct select * from video_impression),

    accounts_video_impression_union as (select account_id, device_model, country_code, placement, video_id, experiment, strategy from video_impression_union inner join accounts_info on account_id = id where experiment in ({experiments})),

    video_impression_union_group as (select country_code, placement, experiment, strategy, device_model, count(*) as impression_union_num from accounts_video_impression_union group by country_code, placement, experiment, strategy, device_model)

    select a.country_code as country_code, a.placement as placement, a.experiment as experiment, a.strategy as strategy, a.device_model as device_model, extract(date from timestamp("{start_time}")) as date, ifnull(click_num, 0) as click_num, impression_num, impression_union_num, round(ifnull(click_num, 0) / impression_num, 4) as ctr, round(ifnull(click_num, 0) / impression_union_num, 4) as ctr_union from (select up.country_code as country_code, up.placement as placement, up.experiment as experiment, up.strategy as strategy, up.device_model as device_model, ifnull(impression_num, 0) as impression_num, impression_union_num from video_impression_union_group as up left join video_impression_group as ip on up.country_code = ip.country_code and up.placement = ip.placement and up.experiment = ip.experiment and up.strategy = ip.strategy and up.device_model = ip.device_model where impression_num is not null) as a left join video_click_group as cp on a.country_code = cp.country_code and a.placement = cp.placement and a.experiment = cp.experiment and a.strategy = cp.strategy and a.device_model = cp.device_model order by country_code, placement, experiment, strategy, device_model