with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),
    
    video_watch_info as (select account_id, json_extract_scalar(data, "$.placement") as placement, safe_cast(json_extract_scalar(data, "$.duration_in_seconds") as numeric) as duration_in_seconds, json_extract_array(meta_tag, "$.experiment") as experiment, json_extract_scalar(meta_tag, "$.recall_experiment_value") as dimension from (select *, json_extract_scalar(data, "$.meta_tag") as meta_tag from `stream_events.video_watch` where created_at >= "{start_time}" and created_at < "{end_time}") where meta_tag is not null),
    
    video_watch as (select account_id, placement, duration_in_seconds, replace(experiment, '"', '') as experiment, replace(dimension, '"', '') as dimension from video_watch_info as v
    cross join unnest(v.experiment) as experiment where dimension is not null),

    video_watch_update as (select account_id, experiment, dimension, duration_in_seconds, (case when placement in ("immersive_videos_tab_popular", "immersive_videos_tab_home", "immersive_videos_tab_home_tab_home_video", "immersive_videos_tab_news_detail_activity") then "immersive_videos_tab_popular" else placement end) as placement from video_watch),
    
    account_video_watch as (select account_id, country_code, placement, duration_in_seconds, replace(experiment, '"', '') as experiment, replace(dimension, '"', '') as dimension from video_watch_update inner join accounts on account_id = id where experiment in ({experiments})),

    duration_group as (select country_code, placement, experiment, dimension, sum(duration_in_seconds) as duration_sum from account_video_watch group by country_code, placement, experiment, dimension),

    account_count as (select country_code, placement, experiment, dimension, count(distinct account_id) as user_num from account_video_watch group by country_code, placement, experiment, dimension)

    select a.country_code as country_code, a.placement as placement, a.experiment as experiment, a.dimension as dimension, extract(date from timestamp("{start_time}")) as date, user_num, duration_sum, round(duration_sum / user_num, 2) as duration_avg from duration_group as d inner join account_count as a on d.country_code = a.country_code and d.placement = a.placement and d.experiment = a.experiment and d.dimension = a.dimension order by country_code, placement, experiment, dimension