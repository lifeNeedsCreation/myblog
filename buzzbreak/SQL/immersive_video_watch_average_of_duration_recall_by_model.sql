with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),
    
    video_watch_info as (select account_id, json_extract_scalar(data, "$.placement") as placement, safe_cast(json_extract_scalar(data, "$.duration_in_seconds") as numeric) as duration_in_seconds, json_extract_scalar(meta_tag, "$.recall_bucket") as bucket, json_extract_array(meta_tag, "$.recall_strategy") as strategies from (select *, json_extract_scalar(data, "$.meta_tag") as meta_tag from `stream_events.video_watch` where created_at >= "{start_time}" and created_at < "{end_time}") where json_extract_scalar(meta_tag, "$.recall_bucket") is not null),

    video_watch as (select account_id, placement, duration_in_seconds, bucket, replace(strategy, '"', '') as strategy from video_watch_info as v
    cross join unnest(v.strategies) as strategy),

    video_watch_update as (select distinct account_id, bucket, strategy, duration_in_seconds, (case when placement in ("immersive_videos_tab_popular", "immersive_videos_tab_home", "immersive_videos_tab_home_tab_home_video", "immersive_videos_tab_news_detail_activity", "immersive_videos_tab_news_detail", "immersive_videos_tab_home_tab_for_you_video") then "immersive_videos_tab_popular" else placement end) as placement from video_watch),
    
    account_video_watch as (select account_id, country_code, placement, duration_in_seconds, bucket, strategy from video_watch_update inner join accounts on account_id = id),

    duration_group as (select country_code, placement, bucket, strategy, sum(duration_in_seconds) as duration_sum from account_video_watch group by country_code, placement, bucket, strategy),

    account_count as (select country_code, placement, bucket, strategy, count(distinct account_id) as user_num from account_video_watch group by country_code, placement, bucket, strategy)

    select a.country_code as country_code, a.placement as placement, a.bucket as bucket, a.strategy as strategy, extract(date from timestamp("{start_time}")) as date, user_num, duration_sum, round(duration_sum / user_num, 2) as duration_avg from duration_group as d inner join account_count as a on d.country_code = a.country_code and d.placement = a.placement and d.bucket = a.bucket and d.strategy = a.strategy order by placement, bucket, strategy