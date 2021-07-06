with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    video_watch_info as (select account_id, json_extract_scalar(data, "$.placement") as placement, json_extract_scalar(meta_tag, "$.recall_bucket") as bucket, json_extract_array(meta_tag, "$.recall_strategy") as strategies, json_extract_scalar(data, "$.id") as video_id from (select *, json_extract_scalar(data, "$.meta_tag") as meta_tag from stream_events.video_watch where created_at >= "{start_time}" and created_at < "{end_time}") where json_extract_scalar(meta_tag, "$.recall_bucket") is not null),

    video_watch as (select account_id, placement, video_id, bucket, replace(strategy, '"', '') as strategy from video_watch_info as v
    cross join unnest(v.strategies) as strategy),

    video_watch_update as (select distinct account_id, bucket, strategy, video_id, (case when placement in ("immersive_videos_tab_popular", "immersive_videos_tab_home", "immersive_videos_tab_home_tab_home_video", "immersive_videos_tab_news_detail_activity", "immersive_videos_tab_news_detail", "immersive_videos_tab_home_tab_for_you_video") then "immersive_videos_tab_popular" else placement end) as placement from video_watch),

    account_video_watch_update as (select country_code, account_id, placement, bucket, strategy, video_id from video_watch_update inner join accounts on account_id = id),

    video_watch_count as (select country_code, placement, bucket, strategy, count(video_id) as watch_num from account_video_watch_update group by country_code, placement, bucket, strategy),

    video_impression_info as (select account_id, json_extract_scalar(data, "$.placement") as placement, json_extract_scalar(meta_tag, "$.recall_bucket") as bucket, json_extract_array(meta_tag, "$.recall_strategy") as strategies, json_extract_scalar(data, "$.id") as video_id from (select *, json_extract_scalar(data, "$.meta_tag") as meta_tag from stream_events.video_impression where created_at >= "{start_time}" and created_at < "{end_time}") where json_extract_scalar(meta_tag, "$.recall_bucket") is not null),

    video_impression as (select account_id, placement, video_id, bucket, replace(strategy, '"', '') as strategy from video_impression_info as v
    cross join unnest(v.strategies) as strategy),

    video_impression_update as (select distinct account_id, bucket, strategy, (case when placement in ("immersive_videos_tab_popular", "immersive_videos_tab_home", "immersive_videos_tab_home_tab_home_video", "immersive_videos_tab_news_detail_activity", "immersive_videos_tab_news_detail", "immersive_videos_tab_home_tab_for_you_video") then "immersive_videos_tab_popular" else placement end) as placement from video_impression),

    account_video_impression_update as (select country_code, account_id, placement, bucket, strategy from video_impression_update inner join accounts on account_id = id),

    video_impression_count as (select country_code, placement, bucket, strategy, count(account_id) as impression_user_num from account_video_impression_update group by country_code, placement, bucket, strategy),

    video_impression_union as (select distinct account_id, placement, bucket, strategy from video_watch union distinct select distinct account_id, placement, bucket, strategy from video_impression),

    video_impression_union_update as (select distinct account_id, bucket, strategy, (case when placement in ("immersive_videos_tab_popular", "immersive_videos_tab_home", "immersive_videos_tab_home_tab_home_video", "immersive_videos_tab_news_detail_activity", "immersive_videos_tab_news_detail", "immersive_videos_tab_home_tab_for_you_video") then "immersive_videos_tab_popular" else placement end) as placement from video_impression_union),

    account_video_impression_union_update as (select country_code, account_id, placement, bucket, strategy from video_impression_union_update inner join accounts on account_id = id),

    video_impression_union_count as (select country_code, placement, bucket, strategy, count(account_id) as impression_union_user_num from account_video_impression_union_update group by country_code, placement, bucket, strategy)

    select a.country_code as country_code, a.placement as placement, a.bucket as bucket, a.strategy as strategy, extract(date from timestamp"{start_time}") as date, ifnull(watch_num, 0) as watch_num, impression_user_num, impression_union_user_num, round(ifnull(watch_num, 0)/impression_user_num, 4) as watch_avg, round(ifnull(watch_num, 0)/impression_union_user_num, 4) as watch_union_avg from (select vu.country_code as country_code, vu.placement as placement, vu.bucket as bucket, vu.strategy as strategy, impression_user_num, impression_union_user_num from video_impression_union_count as vu left join video_impression_count as v on vu.country_code = v.country_code and vu.placement = v.placement and vu.bucket = v.bucket and vu.strategy = v.strategy where impression_user_num is not null) as a left join video_watch_count as w on a.country_code = w.country_code and a.placement = w.placement and a.bucket = w.bucket and a.strategy = w.strategy