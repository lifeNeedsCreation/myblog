with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    video_watch_info as (select account_id, json_extract_scalar(data, "$.placement") as placement, json_extract_array(meta_tag, "$.experiment") as experiment, json_extract_scalar(meta_tag, "$.recall_experiment_value") as dimension, json_extract_scalar(data, "$.id") as video_id from (select *, json_extract_scalar(data, "$.meta_tag") as meta_tag from stream_events.video_watch where created_at >= "{start_time}" and created_at < "{end_time}") where meta_tag is not null),

    video_watch as (select distinct account_id, placement, replace(experiment, '"', '') as experiment, replace(dimension, '"', '') as dimension, video_id from video_watch_info as v cross join unnest(v.experiment) as experiment where dimension is not null),

    video_watch_update as (select account_id, experiment, dimension, video_id, (case when placement in ("immersive_videos_tab_popular", "immersive_videos_tab_home", "immersive_videos_tab_home_tab_home_video", "immersive_videos_tab_news_detail_activity", "immersive_videos_tab_home_tab_for_you_video") then "immersive_videos_tab_popular" else placement end) as placement from video_watch where experiment in ({experiments})),

    account_video_watch_update as (select country_code, account_id, placement, experiment, dimension, video_id from video_watch_update inner join accounts on account_id = id),

    video_watch_count as (select country_code, placement, experiment, dimension, count(video_id) as watch_num from account_video_watch_update group by country_code, placement, experiment, dimension),

    video_impression_info as (select account_id, json_extract_scalar(data, "$.placement") as placement, json_extract_array(meta_tag, "$.experiment") as experiment, json_extract_scalar(meta_tag, "$.recall_experiment_value") as dimension from (select *, json_extract_scalar(data, "$.meta_tag") as meta_tag from stream_events.video_impression where created_at >= "{start_time}" and created_at < "{end_time}") where meta_tag is not null),

    video_impression as (select account_id, placement, replace(experiment, '"', '') as experiment, replace(dimension, '"', '') as dimension from video_impression_info as v cross join unnest(v.experiment) as experiment where dimension is not null),

    video_impression_update as (select distinct account_id, experiment, dimension, (case when placement in ("immersive_videos_tab_popular", "immersive_videos_tab_home", "immersive_videos_tab_home_tab_home_video", "immersive_videos_tab_news_detail_activity", "immersive_videos_tab_home_tab_for_you_video") then "immersive_videos_tab_popular" else placement end) as placement from video_impression where experiment in ({experiments})),

    account_video_impression_update as (select country_code, account_id, placement, experiment, dimension from video_impression_update inner join accounts on account_id = id),

    video_impression_count as (select country_code, placement, experiment, dimension, count(account_id) as impression_user_num from account_video_impression_update group by country_code, placement, experiment, dimension),

    video_impression_union as (select distinct account_id, placement, experiment, dimension from video_watch union distinct select distinct account_id, placement, experiment, dimension from video_impression),

    video_impression_union_update as (select distinct account_id, experiment, dimension, (case when placement in ("immersive_videos_tab_popular", "immersive_videos_tab_home", "immersive_videos_tab_home_tab_home_video", "immersive_videos_tab_news_detail_activity", "immersive_videos_tab_home_tab_for_you_video") then "immersive_videos_tab_popular" else placement end) as placement from video_impression_union where experiment in ({experiments})),

    account_video_impression_union_update as (select country_code, account_id, placement, experiment, dimension from video_impression_union_update inner join accounts on account_id = id),

    video_impression_union_count as (select country_code, placement, experiment, dimension, count(account_id) as impression_union_user_num from account_video_impression_union_update group by country_code, placement, experiment, dimension)

    select a.country_code as country_code, a.placement as placement, a.experiment as experiment, a.dimension as dimension, extract(date from timestamp"{start_time}") as date, ifnull(watch_num, 0) as watch_num, impression_user_num, impression_union_user_num, round(ifnull(watch_num, 0)/impression_user_num, 4) as watch_avg, round(ifnull(watch_num, 0)/impression_union_user_num, 4) as watch_union_avg from (select vu.country_code as country_code, vu.placement as placement, vu.experiment as experiment, vu.dimension as dimension, impression_user_num, impression_union_user_num from video_impression_union_count as vu left join video_impression_count as v on vu.country_code = v.country_code and vu.placement = v.placement and vu.experiment = v.experiment and vu.dimension = v.dimension where impression_user_num is not null) as a left join video_watch_count as w on a.country_code = w.country_code and a.placement = w.placement and a.experiment = w.experiment and a.dimension = w.dimension