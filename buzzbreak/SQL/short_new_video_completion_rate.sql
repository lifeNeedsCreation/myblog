with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    video as (select id, category, video_length_seconds from partiko.videos where video_length_seconds < 60*3 and category is not null and created_at > timestamp_sub(timestamp'{start_time}', interval 7 day)),

    video_watch as (select distinct account_id, safe_cast(json_extract_scalar(data, '$.id') as numeric) as video_id, json_extract_scalar(data, '$.placement') as placement, safe_cast(json_extract_scalar(data, '$.duration_in_seconds') as numeric) as duration_in_seconds, extract(date from created_at) as date from stream_events.video_watch where created_at >= '{start_time}' and created_at < '{end_time}'),

    video_watch_update as (select distinct account_id, video_id, (case when placement in ("immersive_videos_tab_popular", "immersive_videos_tab_home", "immersive_videos_tab_home_tab_home_video", "immersive_videos_tab_news_detail_activity", "immersive_videos_tab_home_tab_for_you_video") then "immersive_videos_tab_popular" else placement end) as placement, duration_in_seconds, date from video_watch),

    video_watch_info as (select country_code, a.*, (case when duration_in_seconds > video_length_seconds then video_length_seconds else duration_in_seconds end) as truely_duration_in_seconds from (select v.*, category, video_length_seconds from video_watch_update as v inner join video on video_id = id) as a inner join accounts on account_id = id where video_length_seconds != 0),

    video_watch_num as (select country_code, date, placement, category, count(*) as watch_num from video_watch_info group by country_code, date, placement, category),

    video_watch_rate as (select v.*, round(truely_duration_in_seconds/video_length_seconds, 4) as completion_rate from video_watch_info as v),

    avg_info as (select country_code, date, placement, category, round(avg(truely_duration_in_seconds), 4) as avg_watch_duration, round(avg(video_length_seconds), 4) as avg_video_length, round(avg(completion_rate), 4) as avg_completion_rate from video_watch_rate group by country_code, date, placement, category order by country_code, date desc, placement, category, avg_completion_rate desc),

    median_info as (select * from (select *, row_number() over (partition by country_code, date, placement, category order by completion_rate) as rank, count(*) over (partition by country_code, date, placement, category) as num from video_watch_rate) where rank > (num-0.5)/2 and rank < (num+2.5)/2),

    median_info_result as (select country_code, date, placement, category, avg(completion_rate) as median_completion_rate from median_info group by country_code, date, placement, category)

    select r.country_code as country_code, r.date as date, r.placement as placement, r.category as category, avg_watch_duration, avg_video_length, avg_completion_rate, median_completion_rate, watch_num from (select a.country_code as country_code, a.date as date, a.placement as placement, a.category as category, avg_watch_duration, avg_video_length, avg_completion_rate, median_completion_rate from avg_info as a inner join median_info_result as m on a.country_code = m.country_code and a.date = m.date and a.placement = m.placement and a.category = m.category) as r inner join video_watch_num as w on r.country_code = w.country_code and r.date = w.date and r.placement = w.placement and r.category = w.category order by country_code, date, placement, category