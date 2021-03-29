with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    video_watch as (select distinct country_code, account_id, json_extract_scalar(data, '$.placement') as placement, safe_cast(json_extract_scalar(data, '$.duration_in_seconds') as numeric) as duration_in_seconds, created_at from stream_events.video_watch inner join accounts on account_id = id where created_at >= '{start_time}' and created_at < '{end_time}'),

    video_watch_update as (select country_code, account_id, (case when placement in ('video_activity', 'immersive_videos_tab_popular', 'immersive_videos_tab_home', 'immersive_videos_tab_home_tab_home_video', 'immersive_videos_tab_news_detail_activity', 'immersive_videos_tab_home_tab_for_you_video') then 'immersive_videos_tab_popular' when placement in ('immersive_vertical_videos_tab_for_you', 'immersive_vertical_videos_tab_home') then 'immersive_vertical_videos_tab_for_you' else placement end) as placement, duration_in_seconds from video_watch),

    duration_group as (select country_code, placement, sum(duration_in_seconds) as sum_duration from video_watch_update group by country_code, placement),

    user_group as (select country_code, placement, count(distinct account_id) as user_num from video_watch_update group by country_code, placement)

    select u.country_code as country_code, u.placement as placement, extract(date from timestamp'{start_time}') as date, sum_duration, user_num, round(sum_duration/user_num, 2) as avg_duration from duration_group as d inner join user_group as u on d.country_code = u.country_code and d.placement = u.placement order by country_code, placement, avg_duration desc