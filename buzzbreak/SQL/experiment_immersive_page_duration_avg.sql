with
    memories as (select * from partiko.memories),

    accounts as (select * from input.accounts where name is not null and country_code in ({country_code})),

    user_time as (select * from stream_events.user_time where created_at >= "{start_time}" and created_at < "{end_time}"),

    experiment_accounts as (select distinct id, country_code, key, value, updated_at from accounts inner join memories on id = account_id),

    experiment_user_time as (select id, country_code, key, value, json_extract_scalar(data,"$.page") as page, safe_cast(json_extract_scalar(data,"$.duration_in_seconds") as numeric) as duration_in_seconds from experiment_accounts inner join user_time on id = account_id where extract(date from created_at) >= extract(date from updated_at) and json_extract_scalar(data,"$.page")  like "immersive%" and key like "experiment%"),

    info as (select *, ROW_NUMBER() OVER (PARTITION BY country_code, key, value, page ORDER BY duration_in_seconds) as rank from experiment_user_time),

    valid_experiment_user_time as (select * from (select info.*, max_rank from info inner join (select country_code, key, value, page, MAX(rank) as max_rank from info group by country_code, key, value, page) as a on info.country_code = a.country_code and info.key = a.key and info.value = a.value and info.page = a.page) where rank BETWEEN max_rank*0.05 AND max_rank*0.95),

    valid_experiment_user_time_update as (select id, country_code, key, value, duration_in_seconds, (case when page in ("immersive_videos_tab_popular", "immersive_videos_tab_home", "immersive_videos_tab_home_tab_home_video", "immersive_videos_tab_news_detail_activity") then "immersive_videos_tab_popular" else page end) as page from valid_experiment_user_time),

    total_duration as (select country_code, key, value, page, sum(duration_in_seconds) as duration_sum from valid_experiment_user_time_update group by country_code, key, value, page order by country_code, key, value, page, duration_sum desc),

    user_count as (select country_code, key, value, page, count(distinct id) as user_count from valid_experiment_user_time_update group by country_code, key, value, page order by country_code, key, value, page, user_count desc)

    select t.country_code as country_code, t.key as treatment_name, t.value as indimension, t.page as page, extract(date from timestamp("{start_time}")) as date, duration_sum, user_count, round(duration_sum / user_count, 2) as duration_avg from total_duration as t inner join user_count as u on t.country_code = u.country_code and t.key = u.key and t.value = u.value and t.page = u.page order by page, indimension