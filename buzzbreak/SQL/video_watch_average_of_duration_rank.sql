with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ("ID")),
    
    video_watch_info as (select account_id, json_extract_scalar(data, "$.placement") as placement, safe_cast(json_extract_scalar(data, "$.duration_in_seconds") as numeric) as duration_in_seconds, json_extract_array(meta_tag, "$.experiment") as experiment, json_extract_array(meta_tag, "$.ranking_strategy") as ranking_strategies from (select *, json_extract_scalar(data, "$.meta_tag") as meta_tag from `stream_events.video_watch` where created_at >= "{start_time}" and created_at < "{end_time}") where meta_tag is not null),
    
    video_watch as (select account_id, placement, duration_in_seconds, experiment, strategy from video_watch_info as v
    cross join unnest(v.experiment) as experiment
    cross join unnest(v.ranking_strategies) as strategy),
    
    account_video_watch as (select account_id, country_code, placement, duration_in_seconds, experiment, strategy from video_watch inner join accounts on account_id = id where experiment not like "%video_recall%"),

    duration_group as (select country_code, placement, experiment, strategy, sum(duration_in_seconds) as duration_sum from account_video_watch group by country_code, placement, experiment, strategy),

    account_count as (select country_code, placement, experiment, strategy, count(distinct account_id) as user_num from account_video_watch group by country_code, placement, experiment, strategy)

    select a.country_code as country_code, a.placement as placement, a.experiment as experiment, a.strategy as strategy, extract(date from timestamp("{start_time}")) as date, user_num, duration_sum, round(duration_sum / user_num, 2) as duration_avg from duration_group as d inner join account_count as a on d.country_code = a.country_code and d.placement = a.placement and d.experiment = a.experiment and d.strategy = a.strategy order by placement, experiment, strategy