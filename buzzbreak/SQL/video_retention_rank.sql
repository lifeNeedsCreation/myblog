with
    accounts as (select * from input.accounts where name is not null and country_code in ({country_code})),

    video_impression_info as (select account_id, json_extract_scalar(data, '$.placement') as placement, json_extract_array(meta_tag, '$.experiment') as experiment, json_extract_array(meta_tag, '$.ranking_strategy') as rank_strategies, created_at from (select *, json_extract_scalar(data, '$.meta_tag') as meta_tag from stream_events.video_impression where created_at >= '{start_time}' and created_at < '{end_time}')),

    video_impression_one_day_info as (select account_id, json_extract_scalar(data, '$.placement') as placement, json_extract_array(meta_tag, '$.experiment') as experiment, json_extract_array(meta_tag, '$.ranking_strategy') as rank_strategies, created_at from (select *, json_extract_scalar(data, '$.meta_tag') as meta_tag from stream_events.video_impression where created_at >= timestamp_sub(timestamp'{start_time}', interval 1 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 1 day))),

    video_impression_seven_day_info as (select account_id, json_extract_scalar(data, '$.placement') as placement, json_extract_array(meta_tag, '$.experiment') as experiment, json_extract_array(meta_tag, '$.ranking_strategy') as rank_strategies, created_at from (select *, json_extract_scalar(data, '$.meta_tag') as meta_tag from stream_events.video_impression where created_at >= timestamp_sub(timestamp'{start_time}', interval 7 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 7 day))),

    video_impression_thirty_day_info as (select account_id, json_extract_scalar(data, '$.placement') as placement, json_extract_array(meta_tag, '$.experiment') as experiment, json_extract_array(meta_tag, '$.ranking_strategy') as rank_strategies, created_at from (select *, json_extract_scalar(data, '$.meta_tag') as meta_tag from stream_events.video_impression where created_at >= timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < timestamp_sub(timestamp'{end_time}', interval 30 day))),

    video_impression_all_info as (select * from video_impression_info union all select * from video_impression_one_day_info union all select * from video_impression_seven_day_info union all select * from video_impression_thirty_day_info),

    accounts_video_impression_all_info as (select country_code, account_id, placement, experiment, rank_strategies, v.created_at as created_at from video_impression_all_info as v inner join accounts on account_id = id),

    video_impression as (select distinct country_code, account_id, placement, replace(experiment, '"', '') as experiment, replace(rank_strategy, '"', '') as rank_strategy, extract(date from created_at) as date from accounts_video_impression_all_info as v 
    cross join unnest(v.experiment) as experiment
    cross join unnest(v.rank_strategies) as rank_strategy),

    video_impression_rank as (select * from video_impression where experiment in ({experiments})),

    retention_day_event as (select * from video_impression_rank where date = extract(date from timestamp'{start_time}')),

    initial_event as (select country_code, account_id, placement, experiment, rank_strategy, date as initial_date from video_impression_rank where date != extract(date from timestamp'{start_time}')),

    retention_event as (select r.country_code as country_code, r.account_id as account_id, r.placement as placement, initial_date, r.date as retention_date, date_diff(r.date, initial_date, day) as date_diff, r.experiment as experiment, r.rank_strategy as rank_strategy, from retention_day_event as r inner join initial_event as i on r.account_id = i.account_id and r.placement = i.placement and r.experiment = i.experiment and r.rank_strategy = i.rank_strategy),

    initial_count as (select country_code, placement, experiment, rank_strategy, initial_date, count(*) as initial_num from initial_event group by country_code, placement, experiment, rank_strategy, initial_date),

    retention_count as (select country_code, placement, initial_date, retention_date, date_diff, experiment, rank_strategy, count(*) as retention_num from retention_event group by country_code, placement, initial_date, retention_date, date_diff, experiment, rank_strategy)

    select i.country_code as country_code, i.placement as placement, i.initial_date as initial_date, retention_date, date_diff, i.experiment as experiment, i.rank_strategy as strategy, initial_num, ifnull(retention_num, 0) as retention_num, round(ifnull(retention_num, 0)/initial_num, 4) as retention_rate from initial_count as i left join retention_count as r on i.country_code = r.country_code and i.placement = r.placement and i.initial_date = r.initial_date and i.experiment = r.experiment and i.rank_strategy = r.rank_strategy where retention_date is not null and date_diff is not null order by country_code, placement, experiment, strategy, initial_date, date_diff