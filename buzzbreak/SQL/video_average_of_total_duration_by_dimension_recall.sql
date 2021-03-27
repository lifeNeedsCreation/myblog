with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),
    
    user_time as (select distinct account_id, safe_cast(json_extract_scalar(data, '$.duration_in_seconds') as numeric) as duration, created_at from stream_events.user_time where created_at >= '{start_time}' and created_at < '{end_time}' and json_extract_scalar(data, '$.page') = 'app'),
    
    account_user_time_total as (select country_code, account_id, sum(duration) as sum_duration from user_time inner join accounts on account_id = id group by country_code, account_id order by sum_duration desc),
    
    video_impression_info as (select account_id, json_extract_array(meta_tag, '$.experiment') as experiment, json_extract_scalar(meta_tag, '$.recall_experiment_value') as dimension from (select *, json_extract_scalar(data, '$.meta_tag') as meta_tag from stream_events.video_impression where created_at >= '{start_time}' and created_at < '{end_time}')),

    video_impression as (select account_id, replace(experiment, '"', '') as experiment, replace(dimension, '"', '') as dimension from video_impression_info as v 
    cross join unnest(v.experiment) as experiment where dimension is not null),

    account_video_impression as (select distinct country_code, account_id, experiment, dimension from video_impression inner join accounts on account_id = id where experiment in ({experiments})),

    account_experiment_user_time_total as (select v.country_code as country_code, v.account_id as account_id, experiment, dimension, sum_duration from account_video_impression as v inner join account_user_time_total as u on v.country_code = u.country_code and v.account_id = u.account_id),

    rank_user_time_total as (select *, ROW_NUMBER() OVER (PARTITION BY country_code, experiment, dimension ORDER BY sum_duration) as rank from account_experiment_user_time_total),

    valid_experiment_user_time_total as (select * from (select r.country_code as country_code, r.account_id as account_id, r.experiment as experiment, r.dimension as dimension, r.sum_duration as sum_duration, rank, max_rank from rank_user_time_total as r inner join (select country_code, experiment, dimension, max(rank) as max_rank from rank_user_time_total group by country_code, experiment, dimension) as a on r.country_code = a.country_code and r.experiment = a.experiment and r.dimension = a.dimension) where rank BETWEEN max_rank*0.05 AND max_rank*0.95),

    experiment_user_time as (select country_code, experiment, dimension, sum(sum_duration) as experiment_sum_duration from valid_experiment_user_time_total group by country_code, experiment, dimension),

    experiment_user_count as (select country_code, experiment, dimension, count(account_id) as user_num from valid_experiment_user_time_total group by country_code, experiment, dimension)

    select t.country_code as country_code, t.experiment as experiment, t.dimension as dimension, extract(date from timestamp('{start_time}')) as date, experiment_sum_duration, user_num, round(experiment_sum_duration/user_num, 2) as avg_duration from experiment_user_time as t inner join experiment_user_count as c on t.country_code = c.country_code and t.experiment = c.experiment and t.dimension = c.dimension order by experiment, dimension, avg_duration desc