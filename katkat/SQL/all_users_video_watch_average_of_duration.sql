with
    accounts as (select id, country_code from input.accounts where name is not null),

    video_watch as (select account_id, json_extract_scalar(data, "$.placement") as placement, safe_cast(json_extract_scalar(data, "$.duration_in_seconds") as numeric) as duration_in_seconds, extract(date from created_at) as date from stream_events.video_watch where created_at >= "{start_time}" and created_at < "{end_time}" and json_extract_scalar(data, "$.placement") in ({placement})),

    account_video_watch as (select id, country_code, placement, duration_in_seconds, date from accounts inner join video_watch on accounts.id = video_watch.account_id),

    account_count as (select country_code, placement, date, count(distinct id) as placement_users_count from account_video_watch group by country_code, placement, date),

    duration_in_seconds_sum as (select country_code, placement, date, sum(duration_in_seconds) as placement_duration_sum from account_video_watch group by country_code, placement, date)

    select a.country_code, a.placement, a.date, placement_users_count, placement_duration_sum, round(ifnull(placement_duration_sum, 0) / placement_users_count, 4) as placement_duration_avg from account_count as a inner join duration_in_seconds_sum as d on a.country_code = d.country_code and a.placement = d.placement and a.date = d.date