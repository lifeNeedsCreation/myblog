with
    accounts as (select id, country_code from input.accounts where name is not null),

    video_watch as (select account_id, extract(date from created_at) as created_date, json_extract_scalar(data, "$.placement") as placement, json_extract_scalar(data, "$.id") as video_id from  stream_events.video_watch as video_watch where video_watch.created_at >= "{start_time}" and video_watch.created_at < "{end_time}" and json_extract_scalar(data, "$.placement") in ({placement})),

    video_impression as (select account_id, extract(date from created_at) as created_date, json_extract_scalar(data, "$.placement") as placement from stream_events.video_impression as video_impression where video_impression.created_at >= "{start_time}" and video_impression.created_at < "{end_time}" and json_extract_scalar(data, "$.placement") in ({placement})),

    account_video_watch as (select distinct id, country_code, placement, video_id, created_date from accounts inner join video_watch on accounts.id = video_watch.account_id),

    account_video_impression as (select distinct id, country_code, placement, created_date from accounts inner join video_impression on accounts.id = video_impression.account_id),

    account_video_impression_union as (select distinct id, country_code, placement, created_date from account_video_watch union distinct select distinct id, country_code, placement, created_date from accounts inner join video_impression on accounts.id = video_impression.account_id),

    account_video_watch_group as (select country_code, placement, created_date, count(video_id) as video_watch_num from account_video_watch group by country_code, placement, created_date),

    account_video_impression_group as (select country_code, placement, created_date, count(id) as account_impression_num from account_video_impression group by country_code, placement, created_date),

    account_video_impression_union_group as (select country_code, placement, created_date, count(id) as account_impression_union_num from account_video_impression_union group by country_code, placement, created_date)

    select a.country_code as country_code, a.placement as placement, a.created_date as date, ifnull(video_watch_num, 0) as video_watch_num, account_impression_num, account_impression_union_num, round(ifnull(video_watch_num, 0) / account_impression_num, 5) as watch_avg_num, round(ifnull(video_watch_num, 0) / account_impression_union_num, 5) as watch_avg_num_union from (select iu.country_code as country_code, iu.placement as placement, iu.created_date as created_date, account_impression_num, account_impression_union_num from account_video_impression_union_group as iu left join account_video_impression_group as i on iu.country_code = i.country_code and iu.placement = i.placement and iu.created_date = i.created_date where account_impression_num is not null) as a left join account_video_watch_group as w on a.country_code = w.country_code and a.placement = w.placement and a.created_date = w.created_date