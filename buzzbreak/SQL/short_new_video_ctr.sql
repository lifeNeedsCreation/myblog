with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    videos as (select id, category from partiko.videos where category is not null and video_length_seconds < 60*3 and created_at > timestamp_sub(timestamp'{start_time}', interval 7 day)),

    video_click as (select distinct account_id, safe_cast(json_extract_scalar(data, '$.id') as numeric) as video_id, json_extract_scalar(data, '$.placement') as placement, created_at from stream_events.video_click where created_at >= '{start_time}' and created_at < '{end_time}'),

    video_impression as (select distinct account_id, safe_cast(json_extract_scalar(data, '$.id') as numeric) as video_id, json_extract_scalar(data, '$.placement') as placement, created_at from stream_events.video_impression where created_at >= '{start_time}' and created_at < '{end_time}' and json_extract_scalar(data, '$.placement') not like 'immersive%'),

    video_click_info as (select country_code, account_id, video_id, category, placement, created_at, date from (select account_id, video_id, category, placement, created_at, extract(date from created_at) as date from video_click inner join videos on video_id = id) inner join accounts on account_id = id),

    video_impression_info as (select country_code, account_id, video_id, category, placement, created_at, date from (select account_id, video_id, category, placement, created_at, extract(date from created_at) as date from video_impression inner join videos on video_id = id) inner join accounts on account_id = id),

    video_impression_union_info as  (select * from video_click_info union distinct select * from video_impression_info),

    video_click_group as (select country_code, date, category, placement, count(*) as click_num from video_click_info group by country_code, date, category, placement),

    video_impression_group as (select country_code, date, category, placement, count(*) as impression_num from video_impression_info group by country_code, date, category, placement),

    video_impression_union_group as (select country_code, date, category, placement, count(*) as  impression_union_num from video_impression_union_info group by country_code, date, category, placement)

    select a.country_code as country_code, a.date as date, a.category as category, a.placement as placement, ifnull(click_num, 0) as click_num, impression_num, impression_union_num, round(ifnull(click_num, 0)/impression_num, 4) as ctr, round(ifnull(click_num, 0)/impression_union_num, 4) as ctr_union from (select iu.country_code as country_code, iu.date as date, iu.category as category, iu.placement as placement, impression_num, impression_union_num from video_impression_union_group as iu left join video_impression_group as i on iu.country_code = i.country_code and iu.date = i.date and iu.category = i.category and iu.placement = i.placement where impression_num is not null) as a left join video_click_group as c on a.country_code = c.country_code and a.date = c.date and a.category = c.category and a.placement = c.placement order by date desc, country_code, category, placement