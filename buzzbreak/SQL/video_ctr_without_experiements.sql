with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    video_click as (select distinct country_code, account_id, json_extract_scalar(data, '$.placement') as placement, json_extract_scalar(data, '$.id') as video_id from stream_events.video_click inner join accounts on account_id = id where created_at >= '{start_time}' and created_at < '{end_time}'),

    video_click_update as (select distinct country_code, account_id, (case when placement in ('home_tab_for_you', 'home_tab_home') then 'home_tab_for_you' when placement in ('home_tab_for_you_video', 'home_tab_home_video') then 'home_tab_for_you_video' else placement end) as placement, video_id from video_click),

    video_click_group as (select country_code, placement, count(*) as click_num from video_click_update group by country_code, placement),

    video_impression as (select distinct country_code, account_id, json_extract_scalar(data, '$.placement') as placement, json_extract_scalar(data, '$.id') as video_id from stream_events.video_impression inner join accounts on account_id = id where created_at >= '{start_time}' and created_at < '{end_time}'),

    video_impression_update as (select distinct country_code, account_id, (case when placement in ('home_tab_for_you', 'home_tab_home') then 'home_tab_for_you' when placement in ('home_tab_for_you_video', 'home_tab_home_video') then 'home_tab_for_you_video' else placement end) as placement, video_id from video_impression),

    video_impression_group as (select country_code, placement, count(*) as impression_num from video_impression_update where placement not like 'immersive%' group by country_code, placement),

    video_click_union_impression as (select * from video_click union distinct select * from video_impression),

    video_click_union_impression_update as (select distinct country_code, account_id, (case when placement in ('home_tab_for_you', 'home_tab_home') then 'home_tab_for_you' when placement in ('home_tab_for_you_video', 'home_tab_home_video') then 'home_tab_for_you_video' else placement end) as placement, video_id from video_click_union_impression),

    video_click_union_impression_group as (select country_code, placement, count(*) as impression_union_num from video_click_union_impression_update where placement not like 'immersive%' group by country_code, placement)

    select a.country_code as country_code, a.placement as placement, extract(date from timestamp'{start_time}') as date, ifnull(click_num, 0) as click_num, impression_num, impression_union_num, round(ifnull(click_num, 0)/impression_num, 4) as ctr, round(ifnull(click_num, 0)/impression_union_num, 4) as ctr_union from (select up.country_code as country_code, up.placement as placement, impression_num, impression_union_num from video_click_union_impression_group as up left join video_impression_group as ip on up.country_code = ip.country_code and up.placement = ip.placement where impression_num is not null) as a left join video_click_group as cp on a.country_code = cp.country_code and a.placement = cp.placement order by country_code, placement