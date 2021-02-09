with
    account_link as (select account_id, case when source='facebook' then concat('https://www.facebook.com/pg/',channel_id,'/posts') when source='youtube' then concat('https://www.youtube.com/channel/',channel_id,'/videos') when source='tiktok' and type='author' then concat('https://www.tiktok.com/@',channel_id,'')when source='clipclaps' then concat('https://ugc.cc.lerjin.com/video/read/user-source?authorId=', channel_id, '&page=0&size=9') else 'not valid source' end as homepage from analytics.crawl_authors where account_id is not null),

    videos as (select id, account_id, country_code, category from partiko.videos where country_code in ({country_code})),
    
    video_click as (select distinct json_extract_scalar(data, "$.id") as video_id, json_extract_scalar(data, "$.placement") as placement, account_id from `stream_events.video_click` where json_extract_scalar(data, "$.placement") in ({placement})),
    
    video_impression as (select distinct json_extract_scalar(data, "$.id") as video_id, json_extract_scalar(data, "$.placement") as placement, account_id from `stream_events.video_impression` where json_extract_scalar(data, "$.placement") in ({placement})),
    
    click_union_impression as (select video_id, placement, account_id from  (select * from video_click union distinct select * from video_impression)),
    
    video_click_group as (select video_id, placement, count(*) as click_num from video_click group by video_id, placement),
    
    video_impression_group as (select video_id, placement, impression_num from (select video_id, placement, count(*) as impression_num from video_impression group by video_id, placement) where impression_num > 10),
    
    video_click_union_impression_group as (select video_id, placement, count(*) as click_union_impression_num from click_union_impression group by video_id, placement),

    video_ctr as (select c.video_id, c.placement, round(click_num/impression_num, 5) as ctr from video_click_group as c inner join video_impression_group as i on c.video_id = i.video_id and c.placement = i.placement),

    video_union_ctr as (select c.video_id, c.placement, round(click_num/click_union_impression_num, 5) as union_ctr from video_click_group as c inner join video_click_union_impression_group as u on c.video_id = u.video_id and c.placement = u.placement), 

    video_all_ctr as (select c.video_id, c.placement, ctr, union_ctr from video_ctr as c inner join video_union_ctr as u on c.video_id = u.video_id and c.placement = u.placement),

    videos_video_all_ctr as (select id, country_code, category, placement, account_id, ctr, union_ctr from video_all_ctr inner join videos on safe_cast(video_all_ctr.video_id as numeric) = videos.id),

    videos_video_ctr_median_odd as (select country_code, category, placement, account_id, union_ctr as ctr from (select country_code, category, placement, account_id, union_ctr, row_number() over(partition by country_code, category, placement, account_id order by union_ctr asc) as id1, row_number() over(partition by country_code, category, placement, account_id order by union_ctr desc) as id2 from videos_video_all_ctr) as newtable where id1 = id2),

    videos_video_ctr_median_even as (select country_code, category, placement, account_id, round(sum(union_ctr)/2, 5) as ctr from (select country_code, category, placement, account_id, union_ctr, row_number() over(partition by country_code, category, placement, account_id order by union_ctr asc) as id1, row_number() over(partition by country_code, category, placement, account_id order by union_ctr desc) as id2 from videos_video_all_ctr) as newtable where abs(id1-id2) = 1 group by country_code, category, placement, account_id),

    videos_video_ctr_median as (select country_code, category, placement, account_id, ctr from videos_video_ctr_median_odd union all select country_code, category, placement, account_id, ctr from videos_video_ctr_median_even),

    result as (select country_code, category, placement, m.account_id as account_id, homepage, ctr from videos_video_ctr_median as m left join account_link as l on m.account_id = l.account_id),

    video_click_union_impression as (select distinct video_id, placement from video_click union distinct select distinct video_id, placement from video_impression),

    videos_data as (select id, account_id, category, placement from videos inner join video_click_union_impression on videos.id = safe_cast(video_click_union_impression.video_id as numeric)),

    video_count as (select r.account_id as account_id, count(*) as num  from videos_data as d left join result as r on d.account_id = r.account_id and d.category = r.category and d.placement = r.placement group by account_id)

    select distinct country_code, category, placement, homepage, ctr, num from result inner join video_count on result.account_id = video_count.account_id order by country_code asc, category desc, placement asc, ctr desc