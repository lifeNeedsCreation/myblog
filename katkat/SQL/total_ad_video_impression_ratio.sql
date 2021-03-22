with
accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    ad_impression as (select account_id, "video_feed" as placement from stream_events.ad_impression where created_at >= "{start_time}" and created_at < "{end_time}"),

    accounts_ad_impression as (select account_id, country_code, placement from ad_impression inner join accounts on account_id = id),

    ad_impression_count as (select country_code, placement, count(*) as ad_impression_num from accounts_ad_impression group by country_code, placement),

    video_impression as (select account_id, "video_feed" as placement from stream_events.video_impression where created_at >= "{start_time}" and created_at < "{end_time}"),

    accounts_video_impression as (select account_id, country_code, placement from video_impression inner join accounts on account_id = id),

    video_impression_count as (select country_code, placement, count(*) as video_impression_num from accounts_video_impression group by country_code, placement)

    select v.country_code as country_code, v.placement as placement, extract(date from timestamp"{start_time}") as date, ifnull(ad_impression_num, 0) as ad_impression_num, video_impression_num, round(ifnull(ad_impression_num, 0)/video_impression_num, 4) as ad_video_impression_ratio from video_impression_count as v left join ad_impression_count as a on v.country_code = a.country_code and v.placement = a.placement