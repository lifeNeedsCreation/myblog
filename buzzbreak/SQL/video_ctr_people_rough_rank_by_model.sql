with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    video_click_info as (select account_id, json_extract_scalar(data, "$.placement") as placement, json_extract_scalar(data, "$.id") as video_id, json_extract_scalar(meta_tag, "$.ranking_bucket") as bucket, json_extract_scalar(meta_tag, "$.ranking_strategy") as strategy from (select *, json_extract_scalar(data, "$.meta_tag") as meta_tag from stream_events.video_click where created_at >= "{start_time}" and created_at < "{end_time}") where json_extract_scalar(meta_tag, "$.ranking_bucket") is not null and json_extract_scalar(meta_tag, "$.ranking_bucket") != ''),

    accounts_video_click as (select account_id, country_code, placement, video_id, bucket, strategy from video_click_info inner join accounts on account_id = id),

    video_click_group as (select country_code, placement, bucket, strategy, count(distinct account_id) as click_num from accounts_video_click group by country_code, placement, bucket, strategy),

    video_impression_info as (select account_id, json_extract_scalar(data, "$.placement") as placement, json_extract_scalar(data, "$.id") as video_id, json_extract_scalar(meta_tag, "$.ranking_bucket") as bucket, json_extract_scalar(meta_tag, "$.ranking_strategy") as strategy from (select *, json_extract_scalar(data, "$.meta_tag") as meta_tag from stream_events.video_impression where created_at >= "{start_time}" and created_at < "{end_time}" and json_extract_scalar(data, "$.placement") not like "immersive%") where json_extract_scalar(meta_tag, "$.ranking_bucket") is not null and json_extract_scalar(meta_tag, "$.ranking_bucket") != ''),

    accounts_video_impression as (select account_id, country_code, placement, video_id, bucket, strategy from video_impression_info inner join accounts on account_id = id),

    video_impression_group as (select country_code, placement, bucket, strategy, count(distinct account_id) as impression_num from accounts_video_impression group by country_code, bucket, placement, strategy),

    video_impression_union as (select * from video_click_info union distinct select * from video_impression_info),

    accounts_video_impression_union as (select account_id, country_code, placement, video_id, bucket, strategy from video_impression_union inner join accounts on account_id = id),

    video_impression_union_group as (select country_code, placement, bucket, strategy, count(distinct account_id) as impression_union_num from accounts_video_impression_union group by country_code, placement, bucket, strategy)

    select a.country_code as country_code, a.placement as placement, a.bucket as bucket, a.strategy as strategy, extract(date from timestamp("{start_time}")) as date, ifnull(click_num, 0) as click_num, impression_num, impression_union_num, round(ifnull(click_num, 0) / impression_num, 4) as ctr, round(ifnull(click_num, 0) / impression_union_num, 4) as ctr_union from (select up.country_code as country_code, up.placement as placement, up.bucket as bucket, up.strategy as strategy, impression_num, impression_union_num from video_impression_union_group as up left join video_impression_group as ip on up.country_code = ip.country_code and up.placement = ip.placement and up.bucket = ip.bucket and up.strategy = ip.strategy where impression_num is not null) as a left join video_click_group as cp on a.country_code = cp.country_code and a.placement = cp.placement and a.bucket = cp.bucket and a.strategy = cp.strategy