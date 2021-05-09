with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    video_click_info as (select account_id, json_extract_scalar(data, '$.placement') as placement, json_extract_scalar(data, '$.id') as video_id, json_extract_scalar(meta_tag, '$.recall_bucket') as bucket, extract(date from created_at) as date from (select *, json_extract_scalar(data, '$.meta_tag') as meta_tag from stream_events.video_click where created_at >= '{start_time}' and created_at < '{end_time}') where json_extract_scalar(meta_tag, "$.recall_bucket") is not null),

    accounts_video_click as (select distinct account_id, country_code, placement, video_id, bucket, date from video_click_info inner join accounts on account_id = id),

    click_group as (select country_code, placement, bucket, date, count(*) as click_num from accounts_video_click group by country_code, placement, bucket, date),

    account_group as (select country_code, placement, bucket, date, count(distinct account_id) as user_num from accounts_video_click group by country_code, placement, bucket, date)

    select c.country_code as country_code, c.placement as placement, c.bucket as bucket, c.date as date, click_num, user_num, round(click_num/user_num, 2) as click_average from click_group as c inner join account_group as a on c.country_code = a.country_code and c.placement = a.placement and c.bucket = a.bucket and c.date = a.date order by placement, date, bucket