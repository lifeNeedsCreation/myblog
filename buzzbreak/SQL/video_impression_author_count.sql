with
    accounts as (
        select id, (case when country_code in ('ID', 'PH', 'TH', 'AR', 'BR') then country_code else 'OT' end) as country_code
        from input.accounts
    ),

    videos_info as (
        select id, account_id
        from partiko.videos
    ),

    video_click_info as (
        select country_code, account_id, video_id, date
        from (
            select distinct account_id, json_extract_scalar(data, '$.id') as video_id, extract(date from created_at) as date
            from stream_events.video_click
            where created_at >= '{start_time}'
            and created_at < '{end_time}'
        )
        left join accounts
        on id = account_id
        where country_code is not null
    ),

    video_impression_info as (
        select country_code, account_id, video_id, date
        from (
            select distinct account_id, json_extract_scalar(data, '$.id') as video_id, extract(date from created_at) as date
            from stream_events.video_impression
            where created_at >= '{start_time}'
            and created_at < '{end_time}'
        )
        left join accounts
        on account_id = id
        where country_code is not null
    ),

    video_union_info as (
        select *
        from video_click_info
        union distinct
        select *
        from video_impression_info
    )

    select country_code, date, count(distinct author_id) as author_num
    from (
        select country_code, u.account_id as account_id, video_id, date, v.account_id as author_id
        from video_union_info as u
        left join videos_info as v
        on safe_cast(video_id as numeric) = id
    )
    where author_id is not null
    group by country_code, date