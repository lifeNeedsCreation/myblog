with
    accounts as (
        select id, (case when country_code in ('ID', 'PH', 'TH', 'AR', 'BR') then country_code else 'OT' end) as country_code
        from input.accounts
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
    ),

    video_union_info as (
        select *
        from video_click_info
        union distinct
        select *
        from video_impression_info
    )

    select country_code, date, count(*) as impression_num
    from video_union_info
    group by country_code, date
    order by country_code, date