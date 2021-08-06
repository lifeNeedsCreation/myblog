with
    news_push_info as (
        select news_id, country_code
        from partiko.news_push
    ),

    notification_pushed_info as (
        select p.country_code as country_code, account_id, content_id as news_id, date
        from (
            select distinct country_code, account_id, content_id, extract(date from created_at) as date
            from push.notification_pushed
            where created_at >= '{start_time}'
            and created_at < '{end_time}'
            and content_type = 'news'
        ) as p
        left join news_push_info as n
        on p.country_code = n.country_code
        and content_id = news_id
        where news_id is not null
    ),

    notification_received_info as (
        select country_code, account_id, r.news_id as news_id, date
        from (
            select distinct account_id, safe_cast(json_extract_scalar(data, '$.id') as numeric) as news_id, extract(date from created_at) as date
            from stream_events.notification_received
            where created_at >= '{start_time}'
            and created_at < '{end_time}'
            and json_extract_scalar(data, '$.type') = 'news'
            and json_extract_scalar(DATA,'$.push_id') like format("%s%s%s", "%", format_date('%Y_%m_%d', date '{start_time}'),"%")
        ) as r
        left join news_push_info as n
        on r.news_id = n.news_id
        where n.news_id is not null
    ),

    notification_click_info as (
        select country_code, account_id, c.news_id as news_id, date
        from (
            select distinct account_id, safe_cast(json_extract_scalar(data, '$.id') as numeric) as news_id, extract(date from created_at) as date
            from stream_events.notification_click
            where created_at >= '{start_time}'
            and created_at < '{end_time}'
            and json_extract_scalar(data, '$.type') = 'news'
            and json_extract_scalar(DATA,'$.push_id') like format("%s%s%s", "%", format_date('%Y_%m_%d', date '{start_time}'),"%")
        ) as c
        left join news_push_info as n
        on c.news_id = n.news_id
        where n.news_id is not null
    ),

    pushed_group as (
        select country_code, date, news_id, count(*) as pushed_num
        from notification_pushed_info
        group by country_code, date, news_id
    ),

    received_group as (
        select country_code, date, news_id, count(*) as received_num
        from notification_received_info
        group by country_code, date, news_id
    ),

    click_group as (
        select country_code, date, news_id, count(*) as click_num
        from notification_click_info
        group by country_code, date, news_id
    )

    select a.country_code as country_code, a.date as date, a.news_id as news_id, ifnull(click_num, 0) as click_num, received_num, (case when received_num = 0 then 0 else round(click_num/received_num, 4) end) as click_ratio, pushed_num, received_ratio
    from (
        select p.country_code as country_code, p.date as date, p.news_id as news_id, ifnull(received_num, 0) as received_num, pushed_num, (case when pushed_num = 0 then 0 else round(received_num/pushed_num, 4) end) received_ratio
        from pushed_group as p
        left join received_group as r
        on p.country_code = r.country_code
        and p.date = r.date
        and p.news_id = r.news_id
        ) as a
    left join click_group as c
    on a.country_code = c.country_code
    and a.date = c.date
    and a.news_id = c.news_id
    order by country_code, date, news_id