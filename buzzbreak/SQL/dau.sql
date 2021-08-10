with
    accounts_info as (
        select id, name, (case when country_code in ({country_code}) then country_code else 'OT' end) as country_code
        from input.accounts
    ),

    app_open_info as (
        select account_id, extract(date from created_at) as date
        from stream_events_with_device_id.app_open
        where created_at >= '{start_time}'
        and created_at < '{end_time}'
    ),

    all_group as (
        select country_code, date, count(distinct account_id) as total_user_num
        from app_open_info
        left join accounts_info
        on account_id = id
        group by country_code, date
        having country_code is not null
    ),

    user_group as (
        select country_code, date, count(distinct account_id) as user_num
        from (
            select account_id, name, country_code, date
            from app_open_info
            left join accounts_info
            on account_id = id
            where name is not null)
        group by country_code, date
    ),

    visitor_group as (
        select country_code, date, count(distinct account_id) as visitor_num
        from (
            select account_id, name, country_code, date
            from app_open_info
            left join accounts_info
            on account_id = id
            where name is null)
        group by country_code, date
    )

    select a.country_code as country_code, a.date as date, ifnull(visitor_num, 0) as visitor_num, user_num, total_user_num
    from (
        select u.country_code as country_code, u.date as date, ifnull(user_num, 0) as user_num, total_user_num
        from all_group as a
        left join user_group as u
        on a.country_code = u.country_code
        and a.date = u.date
    ) as a
    left join visitor_group as v
    on a.country_code = v.country_code
    and a.country_code = v.country_code
    order by country_code, date