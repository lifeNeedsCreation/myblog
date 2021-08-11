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
        and account_id != -1
    )

    select country_code, date, count(distinct if(name is null, account_id, null)) as visitor_num, count(distinct if(name is not null, account_id, null)) as user_num, count(distinct account_id) as total_user_num
    from app_open_info
    left join accounts_info
    on account_id = id
    group by country_code, date
    having country_code is not null
    order by country_code, date 