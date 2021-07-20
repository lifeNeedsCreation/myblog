with
    accounts as (
        select id, country_code
        from input.accounts
        where name is not null
        and country_code in ({country_code})
    ),

    active_user as (
        select id, country_code, day as date
        from (
            select account_id, day
            from bdp.stream_events_account_by_day
            where day = date('{start_time}')
        )
        inner join accounts
        on account_id = id
    ),

    ad_click_info as (
        select account_id, country_code, date
        from (
            select distinct account_id, extract(date from created_at) as date
            from stream_events.ad_click
            where created_at >= '{start_time}'
            and created_at < '{end_time}'
        )
        inner join accounts
        on account_id = id
    ),

    active_user_group as (
        select country_code, date, count(distinct id) as active_user_num
        from active_user
        group by country_code, date
    ),

    ad_click_user_group as (
        select country_code, date, count(distinct account_id) as ad_click_user_num
        from ad_click_info
        group by country_code, date
    )

    select a.country_code as country_code, a.date as date, ifnull(ad_click_user_num, 0) as ad_click_user_num,
    round(ifnull(ad_click_user_num, 0)/active_user_num, 4) as ad_click_ratio,
    (active_user_num - ifnull(ad_click_user_num, 0)) as without_ad_click_user_num,
    round((active_user_num - ifnull(ad_click_user_num, 0))/active_user_num, 4) as without_ad_click_ratio,
    active_user_num
    from active_user_group as a
    inner join ad_click_user_group as c
    on a.country_code = c.country_code
    and a.date = c.date