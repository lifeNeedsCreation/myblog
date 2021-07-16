with
    accounts_info as (
        select id, case when country_code in ({country_code}) THEN country_code
        else 'OT' end as country_code,
        from input.accounts
        where name is not null
        and created_at < '{end_time}'
    ),

    accounts_group as (
        select country_code, count(*) as total_user_num
        from accounts_info
        group by country_code
    ),

    one_level_active as (
        select distinct account_id
        from bdp.stream_events_account_by_day
        where day >= date_sub(DATE '{start_time}', interval 10 day)
        and day < '{end_time}'
    ),

    two_level_active as (
        select distinct account_id
        from bdp.stream_events_account_by_day
        where day >= date_sub(date '{start_time}', interval 30 day)
        and day < '{end_time}'
    ),

    three_level_active as (
        select distinct account_id
        from bdp.stream_events_account_by_day
        where day >= date_sub(date '{start_time}', interval 60 day)
        and day < '{end_time}'
    ),

    one_level_inactive as (
        select country_code, id
        from accounts_info
        where id not in (select account_id from one_level_active)
    ),

    two_level_inactive as (
        select country_code, id
        from accounts_info
        where id not in (select account_id from two_level_active)
    ),

    three_level_inactive as (
        select country_code, id
        from accounts_info
        where id not in (select account_id from three_level_active)
    ),

    one_level_inactive_group as (
        select country_code, count(distinct id) as one_level_inactive_user_num
        from one_level_inactive
        group by country_code
    ),

    two_level_inactive_group as (
        select country_code, count(distinct id) as two_level_inactive_user_num
        from two_level_inactive
        group by country_code
    ),

    three_level_inactive_group as (
        select country_code, count(distinct id) as three_level_inactive_user_num
        from three_level_inactive
        group by country_code
    ),

    level_info as (
        select o.country_code,
            o.one_level_inactive_user_num,
            ifnull(t.two_level_inactive_user_num, 0) as two_level_inactive_user_num,
            ifnull(th.three_level_inactive_user_num, 0) as three_level_inactive_user_num
        from one_level_inactive_group as o
        left join two_level_inactive_group as t
        on o.country_code = t.country_code
        left join three_level_inactive_group as th
        on o.country_code = th.country_code
    )

    select l.country_code as country_code,
        date('{start_time}') as date,
        one_level_inactive_user_num,
        round(one_level_inactive_user_num/total_user_num, 4) as one_level_ratio,
        two_level_inactive_user_num,
        round(two_level_inactive_user_num/total_user_num, 4) as two_level_ratio,
        three_level_inactive_user_num,
        round(three_level_inactive_user_num/total_user_num, 4) as three_level_ratio,
        total_user_num
    from level_info as l
    right join accounts_group as a
    on l.country_code = a.country_code
    order by country_code