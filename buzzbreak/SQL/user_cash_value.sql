with
    accounts as (
        select id, country_code
        from input.accounts
        where name is not null
    ),

    ads_info as (
        select country_code, unit_id, source, val, date(date_string) as date
        from partiko.ads
        where date_string = '{start_time}'
    ),

    ad_configs_history_info as (
        select distinct unit_id, platform, placement
        from partiko.ad_configs_history
    ),

    ad_placement_info as (
        select country_code, a.unit_id as unit_id, source, placement, val, date
        from ads_info as a
        inner join ad_configs_history_info as c
        on a.unit_id = c.unit_id
        and source = platform
    ),

    ad_click as (
        select country_code, account_id, placement, date
        from (
            select account_id, json_extract_scalar(data, '$.placement') as placement, extract(date from created_at) as date
            from stream_events.ad_click
            where created_at >= '{start_time}'
            and created_at < '{end_time}'
        )
        inner join accounts
        on account_id = id
    ),

    ad_click_group_by_user as (
        select country_code, date, placement, account_id, count(*) as click_count
        from ad_click
        group by country_code, date, placement, account_id
    ),

    ad_click_group_by_placement as (
        select country_code, date, placement, count(*) as total_click_count
        from ad_click
        group by country_code, date, placement
    ),

    ad_click_info as (
        select u.country_code as country_code, u.date as date, u.placement as placement, account_id, click_count, total_click_count, (click_count/total_click_count) as ratio
        from ad_click_group_by_user as u
        inner join ad_click_group_by_placement as p
        on u.country_code = p.country_code
        and u.date = p.date 
        and u.placement = p.placement
    )
  
    select country_code, date, placement, account_id, click_count, total_click_count, ratio, sum(ad_value) as ad_value, sum(val) as val
    from (
      select c.*, (val*ratio) as ad_value, val
      from ad_click_info as c
      inner join ad_placement_info as p
      on c.country_code = p.country_code
      and c.placement = p.placement
      and c.date = p.date
    )
    group by country_code, date, placement, account_id, click_count, total_click_count, ratio
    order by country_code, date, placement, ratio desc