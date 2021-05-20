with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    notification_click as (select *, extract(date from created_at) as date from stream_events.notification_click where created_at >= '{start_time}' and created_at < '{end_time}'),

    accounts_click as (select country_code, n.* from notification_click as n inner join accounts on account_id = id), 

    notification_received as (select *, extract(date from created_at) as date from stream_events.notification_received where  created_at >= '{start_time}' and created_at < '{end_time}'),

    accounts_received as (select country_code, n.* from notification_received as n inner join accounts on account_id = id), 

    click_group as (select country_code, date, count(*) as click_num from accounts_click group by country_code, date),

    received_group as (select country_code, date, count(*) as received_num from accounts_received group by country_code, date)

    select r.country_code as country_code, r.date as date, ifnull(click_num, 0) as click_num, received_num, round(ifnull(click_num, 0)/received_num, 4) as ctr from received_group as r left join click_group as c on r.country_code = c.country_code and r.date = c.date order by country_code, date