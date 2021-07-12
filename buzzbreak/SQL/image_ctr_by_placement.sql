with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    image_click_info as (select country_code, account_id, image_id, placement, date from (select distinct account_id, json_extract_scalar(data, '$.id') as image_id, json_extract_scalar(data, '$.placement') as placement, extract(date from created_at) as date from stream_events.image_click where created_at >= '{start_time}' and created_at < '{end_time}') inner join accounts on account_id = id),

    image_impression_info as (select country_code, account_id, image_id, placement, date from (select distinct account_id, json_extract_scalar(data, '$.id') as image_id, json_extract_scalar(data, '$.placement') as placement, extract(date from created_at) as date from stream_events.image_impression where created_at >= '{start_time}' and created_at < '{end_time}') inner join accounts on account_id = id),

    image_click_group as (select country_code, date, placement, count(*) as click_count from image_click_info group by country_code, date, placement),

    image_impression_group as (select country_code, date, placement, count(*) as impression_count from image_impression_info group by country_code, date, placement)

    select i.country_code as country_code, i.date as date, i.placement as placement, ifnull(click_count, 0) as click_count, impression_count, round(ifnull(click_count, 0)/impression_count, 4) as ctr from image_impression_group as i left join image_click_group as c on i.country_code = c.country_code and i.date = c.date and i.placement = c.placement order by country_code, date, placement, ctr desc