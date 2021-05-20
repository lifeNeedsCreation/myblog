with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    point_info_out as (select *, extract(date from created_at) as date from partiko.point_transactions where created_at >= '{start_time}' and created_at < '{end_time}' and amount > 0),

    accounts_point_out_info as (select country_code, p.* from point_info_out as p inner join accounts on account_id = id),

    points_out_group as (select country_code, date, type, sum(amount) as points from accounts_point_out_info group by country_code, date, type),

    points_total_out_group as (select country_code, date, sum(amount) as total_points from accounts_point_out_info group by country_code, date)
    
    select o.country_code as country_code, o.date as date, type, points, total_points, round(points/total_points, 4) as ratio from points_out_group as o inner join points_total_out_group as t on o.country_code = t.country_code and o.date = t.date order by country_code, date, type