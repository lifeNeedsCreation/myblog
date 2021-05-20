with
    accounts as (select id, country_code from input.accounts where name is not null and country_code in ({country_code})),

    point_info_in as (select *, extract(date from created_at) as date from partiko.point_transactions where created_at >= '{start_time}' and created_at < '{end_time}' and amount < 0),

    accounts_point_in_info as (select country_code, p.* from point_info_in as p inner join accounts on account_id = id),

    points_in_group as (select country_code, date, type, sum(amount) as points from accounts_point_in_info group by country_code, date, type),

    points_total_in_group as (select country_code, date, sum(amount) as total_points from accounts_point_in_info group by country_code, date)

    select i.country_code as country_code, i.date as date, type, points, total_points, round(points/total_points, 4) as ratio from points_in_group as i inner join points_total_in_group as t on i.country_code = t.country_code and i.date = t.date order by country_code, date, type