with
    accounts as (select id from `input.accounts` where name is not null and country_code in ({country_code})),
    
    posts as (select account_id, country_code, type, extract(date from created_at) as date from `partiko.posts` where country_code is not null and created_at > '{start_time}' and created_at < '{end_time}' and type is not null)
    
    select country_code, type, date, count(*) as posts_num from posts inner join accounts on account_id = id group by country_code, type, date order by country_code, type, date