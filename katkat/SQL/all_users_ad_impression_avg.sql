with
    accounts as (select id, country_code from input.accounts where name is not null),

    ad_impression as (select account_id, json_extract_scalar(data, "$.placement")as placement, extract(date from created_at) as created_date from stream_events.ad_impression where created_at >= "{start_time}" and created_at < "{end_time}" and json_extract_scalar(data, "$.placement") in ({placement})),

    accounts_ad_impression as (select id, country_code, placement, created_date from accounts as a inner join ad_impression as ad on a.id = ad.account_id),

    ad_impression_count as (select country_code, created_date, count(*) as ad_impression_num from accounts_ad_impression group by country_code, created_date),

    user_count as (select country_code, created_date, count(distinct id) as user_num from accounts_ad_impression group by country_code, created_date)

    select a.country_code as country_code, "total" as placement, a.created_date as date, ad_impression_num, user_num, round(ad_impression_num / user_num, 4) as ad_impression_avg  from ad_impression_count as a inner join user_count as u on a.country_code = u.country_code and a.created_date = u.created_date