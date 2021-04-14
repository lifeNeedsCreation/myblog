with
    accounts as (select id, country_code from input.accounts where name is not null),

    pay_out_log as (select account_id, extract(date from ifnull(paid_out_at, created_at)) as paid_out_date, payout_method, usd_value from partiko.withdraw_transactions where status = "resolved")

    select country_code, paid_out_date, payout_method, sum(usd_value) as money from pay_out_log inner join accounts on account_id = id where paid_out_date >= "{start_time}" and paid_out_date < "{end_time}"  group by country_code, paid_out_date, payout_method order by country_code, paid_out_date