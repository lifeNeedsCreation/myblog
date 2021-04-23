with
    accounts as (select id, country_code from input.accounts where name is not null),

    cash_out as (select account_id, payout_method, usd_value as money, extract(date from created_at) as created_date, extract(date from paid_out_at) as paid_out_date from `partiko.withdraw_transactions` where created_at >= '{start_time}' and created_at < '{end_time}'),    
    
    info as (select account_id, country_code, created_date, payout_method, money, paid_out_date from cash_out inner join accounts on account_id = id)
    
    select country_code, payout_method, created_date as paid_out_date, sum(money) as money from info group by country_code, payout_method, created_date order by country_code, created_date desc