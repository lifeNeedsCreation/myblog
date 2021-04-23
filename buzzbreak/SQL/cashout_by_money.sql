with
    accounts as (select id, country_code from input.accounts where name is not null),

    cash_out as (select account_id, payout_method, usd_value as money, extract(date from paid_out_at) as paid_out_date from `partiko.withdraw_transactions` where status = "resolved"  and paid_out_at >= '{start_time}' and paid_out_at < '{end_time}'),

    cash_out_info as (select country_code, cash_out.* from cash_out inner join accounts on account_id = id),
    
   money_info as (select paid_out_date, country_code, money, payout_method, count(*) as cashout_num  from cash_out_info group by paid_out_date, country_code, money, payout_method order by cashout_num desc),
   
   user_info as (select paid_out_date, country_code, money, payout_method, count(distinct account_id) as user_num from cash_out_info group by paid_out_date, country_code, money, payout_method)
   
   select u.country_code as country_code, u.paid_out_date as paid_out_date, u.money as money, u.payout_method as payout_method, cashout_num, user_num from user_info as u inner join money_info as m on u.paid_out_date = m.paid_out_date and u.country_code = m.country_code and u.money = m.money and u.payout_method = m.payout_method order by paid_out_date desc, money desc