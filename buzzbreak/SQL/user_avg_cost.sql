with
    accounts as (select id, country_code from input.accounts where name is not null),

    cashout_info as (select account_id, sum(usd_value) as cashout from partiko.withdraw_transactions where status = 'resolved' group by account_id),

    account_cashout_info as (select id, country_code, ifnull(cashout, 0) as cashout from accounts left join cashout_info on id = account_id),

    cashout_group as (select country_code, sum(cashout) as paid_money from account_cashout_info group by country_code),

    account_group as (select country_code, count(distinct id) as user_num from account_cashout_info group by country_code)

    select a.country_code as country_code, extract(date from timestamp'{start_time}') as date,  paid_money, user_num, round(paid_money / user_num, 4) as avg_cost from cashout_group as c inner join account_group as a on c.country_code = a.country_code order by user_num desc