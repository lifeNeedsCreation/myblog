with
    accounts as (select id, country_code, extract(date from created_at) as date from input.accounts where name is not null and country_code in ({country_code})),

    account as (select accounts.* from (select distinct account_id from (select mac_address, min(updated_at) as updated_at from partiko.account_profiles group by mac_address) as a inner join (select account_id, mac_address, updated_at from partiko.account_profiles) as b on a.mac_address = b.mac_address and a.updated_at = b.updated_at) inner join accounts on account_id = id),

    account_memories as (select country_code, account_id, key, value, date, updated_at from account as a inner join partiko.memories as m on id = account_id and date <= extract(date from updated_at) and key = 'experiment_treatment_group_new_invite' and value in ('control', 'treatment') and updated_at >= '2021-05-22'),

    referrals as (select referrer_account_id, referee_account_id, created_at from partiko.referrals),

    account_memories_referrals as (select country_code, account_id, key, value, extract(date from updated_at) as date from account_memories inner join referrals on account_id = referrer_account_id and updated_at < created_at)

    select country_code, date, key as treatment_name, value, count(distinct account_id) as user_num from account_memories_referrals where date >= '{start_time}' and date < '{end_time}' group by country_code, date, key, value order by country_code, date, key, value