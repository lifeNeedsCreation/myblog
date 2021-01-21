f"""
with
    accounts as (select * from input.accounts where name is not null and created_at >= "{start_time}" and created_at < "{end_time}"),

    user_time as (select * from stream_events.user_time where created_at >= "{start_time}" and created_at < "{end_time}" and json_extract_scalar(data, "$.page") in ({self.channel})),

    account_user_time as (select id, country_code, json_extract_scalar(data, "$.page") as channel, safe_cast(json_extract_scalar(data, "$.duration_in_seconds") as numeric) as duration_in_seconds, user_time.created_at from accounts inner join user_time on id = account_id),

    account_count as (select country_code, channel, extract(date from created_at) as date, count(distinct id) as channel_users_count from account_user_time group by country_code, channel, date),

    duration_in_seconds_sum as (select country_code, channel, extract(date from created_at) as date, sum(duration_in_seconds) as channel_duration_sum from account_user_time group by country_code, channel, date)

    select a.country_code, a.channel, a.date, channel_users_count, channel_duration_sum, round(ifnull(channel_duration_sum, 0) / channel_users_count, 4) as channel_duration_avg from account_count as a inner join duration_in_seconds_sum as d on a.country_code = d.country_code and a.channel = d.channel and a.date = d.date
"""