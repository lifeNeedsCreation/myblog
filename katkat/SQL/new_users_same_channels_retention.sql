with
    accounts as (select * from input.accounts where name is not null),

    account_profiles as (select * from partiko.account_profiles where mac_address is not null),

    account as (select id, country_code, created_date from (select distinct id,country_code, extract(date from created_at) as created_date from accounts) inner join (select distinct account_id from (select mac_address, min(created_at) as created_at from account_profiles group by mac_address) as a inner join (select account_id, mac_address, created_at from account_profiles) as b on a.mac_address = b.mac_address and a.created_at = b.created_at) on id = account_id),

    watch_channels as (select * from stream_events.video_impression where created_at > timestamp_sub(timestamp"{start_time}", interval 30 day) and created_at < "{end_time}" and json_extract_scalar(data, "$.placement") in ({channel})),

    channel_target_time as (select distinct account_id, extract(date from created_at) as date, json_extract_scalar(data, "$.placement") as channel from watch_channels where created_at >= "{start_time}" and created_at < "{end_time}"),

    channel_one_month as (select distinct account_id, extract(date from created_at) as date, json_extract_scalar(data, "$.placement") as channel from watch_channels),

    initial_channels as (select distinct id, country_code, date as initial_date, channel as initial_channel from account inner join channel_one_month on id = account_id where created_date = date),

    retention_channels as (select distinct id, country_code, initial_date, date as retention_date, initial_channels.initial_channel as initial_channel, channel_target_time.channel as retention_channel, date_diff(date, initial_date, day) as date_diff from initial_channels inner join channel_target_time on id = account_id and initial_channel = channel),

    initial_channels_count as (select count(distinct id) as initial_users, country_code, initial_date, initial_channel from initial_channels group by country_code, initial_date, initial_channel),

    retention_channels_count as (select count(distinct id) as retention_users, country_code, initial_date, retention_date, initial_channel, retention_channel, date_diff from retention_channels group by country_code, initial_date, retention_date, initial_channel, retention_channel, date_diff)

    select i.country_code, i.initial_date, retention_date, i.initial_channel, date_diff, initial_users, ifnull(retention_users, 0) as retention_users, round(ifnull(retention_users, 0) / initial_users, 4) as retention_rate from initial_channels_count as i left join retention_channels_count as r on i.country_code = r.country_code and i.initial_date = r.initial_date and i.initial_channel = r.initial_channel where date_diff is not null
