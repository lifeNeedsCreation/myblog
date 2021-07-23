with
    a as (select extract(date from creation_time) as date, job_id, user_email, query, ifnull(round(total_bytes_processed/(1024*1024*1024), 2), 0) AS GB, ifnull(round(total_bytes_processed*5/(1024*1024*1024*1000), 2), 0) AS cost from `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT where creation_time >= '{start_time}' and creation_time < '{end_time}')

    select date, user_email, round(sum(GB), 2) as traffic, round(sum(cost), 2) as total_cost from a group by date, user_email order by date, total_cost desc