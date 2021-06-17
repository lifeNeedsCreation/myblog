with
    a as (select extract(date from creation_time) as date, job_id, user_email, query, round(total_bytes_processed/(1024*1024*1024), 2) AS GB, round(total_bytes_processed*5/(1024*1024*1024*1000), 2) AS cost from `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT where creation_time >= '2021-06-17' and creation_time < '2021-06-18')

    select date, user_email, round(sum(GB), 2) as traffic, round(sum(cost), 2) as total_cost from a group by date, user_email order by date, total_cost desc