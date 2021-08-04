with
    accounts as (
        select id, (case when country_code in ({country_code}) then country_code else 'OT' end) as country_code
        from input.accounts
        where name is not null
    ),

    comments_info as (
        select account_id, extract(date from created_at) as date
        from stream_events.comments
        where created_at >= '{start_time}'
        and created_at < '{end_time}'
    ),

    comments_group_info as (
        select country_code, date, count(*) as comment_num
        from comments_info
        left join accounts
        on account_id = id
        group by country_code, date
    ),

    comments_people_group_info as (
        select country_code, date, count(distinct account_id) as comment_people_num
        from comments_info
        left join accounts
        on account_id = id
        group by country_code, date
    ),

    sensitive_words_info as (
        select account_id, extract(date from created_at) as date
        from partiko.comments_review_status
        where updated_at >= '{start_time}'
        and updated_at < '{end_time}'
        and review_status = '3'
    ),

    sensitive_words_group_info as (
        select country_code, date, count(*) as sensitive_words_num
        from sensitive_words_info
        left join accounts
        on account_id = id
        group by country_code, date
    ),

    violation_words_info as (
        select account_id, extract(date from created_at) as date
        from partiko.comments_review_status
        where updated_at >= '{start_time}'
        and updated_at < '{end_time}'
        and review_status = '2'
    ),

    violation_words_group_info as (
        select country_code, date, count(*) as violation_words_num
        from violation_words_info
        left join accounts
        on account_id = id
        group by country_code, date
    ),

    one_level_violation_accounts_info as (
        select id, extract(date from comment_level_updated_at) as date
        from partiko.accounts_comments_penalty_info
        where comment_level_updated_at >= '{start_time}'
        and comment_level_updated_at < '{end_time}'
        and comment_penalty_level = 1
    ),

    one_level_violation_accounts_group as (
        select country_code, date, count(*) as one_level_num
        from one_level_violation_accounts_info as o
        left join accounts as a
        on o.id = a.id
        group by country_code, date
    ),

    two_level_violation_accounts_info as (
        select id, extract(date from comment_level_updated_at) as date
        from partiko.accounts_comments_penalty_info
        where comment_level_updated_at >= '{start_time}'
        and comment_level_updated_at < '{end_time}'
        and comment_penalty_level = 2
    ),

    two_level_violation_accounts_group as (
        select country_code, date, count(*) as two_level_num
        from two_level_violation_accounts_info as t
        left join accounts as a
        on t.id = a.id
        group by country_code, date
    ),

    three_level_violation_accounts_info as (
        select id, extract(date from comment_level_updated_at) as date
        from partiko.accounts_comments_penalty_info
        where comment_level_updated_at >= '{start_time}'
        and comment_level_updated_at < '{end_time}'
        and comment_penalty_level = 1
    ),

    three_level_violation_accounts_group as (
        select country_code, date, count(*) as three_level_num
        from three_level_violation_accounts_info as t
        left join accounts as a
        on t.id = a.id
        group by country_code, date
    ),

    four_level_violation_accounts_info as (
        select id, extract(date from comment_level_updated_at) as date
        from partiko.accounts_comments_penalty_info
        where comment_level_updated_at >= '{start_time}'
        and comment_level_updated_at < '{end_time}'
        and comment_penalty_level = 1
    ),

    four_level_violation_accounts_group as (
        select country_code, date, count(*) as four_level_num
        from four_level_violation_accounts_info as f
        left join accounts as a
        on f.id = a.id
        group by country_code, date
    ),

    one_level_violation_accounts_all_info as (
        select id
        from (
            select *, row_number() over (partition by id order by comment_level_updated_at desc) as rank
            from partiko.accounts_comments_penalty_info
            where comment_penalty_level = 1
        )
        where rank = 1
    ),

    one_level_violation_accounts_all_group as (
        select country_code, count(*) as one_level_all_num
        from one_level_violation_accounts_all_info as o
        left join accounts as a
        on o.id = a.id
        group by country_code
    ),

    two_level_violation_accounts_all_info as (
        select id
        from (
            select *, row_number() over (partition by id order by comment_level_updated_at desc) as rank
            from partiko.accounts_comments_penalty_info
            where comment_penalty_level = 2
        )
        where rank = 1
    ),

    two_level_violation_accounts_all_group as (
        select country_code, count(*) as two_level_all_num
        from two_level_violation_accounts_all_info as t
        left join accounts as a
        on t.id = a.id
        group by country_code
    ),

    three_level_violation_accounts_all_info as (
        select id
        from (
            select *, row_number() over (partition by id order by comment_level_updated_at desc) as rank
            from partiko.accounts_comments_penalty_info
            where comment_penalty_level = 3
        )
        where rank = 1
    ),

    three_level_violation_accounts_all_group as (
        select country_code, count(*) as three_level_all_num
        from three_level_violation_accounts_all_info as t
        left join accounts as a
        on t.id = a.id
        group by country_code
    ),

    four_level_violation_accounts_all_info as (
        select id
        from (
            select *, row_number() over (partition by id order by comment_level_updated_at desc) as rank
            from partiko.accounts_comments_penalty_info
            where comment_penalty_level = 4
        )
        where rank = 1
    ),

    four_level_violation_accounts_all_group as (
        select country_code, count(*) as four_level_all_num
        from four_level_violation_accounts_all_info as f
        left join accounts as a
        on f.id = a.id
        group by country_code
    ),

    comments_num_info as (
        select c.country_code as country_code, c.date as date, comment_num, comment_people_num
        from comments_group_info as c
        left join comments_people_group_info as cp
        on c.country_code = cp.country_code
        and c.date = cp.date
    ),

    sensitive_words_num_info as (
        select c.country_code as country_code, c.date as date, comment_num, comment_people_num, ifnull(sensitive_words_num, 0) as sensitive_words_num
        from comments_num_info as c
        left join sensitive_words_group_info as sg
        on c.country_code = sg.country_code
        and c.date = sg.date
    ),

    violation_words_num_info as (
        select s.country_code as country_code, s.date as date, comment_num, comment_people_num, sensitive_words_num, ifnull(violation_words_num, 0) as violation_words_num
        from sensitive_words_num_info as s
        left join violation_words_group_info as v
        on s.country_code = v.country_code
        and s.date = v.date
    ),

    violation_accounts_info as (
        select c.country_code as country_code, c.date as date, comment_num, comment_people_num, sensitive_words_num, violation_words_num, one_level_num, two_level_num, three_level_num, ifnull(four_level_num, 0) as four_level_num
        from (
            select b.country_code as country_code, b.date as date, comment_num, comment_people_num, sensitive_words_num, violation_words_num, one_level_num, two_level_num, ifnull(three_level_num, 0) as three_level_num
            from (
                select a.country_code as country_code, a.date as date, comment_num, comment_people_num, sensitive_words_num, violation_words_num, one_level_num, ifnull(two_level_num, 0) as two_level_num
                from (
                    select v.country_code as country_code, v.date as date, comment_num, comment_people_num, sensitive_words_num, violation_words_num, ifnull(one_level_num, 0) as one_level_num
                    from violation_words_num_info as v
                    left join one_level_violation_accounts_group as o
                    on v.country_code = o.country_code
                    and v.date = o.date
                ) as a
                left join two_level_violation_accounts_group as t
                on a.country_code = t.country_code
                and a.date = t.date
            ) as b
            left join three_level_violation_accounts_group as t
            on b.country_code = t.country_code
            and b.date = t.date
        ) as c
        left join four_level_violation_accounts_group as f
        on c.country_code = f.country_code
        and c.date = f.date
    ),

    all_info as (
        select c.country_code as country_code, c.date as date, comment_num, comment_people_num, sensitive_words_num, violation_words_num, one_level_num, two_level_num, three_level_num, four_level_num, one_level_all_num, two_level_all_num, three_level_all_num, ifnull(four_level_all_num, 0) as four_level_all_num
        from (
            select b.country_code as country_code, b.date as date, comment_num, comment_people_num, sensitive_words_num, violation_words_num, one_level_num, two_level_num, three_level_num, four_level_num, one_level_all_num, two_level_all_num, ifnull(three_level_all_num, 0) as three_level_all_num
            from (
                select a.country_code as country_code, a.date as date, comment_num, comment_people_num, sensitive_words_num, violation_words_num, one_level_num, two_level_num, three_level_num, four_level_num, one_level_all_num, ifnull(two_level_all_num, 0) as two_level_all_num
                from (
                    select v.country_code as country_code, v.date as date, comment_num, comment_people_num, sensitive_words_num, violation_words_num, one_level_num, two_level_num, three_level_num, four_level_num, ifnull(one_level_all_num, 0) as one_level_all_num
                    from violation_accounts_info as v
                    left join one_level_violation_accounts_all_group as o
                    on v.country_code = o.country_code
                ) as a
                left join two_level_violation_accounts_all_group as t
                on a.country_code = t.country_code
            ) as b
            left join three_level_violation_accounts_all_group as t
            on b.country_code = t.country_code
        ) as c
        left join four_level_violation_accounts_all_group as f
        on c.country_code = f.country_code
    )

    select *
    from all_info
    order by country_code, date