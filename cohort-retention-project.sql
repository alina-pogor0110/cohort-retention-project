WITH users_prepared AS (
    SELECT
        user_id,
        promo_signup_flag,
        TO_TIMESTAMP(
            regexp_replace(
                regexp_replace(signup_datetime, '[./]', '-', 'g'),
                '^(\d{1,2})-(\d{1,2})-(\d{2}) ',
                '\1-\2-20\3 '
            ),
            'DD-MM-YYYY HH24:MI'
        ) AS signup_ts
    FROM cohort_users_raw
),

events_prepared AS (
    SELECT
        user_id,
        event_type,
        TO_TIMESTAMP(
            regexp_replace(
                regexp_replace(event_datetime, '[./]', '-', 'g'),
                '^(\d{1,2})-(\d{1,2})-(\d{2}) ',
                '\1-\2-20\3 '
            ),
            'DD-MM-YYYY HH24:MI'
        ) AS event_ts
    FROM cohort_events_raw
),

joined_data AS (
    SELECT
        u.user_id,
        u.promo_signup_flag,
        u.signup_ts,
        e.event_type,
        e.event_ts
    FROM users_prepared u
    LEFT JOIN events_prepared e
        ON u.user_id = e.user_id
    WHERE u.signup_ts IS NOT NULL
      AND (e.event_type IS NULL OR e.event_type <> 'test_event')
),

with_months AS (
    SELECT
        user_id,
        promo_signup_flag,
        date_trunc('month', signup_ts)::date AS cohort_month,
        date_trunc(
            'month',
            COALESCE(event_ts, signup_ts)
        )::date AS activity_month
    FROM joined_data
),

with_offsets AS (
    SELECT
        user_id,
        promo_signup_flag,
        cohort_month,
        activity_month,
        EXTRACT(
            MONTH FROM age(activity_month, cohort_month)
        ) AS month_offset
    FROM with_months
)

SELECT
    promo_signup_flag,
    cohort_month,
    month_offset,
    COUNT(DISTINCT user_id) AS users_total
FROM with_offsets
WHERE activity_month BETWEEN DATE '2025-01-01' AND DATE '2025-06-01'
GROUP BY
    promo_signup_flag,
    cohort_month,
    month_offset
ORDER BY
    promo_signup_flag,
    cohort_month,
    month_offset;
