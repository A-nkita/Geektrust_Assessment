-- =========================
-- ANALYTICS SCHEMA
-- =========================
CREATE SCHEMA IF NOT EXISTS analytics;

-- =========================================================
-- 1. DAILY KYC COMPLETIONS + WEEK-OVER-WEEK GROWTH
-- =========================================================
DROP TABLE IF EXISTS analytics.daily_kyc_metrics;

CREATE TABLE analytics.daily_kyc_metrics AS
WITH daily_kyc AS (
    SELECT
        o.completed_at::date AS kyc_date,
        COUNT(DISTINCT o.user_id) AS daily_kyc_completed
    FROM staging.onboarding o
    WHERE o.status = 'completed'
      AND o.completed_at IS NOT NULL
    GROUP BY o.completed_at::date
),
weekly AS (
    SELECT
        kyc_date,
        daily_kyc_completed,
        SUM(daily_kyc_completed) OVER (
            PARTITION BY DATE_TRUNC('week', kyc_date)
        ) AS weekly_kyc_completed
    FROM daily_kyc
),
wow AS (
    SELECT
        *,
        LAG(weekly_kyc_completed) OVER (
            ORDER BY DATE_TRUNC('week', kyc_date)
        ) AS prev_week_kyc
    FROM weekly
)
SELECT
    kyc_date,
    daily_kyc_completed,
    weekly_kyc_completed,
    CASE
        WHEN prev_week_kyc IS NULL THEN NULL
        ELSE ROUND(
            (weekly_kyc_completed - prev_week_kyc) * 100.0
            / prev_week_kyc, 2
        )
    END AS week_over_week_growth_pct
FROM wow;

-- =========================================================
-- 2. TOP 5 USERS PER DAY BY TRANSACTION AMOUNT
--    + CONTRIBUTION %
-- =========================================================
DROP TABLE IF EXISTS analytics.daily_top_users;

CREATE TABLE analytics.daily_top_users AS
WITH daily_user_txn AS (
    SELECT
        t.transaction_created_at::date AS txn_date,
        t.user_id,
        SUM(t.amount) AS user_daily_amount
    FROM intermediate.transactions_clean t
    GROUP BY t.transaction_created_at::date, t.user_id
),
ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY txn_date
            ORDER BY user_daily_amount DESC
        ) AS rnk,
        SUM(user_daily_amount) OVER (
            PARTITION BY txn_date
        ) AS total_daily_amount
    FROM daily_user_txn
)
SELECT
    txn_date,
    user_id,
    user_daily_amount,
    ROUND(
        user_daily_amount * 100.0 / total_daily_amount, 2
    ) AS contribution_pct
FROM ranked
WHERE rnk <= 5;

-- =========================================================
-- 3. TRANSACTION STATISTICS
-- =========================================================
DROP TABLE IF EXISTS analytics.transaction_stats;

CREATE TABLE analytics.transaction_stats AS
SELECT
    COUNT(*) AS total_transactions,
    AVG(amount) AS avg_transaction_amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_transaction_amount,
    MIN(amount) AS min_transaction_amount,
    MAX(amount) AS max_transaction_amount
FROM intermediate.transactions_clean;

-- =========================================================
-- 4. KYC COMPLETED vs PENDING
--    MERCHANT TRANSACTION BEHAVIOR
-- =========================================================
DROP TABLE IF EXISTS analytics.kyc_merchant_comparison;

CREATE TABLE analytics.kyc_merchant_comparison AS
WITH merchant_users AS (
    SELECT
        u.user_id,
        u.role,
        o.status AS kyc_status
    FROM intermediate.users_clean u
    LEFT JOIN staging.onboarding o
        ON u.user_id = o.user_id
    WHERE u.role = 'merchant'
),
merchant_txns AS (
    SELECT
        m.kyc_status,
        COUNT(t.transaction_id) AS transaction_count,
        SUM(t.amount) AS total_transaction_amount,
        AVG(t.amount) AS avg_transaction_amount
    FROM merchant_users m
    LEFT JOIN intermediate.transactions_clean t
        ON m.user_id = t.user_id
    GROUP BY m.kyc_status
)
SELECT *
FROM merchant_txns;
