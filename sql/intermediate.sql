--create schema;
CREATE SCHEMA IF NOT EXISTS intermediate;

--create clean table;
--One row per user
--Latest record if duplicates
--Clean datatypes
--Join users + onboarding

-- users clean
DROP TABLE IF EXISTS intermediate.users_clean;

CREATE TABLE intermediate.users_clean AS
WITH ranked_users AS (
    SELECT
        u.user_id,
        u.name,
        u.email,
        u.phone,
        u.created_at::timestamp AS user_created_at,
        o.role,
        NULL::timestamp AS onboarding_created_at,
        ROW_NUMBER() OVER (
            PARTITION BY u.user_id
            ORDER BY u.created_at DESC
        ) AS rn
    FROM staging.users u
    LEFT JOIN staging.onboarding o
        ON u.user_id = o.user_id
)
SELECT
    user_id,
    name,
    email,
    phone,
    user_created_at,
    role,
    onboarding_created_at
FROM ranked_users
WHERE rn = 1;

--Add primary key
ALTER TABLE intermediate.users_clean
ADD PRIMARY KEY (user_id);

--Create user_activity
--One row per user
--Transaction metrics

CREATE TABLE intermediate.user_activity AS
SELECT
    t.user_id,
    COUNT(t.transaction_id) AS transaction_count,
    SUM(t.amount) AS total_transaction_amount,
    MIN(t.transaction_date::date) AS first_transaction_date,
    MAX(t.transaction_date::date) AS last_transaction_date
FROM staging.transactions t
GROUP BY t.user_id;

--Add primary key
ALTER TABLE intermediate.user_activity
ADD PRIMARY KEY (user_id);

--Create transactions_clean
DROP TABLE IF EXISTS intermediate.transactions_clean;

CREATE TABLE intermediate.transactions_clean AS
SELECT
    t.transaction_id,
    t.user_id,
    t.amount,
    t.transaction_date::timestamp AS transaction_created_at
FROM staging.transactions t;

--Add primary key
ALTER TABLE intermediate.transactions_clean
ADD PRIMARY KEY (transaction_id);


--Data quality Checks
--1. Duplicates
SELECT user_id, COUNT(*)
FROM intermediate.users_clean
GROUP BY user_id
HAVING COUNT(*) > 1;

--Null critical fields
SELECT *
FROM intermediate.users_clean
WHERE user_id IS NULL;

--Count
SELECT COUNT(*) FROM intermediate.users_clean;
SELECT COUNT(*) FROM intermediate.user_activity;
SELECT COUNT(*) FROM intermediate.transactions_clean;


