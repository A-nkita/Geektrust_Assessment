--create schema
CREATE SCHEMA staging;
CREATE SCHEMA core;
CREATE SCHEMA analytics;

--Create tables for ingestion in staging area same as raw files in staging area.
CREATE TABLE staging.users (
    user_id INT,
    name TEXT,
    email TEXT,
    phone TEXT,
    created_at TIMESTAMP
);
CREATE TABLE staging.onboarding (
    onboarding_id INT,
    user_id INT,
    role TEXT,
    status TEXT,
    completed_at TIMESTAMP
);
CREATE TABLE staging.transactions (
    transaction_id INT,
    user_id INT,
    amount NUMERIC(10,2),
    transaction_date DATE
);

--Confirm data actually loaded
SELECT COUNT(*) FROM staging.users;
SELECT COUNT(*) FROM staging.onboarding;
SELECT COUNT(*) FROM staging.transactions;
