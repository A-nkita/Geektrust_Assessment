--Create database
CREATE DATABASE geektrust_db;

--Create schemas (source layer)
CREATE SCHEMA source_users;
CREATE SCHEMA source_onboarding;
CREATE SCHEMA source_transactions;

--Create tables (match CSV exactly)
CREATE TABLE source_users.users (
    user_id INT,
    name TEXT,
    email TEXT,
    phone TEXT,
    created_at TIMESTAMP
);
CREATE TABLE source_onboarding.onboarding (
    onboarding_id INT,
    user_id INT,
    role TEXT,
    status TEXT,
    completed_at TIMESTAMP
);
CREATE TABLE source_transactions.transactions (
    transaction_id INT,
    user_id INT,
    amount NUMERIC(10,2),
    transaction_date DATE
);

--Load CSV data (Query Tool → COPY)
--user table
COPY source_users.users
FROM 'C:/Users/Admin/OneDrive/Desktop/Geektrust/Data/users.csv'
DELIMITER ','
CSV HEADER;

--onboarding table
COPY source_onboarding.onboarding
FROM 'C:/Users/Admin/OneDrive/Desktop/Geektrust/Data/onboarding.csv'
DELIMITER ','
CSV HEADER;

--transactions table
COPY source_transactions.transactions
FROM 'C:/Users/Admin/OneDrive/Desktop/Geektrust/Data/transactions.csv'
DELIMITER ','
CSV HEADER;

--Verify (always)
SELECT COUNT(*) FROM source_users.users;
SELECT COUNT(*) FROM source_onboarding.onboarding;
SELECT COUNT(*) FROM source_transactions.transactions;
