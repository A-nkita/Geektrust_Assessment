-- Create different schema for different tables;
CREATE SCHEMA source_users;
CREATE SCHEMA source_onboarding;
CREATE SCHEMA source_transactions;

-- Create three tables with respect to the schemas;
-- users table;
CREATE TABLE source_users.users (
  user_id INT,
  name TEXT,
  email TEXT,
  phone TEXT,
  created_at TIMESTAMP
);

--onboarding table;
CREATE TABLE source_onboarding.onboarding (
  onboarding_id INT,
  user_id INT,
  role TEXT,
  status TEXT,
  completed_at TIMESTAMP
);

--transactions table;
CREATE TABLE source_transactions.transactions (
  transaction_id INT,
  user_id INT,
  amount NUMERIC(10,2),
  currency TEXT,
  transaction_type TEXT,
  created_at TIMESTAMP
);

--Add data to tables;
