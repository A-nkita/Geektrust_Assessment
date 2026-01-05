import psycopg2
import logging
import os
from datetime import datetime

# ---------- LOGGING ----------
LOG_DIR = r"C:\Users\Admin\OneDrive\Desktop\Geektrust\scripts\logs"
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(
    LOG_DIR,
    f"intermediate_transform_{datetime.now().strftime('%Y%m%d')}.log"
)

logger = logging.getLogger("intermediate_job")
logger.setLevel(logging.INFO)
logger.propagate = False

if not logger.handlers:
    file_handler = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

logger.info("Intermediate transformation started")

# ---------- DB + SQL ----------
try:
    logger.info("Connecting to database")

    conn = psycopg2.connect(
        host="localhost",
        dbname="geektrust_db",
        user="postgres",
        password="root",
        port=5432
    )
    conn.autocommit = True
    cur = conn.cursor()

    logger.info("Database connection established")

    sql = """
    -- =====================================================
    -- USERS CLEAN (ONE ROW PER USER)
    -- =====================================================
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

    ALTER TABLE intermediate.users_clean
    ADD PRIMARY KEY (user_id);


    -- =====================================================
    -- USER ACTIVITY (USER-LEVEL AGGREGATES)
    -- =====================================================
    DROP TABLE IF EXISTS intermediate.user_activity;

    CREATE TABLE intermediate.user_activity AS
    SELECT
        t.user_id,
        COUNT(t.transaction_id) AS transaction_count,
        SUM(t.amount) AS total_transaction_amount,
        MIN(t.transaction_date::date) AS first_transaction_date,
        MAX(t.transaction_date::date) AS last_transaction_date
    FROM staging.transactions t
    GROUP BY t.user_id;

    ALTER TABLE intermediate.user_activity
    ADD PRIMARY KEY (user_id);


    -- =====================================================
    -- TRANSACTIONS CLEAN (DEDUPED, TRANSACTION-LEVEL)
    -- =====================================================
    DROP TABLE IF EXISTS intermediate.transactions_clean;

    CREATE TABLE intermediate.transactions_clean AS
    WITH ranked_txns AS (
        SELECT
            t.transaction_id,
            t.user_id,
            t.amount,
            t.transaction_date::timestamp AS transaction_created_at,
            ROW_NUMBER() OVER (
                PARTITION BY t.transaction_id
                ORDER BY t.transaction_date DESC
            ) AS rn
        FROM staging.transactions t
    )
    SELECT
        transaction_id,
        user_id,
        amount,
        transaction_created_at
    FROM ranked_txns
    WHERE rn = 1;

    ALTER TABLE intermediate.transactions_clean
    ADD PRIMARY KEY (transaction_id);
    """

    logger.info("Executing transformation SQL")
    cur.execute(sql)

    logger.info("Intermediate tables created successfully")

    cur.close()
    conn.close()
    logger.info("Database connection closed")

except Exception:
    logger.exception("Intermediate transformation failed")
    raise

logger.info("Intermediate transformation completed successfully")
