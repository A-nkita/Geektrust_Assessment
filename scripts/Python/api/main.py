from fastapi import FastAPI
from db import engine
import pandas as pd

app = FastAPI(title="Geektrust Analytics API")


# ================================
# DAILY KYC METRICS
# ================================
@app.get("/kyc/daily")
def daily_kyc():
    query = """
        SELECT
            date,
            daily_kyc_completed,
            growth_from_yesterday
        FROM analytics.daily_kyc_metrics
        ORDER BY date;
    """
    return pd.read_sql(query, engine).to_dict(orient="records")


# ================================
# TOP 5 USERS PER DAY
# ================================
@app.get("/transactions/top-users")
def top_users():
    query = """
        SELECT
            date,
            user_id,
            total_amount,
            contribution_pct
        FROM analytics.daily_top_users
        ORDER BY date, total_amount DESC;
    """
    return pd.read_sql(query, engine).to_dict(orient="records")


# ================================
# TRANSACTION STATISTICS
# ================================
@app.get("/transactions/stats")
def transaction_stats():
    query = """
        SELECT
            date,
            avg_amount,
            median_amount,
            min_amount,
            max_amount
        FROM analytics.transaction_stats
        ORDER BY date;
    """
    return pd.read_sql(query, engine).to_dict(orient="records")


# ================================
# MERCHANT KYC COMPARISON
# ================================
@app.get("/merchants/kyc-comparison")
def merchant_kyc_comparison():
    query = """
        SELECT
            kyc_status,
            txn_count,
            total_amount,
            avg_amount
        FROM analytics.kyc_vs_pending_merchants;
    """
    return pd.read_sql(query, engine).to_dict(orient="records")
