import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# -------------------------
# Database Connection
# -------------------------
engine = create_engine(
    "mysql+pymysql://root:Devgan#2003@localhost:3306/risk_pipline"
)
# -------------------------
# Load Sample Market Data
# -------------------------
def load_market_data():
    dates = pd.date_range(start="2020-01-01", end="2024-12-31", freq="B")
    prices = 100 + np.cumsum(np.random.normal(0, 1, len(dates)))

    df = pd.DataFrame({
        "price_date": dates,
        "close_price": prices
    })
    return df

# -------------------------
# CHECK DATA QUALITY
# -------------------------
def check_nulls(df: pd.DataFrame):
    if df.isnull().any().any():
        raise ValueError("❌ Null values detected")

def check_negative_prices(df: pd.DataFrame):
    if (df["close_price"] <= 0).any():
        raise ValueError("❌ Invalid price values detected")

def check_duplicates(df: pd.DataFrame):
    if df.duplicated(subset=["price_date"]).any():
        raise ValueError("❌ Duplicate dates detected")

def run_all_checks(df: pd.DataFrame):
    check_nulls(df)
    check_negative_prices(df)
    check_duplicates(df)
# -------------------------
# CALCULATE RISK METRICS
# -------------------------
def calculate_risk_metrics(df):
    df = df.sort_values("price_date")

    df["daily_return"] = df["close_price"].pct_change()
    df["rolling_volatility"] = df["daily_return"].rolling(20).std()
    df["var_95"] = df["daily_return"].rolling(20).quantile(0.05)

    cumulative_returns = (1 + df["daily_return"]).cumprod()
    rolling_max = cumulative_returns.cummax()
    df["max_drawdown"] = (cumulative_returns - rolling_max) / rolling_max

    return df.dropna()

# -------------------------
# LOAD INTO DATABASE
# -------------------------
def load_to_database(df):
    with engine.begin() as conn:

        # 🔹 Insert asset safely
        conn.execute(
            text("""
                INSERT INTO assets (ticker, asset_name, asset_type)
                VALUES (:ticker, :name, :type)
                ON DUPLICATE KEY UPDATE ticker = ticker
            """),
            {"ticker": "AAPL", "name": "Apple Inc.", "type": "Equity"}
        )

        # 🔹 Fetch asset_id (FIXED)
        asset_id = conn.execute(
            text("SELECT asset_id FROM assets WHERE ticker = :ticker"),
            {"ticker": "AAPL"}
        ).scalar_one()
        # 2️⃣ DELETE existing data (IDEMPOTENCY)
        conn.execute(
            text("DELETE FROM daily_prices WHERE asset_id = :asset_id"),
            {"asset_id": asset_id}
        )

        conn.execute(
            text("DELETE FROM risk_metrics WHERE asset_id = :asset_id"),
            {"asset_id": asset_id}
        )

        # 🔹 Insert prices
        price_df = df[["price_date", "close_price"]].copy()
        price_df["asset_id"] = asset_id
        price_df.to_sql("daily_prices", conn, if_exists="append", index=False)

        # 4️⃣ Insert risk metrics
        metrics_df = df[[
            "price_date",
            "daily_return",
            "rolling_volatility",
            "var_95",
            "max_drawdown"
        ]].copy()

        metrics_df["asset_id"] = asset_id
        metrics_df.rename(columns={"price_date": "metric_date"}, inplace=True)

        metrics_df.to_sql(
            "risk_metrics",
            conn,
            if_exists="append",
            index=False,
            method="multi"
        )

# -------------------------
# RUN PIPELINE
# -------------------------
if __name__ == "__main__":
    raw = load_market_data()
    metrics = calculate_risk_metrics(raw)
    load_to_database(metrics)
    print("✅ Risk pipeline executed successfully")