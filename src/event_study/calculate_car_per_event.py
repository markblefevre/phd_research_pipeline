import pandas as pd
import numpy as np


def calculate_car_per_event(
    stock_df,
    market_df,
    event_df,           # ← now includes alpha/beta
    event_window=(-1, 1),
):
    """
    Correct CAR computation using event-specific alpha/beta.

    event_df must contain:
        Ticker, EventDate, alpha, beta
    """

    results = []

    # --- Ensure sorted trading calendar ---
    trading_days = (
        stock_df["Date"]
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )

    for _, row in event_df.iterrows():
        ticker = row["Ticker"]
        event_date = row["EventDate"]
        alpha = row["alpha"]
        beta = row["beta"]

        # --- Align event date to trading day (t=0) ---
        idx = trading_days.searchsorted(event_date)

        if idx >= len(trading_days):
            continue

        # --- Build event window ---
        start_idx = idx + event_window[0]
        end_idx = idx + event_window[1]

        if start_idx < 0 or end_idx >= len(trading_days):
            continue

        window_days = trading_days.iloc[start_idx : end_idx + 1]

        # --- Extract stock + market ---
        stock_sub = stock_df[
            (stock_df["Ticker"] == ticker)
            & (stock_df["Date"].isin(window_days))
        ]

        if stock_sub.empty:
            continue

        merged = stock_sub.merge(market_df, on="Date", how="inner")

        if merged.empty:
            continue

        # --- Compute abnormal returns ---
        merged["AR"] = merged["Return"] - (
            alpha + beta * merged["MarketReturn"]
        )

        car = merged["AR"].sum()

        results.append({
            "Ticker": ticker,
            "EventDate": event_date,
            "CAR": car
        })

    return pd.DataFrame(results)