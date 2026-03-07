#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct  3 22:53:34 2025

@author: mlefevre
"""
import pandas as pd
import numpy as np
import statsmodels.api as sm


def calculate_car_per_event(stock_df, market_df, alphas, betas, event_dates, event_window=(-1, 1)):
    """
    Calculate CAR (Cumulative Abnormal Returns) per event (Ticker × EventDate).

    Parameters
    ----------
    stock_df : DataFrame with ['Date','Ticker','Return']
    market_df : DataFrame with ['Date','MarketReturn']
    alphas, betas : dicts of regression parameters per ticker
    event_dates : DataFrame with ['Ticker','EventDate','Sentiment']
    event_window : tuple (start, end) in TRADING days relative to event

    Returns
    -------
    DataFrame with ['Ticker','EventDate','Sentiment','CAR']
    """
    car_list = []

    # Sorted trading days
    trading_days = stock_df['Date'].drop_duplicates().sort_values().reset_index(drop=True)

    for _, row in event_dates.iterrows():
        ticker = row['Ticker']
        event_date = row['EventDate']
        sentiment = row['Sentiment']

        # Snap event date to the nearest prior trading day
        nearest_idx = trading_days.searchsorted(event_date, side="right") - 1
        if nearest_idx < 0 or nearest_idx >= len(trading_days):
            continue

        start_idx = max(0, nearest_idx + event_window[0])
        end_idx   = min(len(trading_days) - 1, nearest_idx + event_window[1])
        window_days = trading_days.iloc[start_idx:end_idx + 1]

        # Subset returns for stock and market
        df = stock_df[(stock_df['Ticker'] == ticker) & (stock_df['Date'].isin(window_days))].copy()
        mkt = market_df[market_df['Date'].isin(window_days)].copy()

        # Skip if data missing
        if df.empty or mkt.empty:
            continue

        # --- NEW: Align by date to avoid length mismatches (7 vs 6 days, etc.) ---
        merged = df.merge(mkt, on='Date', how='inner', suffixes=('_stock', '_market'))
        if merged.empty:
            # No overlapping trading days in this window
            continue

        # --- Compute abnormal returns safely ---
        alpha, beta = alphas.get(ticker, 0), betas.get(ticker, 1)
        # --- Compute abnormal returns safely (detect column names) ---
        if 'Return_stock' in merged.columns and 'Return_market' in merged.columns:
            stock_ret = merged['Return_stock']
            mkt_ret   = merged['Return_market']
        elif 'Return' in merged.columns and 'MarketReturn' in merged.columns:
            stock_ret = merged['Return']
            mkt_ret   = merged['MarketReturn']
        else:
            print(f"[WARN] Could not find expected return columns for {ticker} on {event_date}")
            continue
        
        merged['AbnormalReturn'] = stock_ret - (alpha + beta * mkt_ret)
        car = merged['AbnormalReturn'].sum()

        # Save result
        car_list.append({
            "Ticker": ticker,
            "EventDate": event_date,
            "Sentiment": sentiment,
            "CAR": car
        })

    return pd.DataFrame(car_list)


def main():
    # ===== Synthetic example =====
    np.random.seed(42)

    # Generate 250 trading days
    dates = pd.bdate_range(start="2024-01-01", end="2025-12-31")

    # Market returns
    market_df = pd.DataFrame({
        "Date": dates,
        "MarketReturn": np.random.normal(0, 0.01, size=len(dates))
    })

    # Stock returns for two tickers
    tickers = ["7203.T", "6758.T"]
    stock_data = []
    for t in tickers:
        stock_data.append(pd.DataFrame({
            "Date": dates,
            "Ticker": t,
            "Return": np.random.normal(0, 0.015, size=len(dates))
        }))
    stock_df = pd.concat(stock_data)

    # Alphas and betas (synthetic)
    alphas = {t: 0.0 for t in tickers}
    betas = {t: 1.0 for t in tickers}

    # Example event dates with random sentiment
    event_dates = pd.DataFrame({
        "Ticker": ["7203.T", "6758.T", "7203.T", "6758.T"],
        "EventDate": [pd.Timestamp("2024-06-15"), pd.Timestamp("2024-09-15"),
                      pd.Timestamp("2025-06-15"), pd.Timestamp("2025-09-15")],
        "Sentiment": np.random.uniform(-1, 1, size=4)
    })

    # Calculate CARs for [-3,+3] trading day window
    car_df = calculate_car_per_event(stock_df, market_df, alphas, betas, event_dates, event_window=(-3, 3))
    print("CARs per event:\n", car_df)

    # Regression: CAR ~ Sentiment
    if not car_df.empty:
        X = sm.add_constant(car_df['Sentiment'])
        y = car_df['CAR']
        model = sm.OLS(y, X).fit()
        print("\nRegression Results:\n", model.summary())


if __name__ == "__main__":
    main()
