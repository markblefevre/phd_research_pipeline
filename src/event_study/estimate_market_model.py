#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 10 23:23:51 2025

@author: mlefevre
"""
import pandas as pd
#import pandas_market_calendars as mcal
import numpy as np
import statsmodels.api as sm

def estimate_market_model(stock_df, market_df):
    alphas = {}
    betas = {}
    
    for ticker in stock_df['Ticker'].unique():
        df = stock_df[stock_df['Ticker']==ticker].merge(market_df, on='Date')
        X = sm.add_constant(df['MarketReturn'])
        y = df['Return']
        model = sm.OLS(y, X).fit()
        alphas[ticker] = model.params['const']
        betas[ticker] = model.params['MarketReturn']
    return alphas, betas

def estimate_market_model_fast(stock_df: pd.DataFrame, market_df: pd.DataFrame):
    """
    Fast market-model estimation:
        Return_i,t = alpha_i + beta_i * MarketReturn_t + eps_i,t

    Required columns:
      stock_df : Date, Ticker, Return
      market_df: Date, MarketReturn

    Returns:
      alphas: dict[ticker -> alpha]
      betas : dict[ticker -> beta]
    """
    # Keep only needed columns
    s = stock_df[["Date", "Ticker", "Return"]].copy()
    m = market_df[["Date", "MarketReturn"]].copy()

    # Merge once
    df = s.merge(m, on="Date", how="inner")

    # Clean
    df["Return"] = pd.to_numeric(df["Return"], errors="coerce")
    df["MarketReturn"] = pd.to_numeric(df["MarketReturn"], errors="coerce")
    df = df.dropna(subset=["Ticker", "Return", "MarketReturn"])

    # Group means
    g = df.groupby("Ticker", observed=True)
    mean_r = g["Return"].mean()
    mean_m = g["MarketReturn"].mean()

    # Covariance numerator and variance denominator
    df["r_dm"] = df["Return"] - df.groupby("Ticker", observed=True)["Return"].transform("mean")
    df["m_dm"] = df["MarketReturn"] - df.groupby("Ticker", observed=True)["MarketReturn"].transform("mean")

    cov_rm = (df["r_dm"] * df["m_dm"]).groupby(df["Ticker"], observed=True).sum()
    var_m = (df["m_dm"] ** 2).groupby(df["Ticker"], observed=True).sum()

    beta = cov_rm / var_m
    alpha = mean_r - beta * mean_m

    # Optional: drop tickers with too few observations or zero market variance
    valid = np.isfinite(alpha) & np.isfinite(beta)

    alpha = alpha[valid]
    beta = beta[valid]

    return alpha.to_dict(), beta.to_dict()

if __name__ == "__main__":
    import argparse
    import pandas as pd
    import numpy as np

    parser = argparse.ArgumentParser(description="Test estimate_market_model() with synthetic data or CSVs.")
    parser.add_argument("--mode", choices=["synthetic", "csv"], default="synthetic",
                        help="Run synthetic demo or load CSV files.")
    parser.add_argument("--stocks", type=str, help="Path to stock CSV with columns: Date,Ticker,Return")
    parser.add_argument("--market", type=str, help="Path to market CSV with columns: Date,MarketReturn")
    parser.add_argument("--start", type=str, default="2020-01-01", help="Synthetic: start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default="2020-12-31", help="Synthetic: end date (YYYY-MM-DD)")
    parser.add_argument("--seed", type=int, default=42, help="Synthetic: random seed")
    args = parser.parse_args()

    if args.mode == "csv":
        if not args.stocks or not args.market:
            raise SystemExit("In CSV mode, provide --stocks and --market")
        stock_df = pd.read_csv(args.stocks, parse_dates=["Date"])
        market_df = pd.read_csv(args.market, parse_dates=["Date"])
    else:
        # --- Synthetic demo ---
        rng = np.random.default_rng(args.seed)
        dates = pd.bdate_range(args.start, args.end, freq="C")  # business days
        n = len(dates)

        # Simulate market returns
        market_ret = rng.normal(loc=0.0003, scale=0.01, size=n)  # ~mean 7.5% annualized, ~1% daily vol
        market_df = pd.DataFrame({"Date": dates, "MarketReturn": market_ret})

        # Two stocks with distinct (alpha, beta)
        params = {
            "7203.T": {"alpha": 0.0005, "beta": 1.20, "eps": 0.008},  # Toyota-like
            "9432.T": {"alpha": -0.0002, "beta": 0.80, "eps": 0.009}, # NTT-like
        }

        rows = []
        for tkr, p in params.items():
            eps = rng.normal(loc=0.0, scale=p["eps"], size=n)
            r = p["alpha"] + p["beta"] * market_ret + eps
            rows.append(pd.DataFrame({"Date": dates, "Ticker": tkr, "Return": r}))
        stock_df = pd.concat(rows, ignore_index=True)

    # Ensure types and sort
    stock_df["Date"] = pd.to_datetime(stock_df["Date"])
    market_df["Date"] = pd.to_datetime(market_df["Date"])
    stock_df = stock_df.sort_values(["Ticker", "Date"])
    market_df = market_df.sort_values("Date")

    # Run estimation
    alphas, betas = estimate_market_model(stock_df, market_df)

    # Pretty print
    print("\nEstimated alphas:")
    for k, v in sorted(alphas.items()):
        print(f"  {k:10s}  {v: .6f}")
    print("\nEstimated betas:")
    for k, v in sorted(betas.items()):
        print(f"  {k:10s}  {v: .4f}")
