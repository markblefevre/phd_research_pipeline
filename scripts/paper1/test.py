# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 02:32:47 2026

@author: Mark
"""
from src.prices.compute_lagged_rolling_vol_csv import compute_lagged_rolling_vol_csv
compute_lagged_rolling_vol_csv(
    input_csv="data/curated/paper1/prices/prices_long.csv",
    output_csv="data/curated/paper1/prices/price_features.csv",
    price_col="adj_close",
    offset=11,
    windows=[20, 60, 120],
)