# Pipeline Documentation (Detailed)

Last updated: 2026-03-27  
Owner: Mark Lefevre

---

# Purpose

This document defines the full research pipeline architecture for Paper 1.

It ensures:
- Reproducibility
- Clear separation of concerns
- Consistent econometric design

---

# Core Design Principle

CAR computation is completely independent of sentiment.

Pipeline:

CAR → regression_dataset → regression

Implications:
- CAR computed once per window
- No duplication across sentiment variables
- Sentiment merged only at regression_dataset stage
- All regressions share identical dependent variable

---

# Directory Structure

src/        → core modules  
scripts/    → pipeline drivers  
configs/    → TOML configs  
data/       → raw / interim / curated  
outputs/    → run outputs  
runs/       → metadata  

---

# Run Identity

Outputs stored in:

outputs/<paper>/<run_id>/

---

# Stage Inventory

| Stage | Name | Description |
|------|------|-------------|
| S0 | LMMD scoring | Dictionary sentiment |
| S0.5 | Market model | Estimate alpha/beta |
| S0.75 | Build panel | Merge GPT + LMMD + controls |
| S1 | CAR computation | Compute abnormal returns |
| S2 | Regression dataset | Merge + z-score |
| S3 | Regression | Run models |

---

# S1 — CAR Computation

Purpose:
- Compute CAR per (Ticker, EventDate)

Inputs:
- Panel (event dates)
- Prices
- Market returns
- Alpha/Beta

Parameters:
windows = [[0,0],[0,1],[-1,1],[-2,2],[-3,3]]

Outputs:
car_results_{w0}_{w1}.csv

Columns:
Ticker, EventDate, CAR

Notes:
- No sentiment included
- Computed once per window

---

# S2 — Regression Dataset

Purpose:
- Merge CAR with sentiment and controls
- Create regression-ready dataset

Inputs:
- CAR files
- Panel dataset

Transformations:
- Rename columns
- Compute z-scores:
  gpt_z, lmmd_z, neg_z, pos_z
- Compute disagreement:
  gpt_z - lmmd_z

Outputs:
regression_dataset_{w0}_{w1}.csv

---

# S3 — Regression

Purpose:
- Run regression specifications

Models include:
- GPT
- LMMD
- GPT + LMMD
- Neg / Pos
- Disagreement
- With/without FE
- With/without controls

Outputs:
regression_summary.csv

Contains:
spec, coefficients, SE, p-values, FE flags, N, R²

---

# Config Example

[car_computation]
windows = [[0,0],[0,1],[-1,1],[-2,2],[-3,3]]

[regression_dataset]
windows = [[0,0],[0,1],[-1,1],[-2,2],[-3,3]]

[regression]
windows = [[0,0],[0,1],[-1,1],[-2,2],[-3,3]]

---

# Workflow

build_panel  
→ market_model  
→ car_computation  
→ regression_dataset  
→ regression  
→ tables  

---

# Design Guarantees

- No recomputation of CAR  
- Clean separation of stages  
- Consistent dependent variable  
- Comparable regressions  

---

# Future Extensions

- Table generation stage  
- Additional controls  
- Alternative return models  

---

# Changelog

2026-03-27:
- Introduced regression_dataset stage
- Removed sentiment loop from CAR
- Renamed horserace → regression
- Standardized outputs
