#!/usr/bin/env python3
from __future__ import annotations

import sys
import json
import logging
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple
import time
import os

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:
    import tomli as tomllib  # Python <=3.10


# ---- Import your stage(s) ----
from src.prices.fetch_jpx_prices import run_fetch_jpx_prices
from src.prices.compute_lagged_rolling_vol_csv import compute_lagged_rolling_vol_csv
from src.prices.fetch_market_indexes import run_fetch_market_indexes
from src.panel.run_lmmd_score import run_lmmd_score
from src.event_study.run_market_model import run_market_model
from src.event_study.run_car_computation import run_car_computation
from src.event_study.run_event_study_regression import run_regression
from src.panel.run_build_panel import build_panel
from src.panel.build_regression_dataset import build_regression_dataset


# ----------------------------
# Helpers
# ----------------------------

def repo_root() -> Path:
    # scripts/paper1/run_pipeline.py -> parents[2] is repo root
    return Path(__file__).resolve().parents[2]


def load_toml(path: Path) -> Dict[str, Any]:
    with path.open("rb") as f:
        return tomllib.load(f)


def now_run_id() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")


def get_git_commit_short(root: Path) -> str | None:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(root),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out or None
    except Exception:
        return None


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def setup_logging(log_file: Path) -> logging.Logger:
    ensure_dir(log_file.parent)

    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if re-run in same interpreter session (Spyder)
    if logger.handlers:
        logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


def write_run_metadata(
    run_dir: Path,
    cfg: Dict[str, Any],
    run_id: str,
    paper: str,
    logger: logging.Logger,
) -> None:
    ensure_dir(run_dir)

    # command used
    (run_dir / "command.txt").write_text(" ".join(sys.argv) + "\n", encoding="utf-8")

    # git info
    commit = get_git_commit_short(repo_root())
    (run_dir / "git.txt").write_text((commit or "unknown") + "\n", encoding="utf-8")

    # resolved config snapshot (json is easiest without extra deps)
    snapshot = {
        "paper": paper,
        "run_id": run_id,
        "config": cfg,
    }
    (run_dir / "config_resolved.json").write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    logger.info("Run metadata written to %s", run_dir)


def parse_windows(raw: Any) -> List[Tuple[int, int]]:
    """
    raw should look like: [[0,0],[0,1],[-1,1]]
    """
    windows: List[Tuple[int, int]] = []
    if raw is None:
        return windows
    for w in raw:
        if not (isinstance(w, (list, tuple)) and len(w) == 2):
            raise ValueError(f"Invalid window entry: {w!r} (expected [a,b])")
        windows.append((int(w[0]), int(w[1])))
    return windows

# ----------------------------
# Stage: Fetch JPX Prices
# ----------------------------
def run_stage_price_data(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        pd_cfg = cfg.get("price_data", {})
        skip_if_exists = bool(pd_cfg.get("skip_if_exists", True))
        root = repo_root()

        symbols_csv = pd_cfg.get("symbols_csv")
        if not symbols_csv:
            raise ValueError("Config error: [price_data] requires symbols_csv")

        symbols_col = pd_cfg.get("symbols_col", "symbol")
        start = pd_cfg.get("start", "2015-01-01")
        end = pd_cfg.get("end") or None
        chunk_size = int(pd_cfg.get("chunk_size", 60))
        retries = int(pd_cfg.get("retries", 1))

        stocks_out_csv = pd_cfg.get("stocks_out_csv", f"data/curated/{paper}/prices/prices_long.csv")
        dividends_out_csv = pd_cfg.get("dividends_out_csv", f"data/curated/{paper}/prices/dividends.csv")
        splits_out_csv = pd_cfg.get("splits_out_csv", f"data/curated/{paper}/prices/splits.csv")

        symbols_csv_p = root / symbols_csv
        prices_csv_p = root / stocks_out_csv
        dividends_csv_p = root / dividends_out_csv
        splits_csv_p = root / splits_out_csv

        # run_fetch_jpx_prices writes fixed filenames into a single output directory,
        # so require all configured outputs to live in the same directory.
        outdirs = {
            prices_csv_p.parent.resolve(),
            dividends_csv_p.parent.resolve(),
            splits_csv_p.parent.resolve(),
        }
        if len(outdirs) != 1:
            raise ValueError(
                "Config error: stocks_out_csv, dividends_out_csv, and splits_out_csv "
                "must all be in the same directory for run_fetch_jpx_prices"
            )
        stocks_outdir_p = prices_csv_p.parent

        # Optional sanity check: current fetcher writes fixed filenames
        if prices_csv_p.name != "prices_long.csv":
            raise ValueError("Config error: stocks_out_csv filename must be 'prices_long.csv'")
        if dividends_csv_p.name != "dividends.csv":
            raise ValueError("Config error: dividends_out_csv filename must be 'dividends.csv'")
        if splits_csv_p.name != "splits.csv":
            raise ValueError("Config error: splits_out_csv filename must be 'splits.csv'")

        logger.info("Stage price_data: symbols_csv=%s", symbols_csv_p)
        logger.info("Stage price_data: prices_csv=%s", prices_csv_p)
        logger.info("Stage price_data: dividends_csv=%s", dividends_csv_p)
        logger.info("Stage price_data: splits_csv=%s", splits_csv_p)
        logger.info("Stage price_data: symbols_col=%s", symbols_col)
        logger.info("Stage price_data: start=%s end=%s", start, end)

        if skip_if_exists and prices_csv_p.exists() and dividends_csv_p.exists() and splits_csv_p.exists():
            status = "skipped"
            logger.info("[SKIP] price_data already done at %s", stocks_outdir_p)
            return

        if not symbols_csv_p.exists():
            raise FileNotFoundError(f"Missing symbols_csv: {symbols_csv_p}")

        stocks_outdir_p.mkdir(parents=True, exist_ok=True)

        logger.info("[RUN] price_data")
        summary = run_fetch_jpx_prices(
            input_csv=symbols_csv_p,
            outdir=stocks_outdir_p,
            symbols_col=symbols_col,
            start=start,
            end=end,
            chunk_size=chunk_size,
            retries=retries,
        )

        logger.info("price_data summary: %s", summary)

        if not prices_csv_p.exists():
            raise FileNotFoundError(f"Expected output not written: {prices_csv_p}")
        if not dividends_csv_p.exists():
            logger.warning("Dividend output not found: %s", dividends_csv_p)
        if not splits_csv_p.exists():
            logger.warning("Split output not found: %s", splits_csv_p)

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info("Stage price_data finished: status=%s elapsed=%.3fs", status, elapsed)

# ----------------------------
# Stage: Price Features (lagged rolling volatility)
# ----------------------------
def run_stage_price_features(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        pf_cfg = cfg.get("price_features", {})
        skip_if_exists = bool(pf_cfg.get("skip_if_exists", True))
        root = repo_root()

        input_csv = pf_cfg.get("input_csv", f"data/curated/{paper}/prices/prices_long.csv")
        output_csv = pf_cfg.get("output_csv", f"data/curated/{paper}/prices/price_features.csv")
        price_col = pf_cfg.get("price_col", "adj_close")
        symbol_col = pf_cfg.get("symbol_col", "symbol")
        date_col = pf_cfg.get("date_col", "date")
        return_col = pf_cfg.get("return_col", "ret")
        offset = int(pf_cfg.get("offset", 11))
        windows = pf_cfg.get("windows", [20, 60, 120])
        ddof = int(pf_cfg.get("ddof", 1))

        input_csv_p = root / input_csv
        output_csv_p = root / output_csv

        logger.info("Stage price_features: input_csv=%s", input_csv_p)
        logger.info("Stage price_features: output_csv=%s", output_csv_p)
        logger.info(
            "Stage price_features: price_col=%s symbol_col=%s date_col=%s return_col=%s offset=%s windows=%s ddof=%s",
            price_col, symbol_col, date_col, return_col, offset, windows, ddof
        )

        if skip_if_exists and output_csv_p.exists():
            status = "skipped"
            logger.info("[SKIP] price_features already done at %s", output_csv_p)
            return

        if not input_csv_p.exists():
            raise FileNotFoundError(f"Missing input_csv: {input_csv_p}")

        logger.info("[RUN] price_features")
        df_out = compute_lagged_rolling_vol_csv(
            input_csv=input_csv_p,
            output_csv=output_csv_p,
            price_col=price_col,
            symbol_col=symbol_col,
            date_col=date_col,
            offset=offset,
            windows=windows,
            return_col=return_col,
            ddof=ddof,
        )

        logger.info(
            "price_features wrote %s rows x %s cols to %s",
            len(df_out),
            len(df_out.columns),
            output_csv_p,
        )

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info("Stage price_features finished: status=%s elapsed=%.3fs", status, elapsed)        

# ----------------------------
# Stage: Market Indexes
# ----------------------------
def run_stage_market_indexes(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        mi_cfg = cfg.get("market_indexes", {})
        skip_if_exists = bool(mi_cfg.get("skip_if_exists", True))
        root = repo_root()

        topix_out_csv = mi_cfg.get("topix_out_csv", f"data/curated/{paper}/prices/TOPIX_prices.csv")
        n225_out_csv = mi_cfg.get("n225_out_csv", f"data/curated/{paper}/prices/N225_prices.csv")
        topix_source = mi_cfg.get("topix_source", "jpx")
        start = mi_cfg.get("start", None)
        end = mi_cfg.get("end") or None
        jpx_api_key_env = mi_cfg.get("jpx_api_key_env", "JQUANTS_API_KEY")

        topix_out_csv_p = root / topix_out_csv
        n225_out_csv_p = root / n225_out_csv

        outdirs = {
            topix_out_csv_p.parent.resolve(),
            n225_out_csv_p.parent.resolve(),
        }
        if len(outdirs) != 1:
            raise ValueError(
                "Config error: topix_out_csv and n225_out_csv must be in the same directory "
                "for run_fetch_market_indexes"
            )
        outdir_p = topix_out_csv_p.parent

        if topix_out_csv_p.name != "TOPIX_prices.csv":
            raise ValueError("Config error: topix_out_csv filename must be 'TOPIX_prices.csv'")
        if n225_out_csv_p.name != "N225_prices.csv":
            raise ValueError("Config error: n225_out_csv filename must be 'N225_prices.csv'")

        jpx_api_key = os.getenv(jpx_api_key_env)

        logger.info("Stage market_indexes: outdir=%s", outdir_p)
        logger.info("Stage market_indexes: topix_out_csv=%s", topix_out_csv_p)
        logger.info("Stage market_indexes: n225_out_csv=%s", n225_out_csv_p)
        logger.info("Stage market_indexes: topix_source=%s", topix_source)
        logger.info("Stage market_indexes: start=%s end=%s", start, end)
        logger.info("Stage market_indexes: jpx_api_key_env=%s present=%s", jpx_api_key_env, bool(jpx_api_key))

        if skip_if_exists and topix_out_csv_p.exists() and n225_out_csv_p.exists():
            status = "skipped"
            logger.info("[SKIP] market_indexes already done at %s", outdir_p)
            return

        outdir_p.mkdir(parents=True, exist_ok=True)

        logger.info("[RUN] market_indexes")
        summary = run_fetch_market_indexes(
            outdir=outdir_p,
            start=start,
            end=end,
            topix_source=topix_source,
            jpx_api_key=jpx_api_key,
        )
        logger.info("market_indexes summary: %s", summary)

        if not topix_out_csv_p.exists():
            raise FileNotFoundError(f"Expected output not written: {topix_out_csv_p}")
        if not n225_out_csv_p.exists():
            raise FileNotFoundError(f"Expected output not written: {n225_out_csv_p}")

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info("Stage market_indexes finished: status=%s elapsed=%.3fs", status, elapsed)
        
# ----------------------------
# Stage: LMMD Score
# ----------------------------
def run_stage_lmmd_score(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    t0 = time.perf_counter()
    status = "ok"
    try:
        ls_cfg = cfg.get("lmmd_score", {})
        skip_if_exists = bool(ls_cfg.get("skip_if_exists", True))
        root = repo_root()

        index_csv = root / ls_cfg.get("index_csv", f"data/curated/{paper}/panel/mdna_summary_nikkei225_filtered.csv")
        mdna_dir = root / ls_cfg.get("mdna_dir", f"data/interim/{paper}/data")
        lmmd_dict_csv = root / ls_cfg["lmmd_dict_csv"]
        out_csv = root / ls_cfg.get("out_csv", f"data/curated/{paper}/panel/lmmd_scores_nikkei225.csv")

        token_col = ls_cfg.get("token_col", "GPT_JA")

        logger.info("Stage lmmd_score: index_csv=%s", index_csv)
        logger.info("Stage lmmd_score: mdna_dir=%s", mdna_dir)
        logger.info("Stage lmmd_score: lmmd_dict_csv=%s", lmmd_dict_csv)
        logger.info("Stage lmmd_score: out_csv=%s", out_csv)

        if skip_if_exists and out_csv.exists():
            status = "skipped"
            logger.info("[SKIP] lmmd_score already done at %s", out_csv)
            return

        if not index_csv.exists():
            raise FileNotFoundError(f"Missing index_csv: {index_csv}")
        if not mdna_dir.exists():
            raise FileNotFoundError(f"Missing mdna_dir: {mdna_dir}")
        if not lmmd_dict_csv.exists():
            raise FileNotFoundError(f"Missing lmmd_dict_csv: {lmmd_dict_csv}")

        logger.info("[RUN] lmmd_score")
        df = run_lmmd_score(
            index_csv=index_csv,
            mdna_dir=mdna_dir,
            lmmd_dict_csv=lmmd_dict_csv,
            out_csv=out_csv,
            token_col=token_col,
        )
        logger.info("lmmd_score produced %s rows -> %s", df.shape[0], out_csv)

    except Exception:
        status = "failed"
        raise
    finally:
        elapsed = time.perf_counter() - t0
        logger.info("Stage lmmd_score finished: status=%s elapsed=%.3fs", status, elapsed)

# ----------------------------
# Stage: Build Panel (GPT and LMMD)
# ----------------------------

def run_stage_build_panel(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        bp_cfg = cfg.get("build_panel", {})
        skip_if_exists = bool(bp_cfg.get("skip_if_exists", True))
        root = repo_root()

        gpt_rel = bp_cfg.get("gpt_csv")
        lmmd_rel = bp_cfg.get("lmmd_csv")
        out_rel = bp_cfg.get("out_csv")
        fundamentals_rel = bp_cfg.get("fundamentals_csv")
        price_features_rel = bp_cfg.get("price_features_csv")
        if not (gpt_rel and lmmd_rel and out_rel and fundamentals_rel and price_features_rel):
            raise ValueError("Config error: [build_panel] requires gpt_csv, lmmd_csv, out_csv, fundamentals_csv, price_features_csv")

        # Convert TOML strings -> absolute Paths
        gpt_csv = root / gpt_rel
        lmmd_csv = root / lmmd_rel
        out_csv = root / out_rel
        fundamentals_csv = root / fundamentals_rel
        price_features_csv = root / price_features_rel

        logger.info("Stage build_panel: gpt_csv=%s", gpt_csv)
        logger.info("Stage build_panel: lmmd_csv=%s", lmmd_csv)
        logger.info("Stage build_panel: out_csv=%s", out_csv)
        logger.info("Stage build_panel: fundamentals_csv=%s", fundamentals_csv)
        logger.info("Stage build_panel: price_features_csv=%s", price_features_csv)

        if skip_if_exists and out_csv.exists():
            status = "skipped"
            logger.info("[SKIP] build_panel already done at %s", out_csv)
            return

        if not gpt_csv.exists():
            raise FileNotFoundError(f"Missing GPT input: {gpt_csv}")
        if not lmmd_csv.exists():
            raise FileNotFoundError(f"Missing LMMD input: {lmmd_csv}")
        if fundamentals_csv and not fundamentals_csv.exists():
            raise FileNotFoundError(f"Missing fundamentals_csv: {fundamentals_csv}")
        if price_features_csv and not price_features_csv.exists():
            raise FileNotFoundError(f"Missing price_features_csv: {price_features_csv}")

        logger.info("[RUN] build_panel")
        df = build_panel(gpt_csv=gpt_csv, lmmd_csv=lmmd_csv, fundamentals_csv=fundamentals_csv,
                         price_features_csv=price_features_csv, out_csv=out_csv)
        logger.info("build_panel rows=%s cols=%s -> %s", df.shape[0], df.shape[1], out_csv)

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info("Stage build_panel finished: status=%s elapsed=%.3fs", status, elapsed)


# ----------------------------
# Stage: Market model (alphas/betas)
# ----------------------------

def market_model_done(out_csv: Path) -> bool:
    return out_csv.exists()


def run_stage_market_model(
    *,
    paper: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        mm_cfg = cfg.get("market_model", {})
        skip_if_exists = bool(mm_cfg.get("skip_if_exists", True))

        root = repo_root()

        panel_csv = mm_cfg.get("panel_csv", f"data/curated/{paper}/panel/mdna_summary_nikkei225_with_lmmd.csv")
        market_csv = mm_cfg.get("market_csv", f"data/curated/{paper}/prices/TOPIX_prices.csv")
        stocks_csv = mm_cfg.get("stocks_csv", f"data/curated/{paper}/prices/prices_long.csv")
        out_csv = mm_cfg.get("out_csv", f"data/curated/{paper}/event_study/alphas_betas.csv")

        panel_csv_p = root / panel_csv
        market_csv_p = root / market_csv
        stocks_csv_p = root / stocks_csv
        out_csv_p = root / out_csv

        logger.info("Stage market_model: panel_csv=%s", panel_csv_p)
        logger.info("Stage market_model: market_csv=%s", market_csv_p)
        logger.info("Stage market_model: stocks_csv=%s", stocks_csv_p)
        logger.info("Stage market_model: out_csv=%s", out_csv_p)

        if skip_if_exists and market_model_done(out_csv_p):
            status = "skipped"
            logger.info("[SKIP] market_model already done at %s", out_csv_p)
            return

        if not market_csv_p.exists():
            raise FileNotFoundError(f"Missing market_csv: {market_csv_p}")
        if not stocks_csv_p.exists():
            raise FileNotFoundError(f"Missing stocks_csv: {stocks_csv_p}")

        logger.info("[RUN] market_model")
        res = run_market_model(
            panel_csv=panel_csv_p,
            market_csv=market_csv_p,
            stocks_csv=stocks_csv_p,
            out_csv=out_csv_p,
        )
        logger.info("market_model produced %s rows -> %s", res.shape[0], out_csv_p)

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info("Stage market_model finished: status=%s elapsed=%.3fs", status, elapsed)
    
# ----------------------------
# Stage: Event study
# ----------------------------

def event_study_out_dir(root: Path, paper: str, run_id: str) -> Path:
    return root / "outputs" / paper / run_id / "event_study"


def event_study_done(out_dir: Path) -> bool:
    # Check for at least one CAR output file
    return any(out_dir.glob("car_results_*.csv"))


def regression_out_path(root: Path, paper: str, run_id: str) -> Path:
    return root / "outputs" / paper / run_id / "event_study" / "regression_summary.csv"


def regression_done(root: Path, paper: str, run_id: str) -> bool:
    return regression_out_path(root, paper, run_id).exists()


def run_stage_car_computation(
    *,
    paper: str,
    run_id: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        es_cfg = cfg.get("car_computation", {})
        windows = parse_windows(es_cfg.get("windows", [[0, 0], [0, 1], [-1, 1]]))
        skip_if_exists = bool(es_cfg.get("skip_if_exists", True))

        out_dir = ensure_dir(event_study_out_dir(repo_root(), paper, run_id))

        logger.info("Stage car_computation: out_dir=%s", out_dir)
        logger.info("Stage car_computation: windows=%s", windows)

        ran_any = False
        all_skipped = True

        if skip_if_exists and event_study_done(out_dir):
            logger.info("[SKIP] CAR computation already done")
            status = "skipped"
            return
        
        all_skipped = False
        ran_any = True
        
        inputs = es_cfg.get("inputs", {})
        root = repo_root()
        
        sentiment_csv = inputs.get("sentiment_csv")
        alphas_betas_csv = inputs.get("alphas_betas_csv")
        prices_csv = inputs.get("prices_csv")
        market_csv = inputs.get("market_csv")
        
        sentiment_csv = (root / sentiment_csv) if sentiment_csv else None
        alphas_betas_csv = (root / alphas_betas_csv) if alphas_betas_csv else None
        prices_csv = (root / prices_csv) if prices_csv else None
        market_csv = (root / market_csv) if market_csv else None
        
        logger.info("[RUN] CAR computation")
        run_car_computation(
            windows=windows,
            paper=paper,
            run_id=run_id,
            sentiment_csv=sentiment_csv,
            alphas_betas_csv=alphas_betas_csv,
            prices_csv=prices_csv,
            market_csv=market_csv,
        )

        if all_skipped and not ran_any:
            status = "skipped"

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info("Stage car_computation finished: status=%s elapsed=%.3fs", status, elapsed)


# ----------------------------
# Stage: Regression Dataset
# ----------------------------
def run_stage_regression_dataset(
    *,
    paper: str,
    run_id: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        ds_cfg = cfg.get("regression_dataset", {})
        skip_if_exists = bool(ds_cfg.get("skip_if_exists", True))

        # --- FIX: use windows (plural) ---
        windows = parse_windows(ds_cfg.get("windows", [[0, 1]]))

        out_dir = ensure_dir(event_study_out_dir(repo_root(), paper, run_id))

        inputs = ds_cfg.get("inputs", {})
        panel_csv = inputs.get("panel_csv")

        if not panel_csv:
            raise ValueError("regression_dataset requires panel_csv")

        panel_csv = repo_root() / panel_csv

        if not panel_csv.exists():
            raise FileNotFoundError(f"Missing panel_csv: {panel_csv}")

        # --- FIX: loop over all windows ---
        for w0, w1 in windows:
            out_csv = out_dir / f"regression_dataset_{w0}_{w1}.csv"

            if skip_if_exists and out_csv.exists():
                logger.info("[SKIP] regression_dataset already exists for window=(%s,%s)", w0, w1)
                continue

            car_csv = out_dir / f"car_results_{w0}_{w1}.csv"

            if not car_csv.exists():
                raise FileNotFoundError(f"Missing CAR file: {car_csv}")

            logger.info("[RUN] regression_dataset window=(%s,%s)", w0, w1)

            df = build_regression_dataset(
                car_csv=car_csv,
                panel_csv=panel_csv,
                out_csv=out_csv,
            )

            logger.info("regression_dataset rows=%s -> %s", len(df), out_csv)

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info("Stage regression_dataset finished: status=%s elapsed=%.3fs", status, elapsed)        
        
# ----------------------------
# Stage: Joint Regression
# ----------------------------

def run_stage_regression(
    *,
    paper: str,
    run_id: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
    windows: List[Tuple[int, int]],
) -> None:
    t0 = time.perf_counter()
    status = "ok"

    try:
        hs_cfg = cfg.get("regression", {})
        skip_if_exists = bool(hs_cfg.get("skip_if_exists", True))

        hs_windows = hs_cfg.get("windows", None)
        if hs_windows is not None:
            windows = parse_windows(hs_windows)

        out_path = regression_out_path(repo_root(), paper, run_id)

        logger.info("Stage regression: out_path=%s", out_path)
        logger.info("Stage regression: windows=%s", windows)

        if skip_if_exists and out_path.exists():
            status = "skipped"
            logger.info("[SKIP] regression already done at %s", out_path)
            return

        inputs = hs_cfg.get("inputs", {})
        panel_csv = inputs.get("panel_csv")
        if panel_csv:
            panel_csv = repo_root() / panel_csv

        logger.info("[RUN] regression")
        run_regression(
            windows=windows,
            paper=paper,
            run_id=run_id,
            panel_csv=panel_csv,
        )

    except Exception:
        status = "failed"
        raise

    finally:
        elapsed = time.perf_counter() - t0
        logger.info("Stage regression finished: status=%s elapsed=%.3fs", status, elapsed)


# ----------------------------
# Main pipeline
# ----------------------------

def main() -> int:
    root = repo_root()
    config_path = root / "configs" / "paper1" / "pipeline.toml"
    if not config_path.exists():
        print(f"[ERROR] Missing config: {config_path}")
        return 2

    cfg = load_toml(config_path)

    # Run identity: ONE run_id for ALL stages
    run_cfg = cfg.get("run", {})
    paper = run_cfg.get("paper", "paper1")

    run_id_cfg = run_cfg.get("run_id", "auto")
    run_id = run_id_cfg if (isinstance(run_id_cfg, str) and run_id_cfg != "auto") else now_run_id()

    # Standard run folders
    out_base = ensure_dir(root / "outputs" / paper / run_id)
    run_dir = ensure_dir(root / "runs" / paper / run_id)
    log_file = out_base / "logs" / "run.log"

    logger = setup_logging(log_file)
    logger.info("Pipeline start: paper=%s run_id=%s", paper, run_id)
    logger.info("Repo root: %s", root)
    logger.info("Config path: %s", config_path)

    write_run_metadata(run_dir, cfg, run_id, paper, logger)

    stages = cfg.get("stages", {})
    if not isinstance(stages, dict):
        logger.error("Config error: [stages] must be a table/dict")
        return 2

    es_cfg = cfg.get("car_computation", {})
    windows = parse_windows(es_cfg.get("windows", [[0, 0], [0, 1], [-1, 1]]))

    # Stage execution order (expand as you add stages)
    if bool(stages.get("price_data", False)):
        run_stage_price_data(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage price_data disabled")

    if bool(stages.get("price_features", False)):
        run_stage_price_features(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage price_features disabled")
        
    if bool(stages.get("market_indexes", False)):
        run_stage_market_indexes(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage market_indexes disabled")

    if bool(stages.get("lmmd_score", False)):
        run_stage_lmmd_score(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage lmmd_score disabled")

    ## STAGE build_panel (must run before event_study)
    if bool(stages.get("build_panel", False)):
        run_stage_build_panel(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage build_panel disabled")
        
    ## STAGE market_model (must run before event_study if alphas_betas not present)
    if bool(stages.get("market_model", False)):
        run_stage_market_model(paper=paper, cfg=cfg, logger=logger)
    else:
        logger.info("Stage market_model disabled")

    ## STAGE car_computation
    if bool(stages.get("car_computation", False)):
        run_stage_car_computation(paper=paper, run_id=run_id, cfg=cfg, logger=logger)
    else:
        logger.info("Stage car_computation disabled")

    ## STAGE regression_dataset
    if bool(stages.get("regression_dataset", False)):
        run_stage_regression_dataset(paper=paper, run_id=run_id, cfg=cfg, logger=logger)
    else:
        logger.info("Stage regression_dataset disabled")
    
    ## STAGE regression
    if bool(stages.get("regression", False)):
        run_stage_regression(paper=paper, run_id=run_id, cfg=cfg, logger=logger, windows=windows)
    else:
        logger.info("Stage regression disabled")
    
    logger.info("Pipeline done: paper=%s run_id=%s", paper, run_id)
    logger.info("Outputs base: %s", out_base)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
