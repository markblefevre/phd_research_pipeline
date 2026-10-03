"""Stage 6D: reproducible Sol/LLM sentiment figures and document-level QC."""
from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

DOC_KEY = ["edinetCode", "docID"]


def _read(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype={"docID": "string", "edinetCode": "string"}, low_memory=False)


def _save(fig, stem: Path, formats: tuple[str, ...], dpi: int) -> None:
    stem.parent.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        fig.savefig(stem.with_suffix('.' + fmt), dpi=dpi, bbox_inches="tight")
    import matplotlib.pyplot as plt
    plt.close(fig)


def generate_llm_plots(*, llm_csv: str | Path, filings_csv: str | Path,
                       output_dir: str | Path, lmmd_csv: str | Path | None = None,
                       bert_csv: str | Path | None = None,
                       formats: tuple[str, ...] = ("pdf", "png"), dpi: int = 180,
                       min_year_observations: int = 25) -> pd.DataFrame:
    """Generate paper/diagnostic plots without changing the frozen LLM scores.

    Optional LMMD/BERT comparisons use identical (edinetCode, docID) keys.
    """
    import matplotlib.pyplot as plt
    llm_csv, filings_csv, output_dir = map(lambda p: Path(p).expanduser().resolve(),
                                           (llm_csv, filings_csv, output_dir))
    paper, diag = output_dir / 'paper', output_dir / 'diagnostics'
    paper.mkdir(parents=True, exist_ok=True)
    diag.mkdir(parents=True, exist_ok=True)
    s, f = _read(llm_csv), _read(filings_csv)
    needed = {'docID', 'llmNet', 'llmPositive', 'llmNegative', 'retentionShare'}
    if needed - set(s.columns):
        raise ValueError(f"LLM scores missing columns: {sorted(needed - set(s.columns))}")
    if {'edinetCode', 'docID', 'periodEnd'} - set(f.columns):
        raise ValueError('Filings must have edinetCode, docID, and periodEnd')
    if s.docID.isna().any() or s.docID.duplicated().any():
        raise ValueError('LLM docID must be unique and nonmissing')
    if f.docID.isna().any() or f.docID.duplicated().any():
        raise ValueError('Filing docID must be unique and nonmissing')
    if s['llmNet'].isna().any():
        raise ValueError('Missing LLM net sentiment')
    df = s.merge(f[DOC_KEY + ['periodEnd']], on='docID', how='left', validate='one_to_one')
    if df.edinetCode.isna().any():
        raise ValueError(f"{df.edinetCode.isna().sum()} LLM documents have no filing metadata")
    if 'sourcePath' in df.columns:
        path_codes = df.sourcePath.astype('string').str.extract(r'/([^/]+)/[^/]+\.txt$', expand=False)
        mismatch = path_codes.notna() & (path_codes != df.edinetCode)
        if mismatch.any():
            raise ValueError(f"{int(mismatch.sum())} LLM source paths disagree with filing edinetCode")
    df['periodEnd'] = pd.to_datetime(df['periodEnd'], errors='coerce')
    if df.periodEnd.isna().any():
        raise ValueError('Missing/invalid filing periodEnd')
    df['fiscalYear'] = df.periodEnd.dt.year.astype(int)
    annual = (df.groupby('fiscalYear', as_index=False)
                .agg(n=('llmNet', 'size'), llmMean=('llmNet', 'mean'),
                     llmMedian=('llmNet', 'median'), llmStd=('llmNet', 'std'),
                     positiveMean=('llmPositive', 'mean'),
                     negativeMean=('llmNegative', 'mean'),
                     retentionMean=('retentionShare', 'mean')))
    annual.to_csv(output_dir / 'annual_llm_summary.csv', index=False)
    view = annual.loc[annual.n >= min_year_observations]
    if view.empty:
        raise ValueError('No year meets min_year_observations')

    fig, ax = plt.subplots(figsize=(7.4, 4.6))
    ax.hist(df.llmNet, bins=75)
    ax.axvline(0, linewidth=1)
    ax.set(xlabel='LLM net sentiment', ylabel='Documents', title='Distribution of LLM net sentiment')
    _save(fig, paper / 'llm_distribution', formats, dpi)

    fig, ax = plt.subplots(figsize=(7.5, 4.6))
    ax.plot(view.fiscalYear, view.llmMean, marker='o', label='Mean')
    ax.plot(view.fiscalYear, view.llmMedian, marker='o', label='Median')
    ax.axhline(0, linewidth=1)
    ax.set(xlabel='Fiscal year (period end)', ylabel='LLM net sentiment', title='LLM net sentiment by fiscal year')
    ax.legend()
    _save(fig, paper / 'llm_by_fiscal_year', formats, dpi)

    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    years = view.fiscalYear.astype(int).tolist()
    ax.boxplot([df.loc[df.fiscalYear.eq(y), 'llmNet'].dropna().to_numpy() for y in years],
               labels=years, showfliers=False)
    ax.axhline(0, linewidth=1)
    ax.tick_params(axis='x', rotation=45)
    ax.set(xlabel='Fiscal year (period end)', ylabel='LLM net sentiment', title='LLM sentiment distribution by year')
    _save(fig, diag / 'llm_by_year_boxplot', formats, dpi)

    fig, ax = plt.subplots(figsize=(7.5, 4.6))
    ax.plot(view.fiscalYear, view.positiveMean, marker='o', label='Positive score')
    ax.plot(view.fiscalYear, view.negativeMean, marker='o', label='Negative score')
    ax.set(xlabel='Fiscal year (period end)', ylabel='Average LLM component score',
           title='LLM positive and negative components by year')
    ax.legend()
    _save(fig, diag / 'llm_components_by_year', formats, dpi)

    fig, ax = plt.subplots(figsize=(7.0, 4.7))
    ax.hist(df.retentionShare.dropna(), bins=60)
    ax.set(xlabel='Retained narrative characters / raw characters', ylabel='Documents',
           title='LLM narrative-filter retention')
    _save(fig, diag / 'llm_retention_distribution', formats, dpi)

    correlations = []
    for path, col, label in ((lmmd_csv, 'lmmdNet', 'LMMD'), (bert_csv, 'bertNet', 'Financial BERT')):
        if path is None:
            continue
        path = Path(path).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(f'Configured comparison CSV missing: {path}')
        other = _read(path)
        if set(DOC_KEY + [col]) - set(other.columns):
            raise ValueError(f'{label} comparison missing document key or {col}')
        if other.duplicated(DOC_KEY).any():
            raise ValueError(f'{label} has duplicate document keys')
        both = df[DOC_KEY + ['llmNet']].merge(other[DOC_KEY + [col]], on=DOC_KEY,
                                             how='inner', validate='one_to_one').dropna()
        if len(both) < 2:
            raise ValueError(f'Insufficient matched observations for {label}')
        pearson = both[['llmNet', col]].corr('pearson').iloc[0, 1]
        spearman = both[['llmNet', col]].corr('spearman').iloc[0, 1]
        correlations.append({'comparison': label, 'n': len(both),
                             'pearson': pearson, 'spearman': spearman})
        fig, ax = plt.subplots(figsize=(6.6, 5.0))
        hb = ax.hexbin(both[col], both.llmNet, gridsize=55, mincnt=1)
        fig.colorbar(hb, ax=ax, label='Documents')
        ax.axhline(0, linewidth=.8)
        ax.axvline(0, linewidth=.8)
        ax.set(xlabel=f'{label} net sentiment', ylabel='LLM net sentiment',
               title=f'LLM vs {label} (Pearson={pearson:.3f}; Spearman={spearman:.3f})')
        _save(fig, diag / ('llm_vs_lmmd' if col == 'lmmdNet' else 'llm_vs_bert'), formats, dpi)
    pd.DataFrame(correlations, columns=['comparison', 'n', 'pearson', 'spearman']).to_csv(
        output_dir / 'llm_model_correlations.csv', index=False)
    return annual


__all__ = ['generate_llm_plots']
