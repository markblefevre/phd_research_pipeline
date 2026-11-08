# Paper 2 — preliminary extreme-CAR event review (November 7, 2026)

**Source:** Recovered 96-row Stage 9 CSV (`paper2_extreme_car_events_20261107.csv`). All observations exceed 20 absolute percentage points for the [-1,+1] market-model CAR. This is an *investigative aide*, not a causal identification or validation of data prices. No regression or LLM step was rerun.

## Provenance and reproducibility

- **Date reviewed:** 2026-11-07. This folder records an exploratory **ChatGPT-assisted** analysis, not a separately validated research result.
- **Numeric source:** existing Paper 2 Stage 9 primary regression data and its precomputed three-day market-model CARs. The 96-row extraction uses baseline regression eligibility and `abs(car_pp_m1_1) > 20` percentage points. The unannotated `extreme_car_events.csv` is the frozen extraction; `extreme_car_review.csv` adds news-search and explanatory fields without replacing source returns. The exact upstream dataset file and extraction command should be recorded when the extraction is rerun locally; they were not independently re-established for this package.
- **External evidence:** company/exchange/news items were found with ChatGPT-assisted web research. Evidence URLs, possible mechanisms, and unreviewed statuses are preserved in the review CSV. Each cited source and timestamp must be checked against the underlying page or announcement before use as definitive manuscript evidence. Suggested mechanisms are **not** demonstrated causes of CAR.
- **Coverage:** 21 events have preliminary event-level annotations; 75 have not been individually researched. The set reviewed was deliberately weighted toward the largest returns and is not a representative sample for estimating the prevalence of overlapping news.
- **No computational rerun:** the original market-model CAR construction, LLM classification/sentiment scoring, and regressions were not rerun in this review. Arithmetic reconciliation of daily abnormal returns with the stored CAR does not validate original adjusted prices or corporate actions.
- **Research use:** supplementary explanation for §5.6 and optional appendix; retain the full-sample baseline and existing outlier-sensitivity regressions. Do not exclude filings on the basis of news identified post hoc.

## Descriptive patterns within the 96 extremes

- 66 positive and 30 negative; 17 of top 20 are positive.
- 54 before/at-close submissions; 42 after-close. The latter's assigned event day is the next trading session.
- Largest |AR| occurs on day −1 for 33 events, on day 0 for 30, and day +1 for 33. Abs-return contributions summed across these cases: about 32%, 35%, 33%, respectively.
- 22 occur in 2020, 16 in 2025, 16 in 2026. These are within-extreme counts, *not annual incidence rates*; baseline yearly event counts have not been compared.
- Newly introduced sentiment is positive in 36, negative in 35, and exactly zero in 25. CAR direction and nonzero new-sentiment direction agree in 41 cases and disagree in 30. These are purely descriptive; they are **not** estimates of regression influence or a sign test.
- All three daily abnormal returns sum to the recorded three-day CAR with negligible floating-point error. This does not independently confirm original price data, adjusted shares, or corporate actions.

## External research scope

- 4 — Documented contemporaneous news events
- 2 — Documented exchange action events
- 1 — Multiple contemporaneous disclosures events
- 1 — Sector explanation reported events
- 1 — Documented audit event events
- 1 — Contemporaneous disclosure identified events
- 4 — Price trend confirmed only events
- 1 — Documented sector context events
- 1 — Contemporaneous market reporting events
- 1 — Broad theme documented; timing uncertain events
- 1 — Disclosure search only events
- 1 — Price movement confirmed only events
- 1 — Documented prior corporate event events
- 75 — Not individually researched events
- 1 — Documented contemporaneous results events

Only the explicitly annotated rows have been checked against news, exchange releases, issuer communications, or contemporaneous price histories; all other records remain unreviewed. The included source URLs document the existence and timing of plausible events, not that those events explain every percentage point of CAR. Some recorded items are market commentary or unconfirmed rumors and are labeled accordingly. For a rigorous full classification of all 96, manual event-by-event verification remains necessary.

## Emerging explanations

1. **Other announcements before the filing:** Daikokuya's AI purchasing announcement (rank 1), EMNET's generative-AI service (rank 2), Cynthia's buyback (rank 14), and Sekido's new sales figures (rank 17).
2. **Filing-related listing consequences:** ENECHANGE (rank 3) and Shodensha (rank 4) regained relief from TSE supervision designations on filing of overdue annual reports.
3. **Auditor/financial distress:** Kaihan's disclaimer of audit opinion (rank 7); Advance Create's negative equity and delayed financial disclosures (rank 23).
4. **Contemporaneous business disclosures and rumors:** Terra Drone released defense-product and strategy information around the filing (rank 5); T-Gaia (rank 12) was the subject of restructuring speculation.
5. **Sector themes, preexisting trends, and unresolved triggers:** COVID mask demand (Koken, rank 10), physical-AI stock sympathy (RSC, rank 6), prolonged rally (Succeed, rank 9), and cryptocurrency-stock speculation (S-Science, rank 16). These require greater care in attributing individual causes.

## Methodological implications

- Legitimate large returns are not return-measurement errors. We should retain them in the baseline unless a documented data issue requires correction.
- The primary new-sentiment slope changes from +0.2453 (p=0.030) to +0.1411 (p=0.126) excluding all 96, but this is a sensitivity analysis, not proof that these specific events causally drive the estimate.
- The main test of slope differences, new minus unchanged, fails to reject equality in the baseline (p=0.157) and the tail sensitivity models.
- A filing-triggered delisting-risk resolution is related to *filing the report* but not necessarily the tone of the MD&A; other releases and market themes are potentially concurrent. This is exactly the distinction between new annual-report language and information reaching investors.
- **Do not** estimate the fraction of contaminated events from this hand-picked top-of-tail review or exclude events post hoc on the basis of whether their news fits the sentiment hypothesis.

## Follow-up for paper

Review the remaining 75 cases if comprehensive assignment is needed, verify adjusted-price and market-model inputs for the largest cases, inspect time-stamped TDnet news, and predefine event overlap before any additional sample-exclusion regression. If these checks hold, add a compact source-backed paragraph to §5.6 and consider an appendix example table. The abstract need not change.
