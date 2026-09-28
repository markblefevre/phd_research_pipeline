# Paper 2 --- Current Status

**Last updated:** 2026-09-28

## Current thesis

The paper asks whether greater NLP sophistication becomes more
economically useful when financial disclosures contain more textually
novel information.

Rather than treating the study as a simple comparison of LMMD, Japanese
Financial BERT, and GPT, the paper now focuses on a conditional
question:

> **Does contextual NLP add more value when financial disclosures
> contain genuinely new information?**

More precisely, the study asks whether the relative economic
informativeness of contextual and generative sentiment measures
increases as disclosure language becomes more textually novel relative
to the firm's prior report.

## What is complete

### Related Work

The Related Work section is now in coherent prose and organized around
seven sections:

1.  Foundations of financial sentiment measurement
2.  Machine learning, contextual models, and domain adaptation
3.  Generative LLMs and financial reasoning
4.  Japanese financial text and closest empirical precedents
5.  Information location and forward-looking disclosure
6.  Textual change, novelty, and disclosure informativeness
7.  Research gap

The old "Overall story arc" planning section has been removed because
the narrative is now embedded in the prose.

### Research gap

The gap is no longer:

> Which sentiment model performs best on Japanese annual reports?

Okada et al. (2025) already makes that framing too weak.

The current gap is:

> **Whether the relative economic usefulness of lexical, contextual, and
> generative sentiment measures depends systematically on textual
> novelty.**

The design therefore shifts from an unconditional comparison of
sentiment technologies to a conditional comparison.

### Key literature positioning

-   **Loughran & McDonald / Henry & Leone:** domain-specific lexical
    methods remain strong benchmarks.
-   **Li (2010):** information location matters; sentiment measurement
    and text selection are separate empirical choices.
-   **Huang et al. (2023):** strong precedent for constructing sentiment
    independently of returns and then validating it economically.
-   **Frankel et al. / Siano:** important contrast because their text
    measures are trained directly on market outcomes.
-   **Suzuki et al. (2023):** Japanese and finance-specific
    language-model adaptation matters, including tokenizer adaptation.
-   **Kato & Goto (2021):** Japanese annual-report tone contains
    information about future firm performance.
-   **Okada et al. (2025):** closest Japanese empirical precedent;
    GPT-4o-mini and Claude 3 Haiku portfolios generate significant
    negative risk-adjusted alphas, while dictionary, DeBERTaV2, and
    Gemini measures do not.
-   **Brown & Tucker / Dyer / Cohen et al.:** textual change and
    persistence provide the foundation for the novelty dimension.
-   **Muslu et al.:** persistent text should not automatically be
    treated as stale or uninformative.

### Bibliography / LaTeX

-   Unicode-related BibTeX problems were substantially cleaned.
-   Acronyms and proper nouns are protected more consistently.
-   Japanese-reference handling is cleaner.
-   Related Work is compiling cleanly in Overleaf.
-   The bibliography is usable, though small metadata/capitalization
    cleanup may still remain.

### Data acquisition and pipeline

The Paper 2 data pipeline has advanced substantially.

-   **Stage 1 --- EDINET acquisition:** operational and effectively
    complete for the current corpus.
-   The canonical filing manifest now contains **37,807 Annual
    Securities Reports**, with **37,807 unique document IDs**, **4,448
    unique EDINET codes**, and **4,448 unique securities codes**.
-   A calendar-year summary initially showed **37,724 unique
    `edinetCode × periodEnd.year` combinations** and **83 apparent
    duplicate firm-years**. Stage 3 later showed that these were
    legitimate reporting-period transitions, mainly fiscal-year-end
    changes, rather than true duplicate reporting periods.
-   Submission-date coverage extends from **2016-09-20 through
    2026-09-18**, while fiscal-period coverage extends from **2015-07-31
    through 2026-06-30**.
-   The damaged/truncated filing manifest was reconstructed from the
    EDINET listing API, validated with the filing-summary utility, and
    then extended through the current scan window.
-   The final manifest row count of **37,807** matches the count of
    downloaded non-empty raw EDINET ZIP files, providing a strong
    corpus-integrity cross-check.
-   Stage 1 is resumable using `download_checkpoint.json`; previously
    downloaded non-empty ZIP files are skipped safely.
-   A standalone filing-summary utility produces filing counts, yearly
    distributions, firm-year counts, duplicate diagnostics,
    missing-field diagnostics, and EDINET field distributions.
-   A standalone manifest-rebuild utility now provides a safe recovery
    path if `filings.csv` is damaged or truncated.
-   The pipeline runner has begun to be modularized: the EDINET stage
    was moved out of the monolithic `run_pipeline.py` into
    `src/pipeline/stages/edinet_download.py`.

The EDINET reference-data utilities from Paper 1 have also been migrated
into the Paper 2 codebase. The current Japanese and English EDINET code
lists can be downloaded reproducibly using dated snapshots, `latest`
aliases, SHA-256 hashes, and a manifest. These code lists are treated as
**current-state reference metadata**, not as historical point-in-time
listing-status data, because using current listing status to filter
historical filings would introduce survivorship bias.

### MD&A extraction

Stage 2 MD&A extraction is now implemented as a reproducible, resumable
pipeline stage.

The production design uses a fast `lxml` path:

1.  open the EDINET ZIP;
2.  select the primary Annual Securities Report XBRL under
    `XBRL/PublicDoc/`;
3.  locate the standardized MD&A text block by local name;
4.  fall back to Japanese anchor-text matching only when necessary;
5.  normalize embedded XHTML to canonical plain Japanese text.

Arelle was tested successfully as a full XBRL-aware parser and remains
useful as a validation/debugging fallback, but the `lxml` approach is
substantially faster and is preferred for batch extraction.

The extraction architecture is now separated into reusable components:

-   `src/mdna_analysis/mdna_extraction.py` --- single-filing XBRL/MD&A
    parser;
-   `src/mdna_analysis/extract_mdna_batch.py` --- reusable
    manifest-driven batch extractor;
-   `scripts/paper2/extract_mdna_batch.py` --- thin command-line
    wrapper;
-   `src/pipeline/stages/mdna_extract.py` --- Stage 2 pipeline adapter.

The batch extractor reads the canonical `filings.csv`, constructs each
expected ZIP path directly without recursively scanning the NAS, writes
canonical text to `data/interim/paper2/mdna/<edinetCode>/<docID>.txt`,
and maintains `data/interim/paper2/mdna/extraction_manifest.csv` with
extraction status, method, matched XBRL tag, text length, and error
diagnostics.

A 25-filing smoke test completed successfully with **25/25
extractions**, no missing ZIPs, no parser failures, and all observations
extracted through the standardized XBRL MD&A tag rather than fallback
matching. Two manually inspected outputs contained the expected
management discussion content and no obvious cover-page, audit-report,
or unrelated-section contamination.

The refactored batch implementation was then regression-tested against
the existing extraction manifest: all 25 prior successful outputs were
recognized and skipped correctly, confirming resumability.

Stage 2 has now been wired into `run_pipeline.py` through the modular
pipeline adapter and successfully tested through the normal pipeline
entry point.

The full **37,807-filing Stage 2 extraction is now complete and
QC-validated**.

Final extraction results are:

-   **37,757 successful MD&A extractions** out of 37,807 filings;
-   **99.8677% extraction success rate**;
-   **37,752** successful extractions using the standardized primary
    XBRL MD&A tag;
-   **4** successful extractions using the conservative Japanese
    anchor-text fallback;
-   **1** successful manual iXBRL extraction for document `S100QGPT`
    (Japan Aqua, E30126);
-   **50 remaining failures**.

The fallback logic was tightened after manual review showed that score-1
anchor matches could produce false positives. The production fallback
now requires an anchor score of at least 3. The four surviving fallback
cases were manually reviewed and found to contain genuine MD&A content.

The Japan Aqua FY2022 filing (`S100QGPT`) provided an unusual tagging
case. The MD&A was clearly present in section 3 of the human-readable
iXBRL HTML but was not enclosed in either of the expected standardized
MD&A TextBlock concepts in the consolidated XBRL instance. Rather than
weakening the general parser to accommodate a single anomalous filing,
the MD&A was recovered through an explicit one-off `manual_ixbrl`
extraction with provenance recorded in the extraction manifest.

Final Stage 2 QC confirms:

-   Stage 1 filings rows: **37,807**
-   Stage 2 manifest rows: **37,807**
-   successful extractions: **37,757**
-   failed extractions: **50**
-   missing Stage 2 manifest rows: **0**
-   extra Stage 2 manifest rows: **0**
-   duplicate Stage 1 document IDs: **0**
-   duplicate Stage 2 document IDs: **0**
-   output-file issues among successful rows: **0**

Successful MD&A text length has a median of approximately **6,216
characters**, with the 1st and 99th percentiles at approximately **971**
and **20,936** characters respectively. There are **29 short texts below
500 characters** and **37 long texts above 50,000 characters**; these
are review flags rather than automatic exclusion criteria.

Stage 2 should now be treated as **complete and frozen**. Generated MD&A
text files and extraction manifests are pipeline artifacts and are not
committed to Git; the extraction logic, QC utility, configuration, and
explicit manual-recovery script are version controlled.

### Longitudinal matching

Stage 3 --- longitudinal matching --- is now implemented, validated, and
should be treated as **complete and frozen**.

The stage combines the canonical Stage 1 filing metadata with successful
Stage 2 MD&A extractions and works directly with actual reporting-period
start/end dates. An initial implementation defined firm-years using
`periodEnd.year`, which incorrectly treated legitimate fiscal-year-end
changes as duplicate firm-years. Manual review showed that the 83
apparent duplicate groups were typically consecutive reporting periods
ending in the same calendar year, often a normal annual period followed
by a shortened transition period.

The production Stage 3 logic therefore:

1.  joins successful Stage 2 MD&A observations to Stage 1 filing
    metadata;
2.  orders filings within each EDINET issuer by actual reporting period;
3.  identifies only true duplicate reporting periods using
    `edinetCode + periodStart + periodEnd`;
4.  constructs adjacent within-firm reporting-period pairs;
5.  flags whether reporting periods are contiguous;
6.  classifies contiguous pairs as standard annual or transition-period
    pairs based on reporting-period duration;
7.  retains noncontiguous observations separately for audit rather than
    silently treating them as year-over-year pairs.

Final Stage 3 results are:

-   **37,757 matched panel rows**, exactly matching the number of
    successful Stage 2 extractions;
-   **4,441 matched EDINET codes**;
-   **0 true duplicate reporting-period groups**;
-   **33,316 adjacent reporting-period pairs**;
-   **33,046 standard annual pairs**;
-   **268 contiguous transition-period pairs**;
-   **2 noncontiguous pairs**.

The two noncontiguous cases were manually investigated and found to
reflect genuine issuer/listing discontinuities rather than matching
failures:

-   **SBI Shinsei Bank** --- gap associated with its 2023 delisting;
-   **Sony Financial Group** --- gap associated with Sony's 2020 full
    acquisition / privatization and later 2025 relisting through the
    partial spin-off.

These checks provide strong validation that Stage 3 is identifying
genuine longitudinal relationships rather than forcing observations into
artificial calendar-year buckets.

Canonical Stage 3 outputs are written under:

``` text
data/interim/paper2/longitudinal/
    longitudinal_panel.csv
    duplicate_reporting_periods.csv
    adjacent_period_pairs.csv
    standard_annual_pairs.csv
    transition_period_pairs.csv
    noncontiguous_pairs.csv
    summary.json
```

The baseline Stage 4 novelty analysis should begin from the **33,046
standard annual pairs**. Transition-period pairs should be retained as a
diagnostic or robustness sample rather than automatically discarded.

A final Stage 3 sample-eligibility check now makes the domestic-company
research universe explicit using the historical EDINET filing `formCode`
attached to each filing rather than current issuer-listing metadata.

The domestic Annual Securities Report form codes used for eligibility
are:

``` text
030000
030200
040000
```

Foreign-company Annual Securities Reports (`formCode = 080000`) are
outside the target research universe.

This rule is applied at the Stage 3 → Stage 4 boundary rather than
inside the extractor. Stage 1 therefore preserves the complete filing
universe, Stage 2 remains an extraction stage, and Stage 3 retains
mechanical longitudinal matching before defining the research-eligible
pair manifest.

The eligibility check confirms:

-   **37,757 domestic matched panel rows**;
-   **0 foreign matched panel rows**;
-   **33,046 domestic standard annual pairs**;
-   **0 foreign standard annual pairs**;
-   **33,046 research-eligible pairs**.

Thus, making the domestic-company criterion explicit changes **zero
observations** in the current Stage 4 baseline sample. The 50
foreign-company filings in Stage 1 are exactly the 50 Stage 2 extraction
failures, so the prior sample happened already to be domestic-only; the
new rule makes that property intentional and reproducible rather than
dependent on parser behavior.

The canonical Stage 3 handoff to Stage 4 is now:

``` text
data/interim/paper2/longitudinal/research_eligible_pairs.csv
```

### Stage 4 --- token representation construction

Stage 4 has now been implemented and QC-validated through the
token-representation layer. It begins from the frozen Stage 3
research-eligible pair manifest and constructs the unique document
universe required for novelty measurement.

The Stage 3 research sample contains **33,046 adjacent standard annual
pairs**, corresponding to **37,473 unique MD&A documents**.

Stage 4 is organized into three internal substages:

1.  **4A --- raw Japanese tokenization**
2.  **4B --- numeric normalization**
3.  **4C --- cross-variant quality control**

#### Stage 4A --- raw tokenization

The production tokenizer uses Sudachi with NFKC normalization,
natural-boundary chunking, raw numbers retained, and whitespace-only /
punctuation-symbol-only tokens removed after tokenization.

Three Sudachi split modes are generated from the identical
37,473-document universe:

  Variant             Documents   Total tokens   Mean tokens/document
  ----------------- ----------- -------------- ----------------------
  `sudachi_a_raw`        37,473    113,574,929                3,030.8
  `sudachi_b_raw`        37,473    109,407,286                2,919.6
  `sudachi_c_raw`        37,473    105,993,616                2,828.5

All three variants completed with **37,473/37,473 successful
documents**, no duplicate `(edinetCode, docID)` keys, and no missing
token files.

The expected Sudachi segmentation relationship holds for every document:

``` text
tokenCount(A) >= tokenCount(B) >= tokenCount(C)
```

with **zero A\<B violations** and **zero B\<C violations**.

#### Stage 4B --- numeric normalization

Each raw token variant is deterministically transformed into a
corresponding number-normalized representation:

``` text
sudachi_a_raw -> sudachi_a_num
sudachi_b_raw -> sudachi_b_num
sudachi_c_raw -> sudachi_c_num
```

The final number-normalized representations use semantic numeric
normalization:

``` text
numeric magnitudes        -> <NUM>
percentages               -> <NUM>%
yen-denominated amounts   -> <NUM>円
```

Japanese scale markers such as `万`, `億`, and `兆` are treated as part
of numeric magnitude, while semantically meaningful mixed expressions
such as `3Q`, `1人`, and `100年企業` are intentionally retained.

The transformation remains one-input-token to one-output-token, so raw
and number-normalized token counts remain identical document by
document.

All three numeric variants contain **37,473 documents**, all completed
successfully, and raw-versus-normalized token-count mismatches are
**zero** for A, B, and C.

#### Stage 4C --- QC

Stage 4 now runs an explicit validator over the complete six-variant
output family. The validator confirms:

-   identical 37,473-document universes across all variants;
-   zero duplicate document keys;
-   zero missing token files;
-   consistent raw-source metadata;
-   zero failed or zero-token documents;
-   `A >= B >= C` token-count ordering for raw and numeric variants;
-   exact raw / `<NUM>` token-count equality for each Sudachi mode;
-   internally valid numeric replacement counts.

The QC result is written to:

``` text
data/interim/paper2/tokens/qc_summary.json
```

and currently reports:

``` text
status = passed
documentCount = 37,473
```

Stage 4 token preparation should therefore now be treated as **complete
and validated**.

### Stage 5 --- TF-IDF / cosine textual novelty

Stage 5 is now **complete, validated, and frozen**.

The stage fits corpus-wide TF-IDF on the common **37,473-document**
universe and computes adjacent-period cosine similarity / novelty for
the same **33,046 research-eligible annual-report pairs** across all six
Stage 4 representations.

Final novelty summaries are:

  Variant             Mean novelty   Median novelty
  ----------------- -------------- ----------------
  `sudachi_a_raw`         0.106852         0.088598
  `sudachi_b_raw`         0.109630         0.091106
  `sudachi_c_raw`         0.111810         0.093010
  `sudachi_a_num`         0.049687         0.034892
  `sudachi_b_num`         0.051269         0.036312
  `sudachi_c_num`         0.052434         0.037234

The Sudachi A/B/C variants are nearly identical within the raw and
normalized families, with pairwise correlations around 0.997--0.999. Raw
versus normalized novelty remains strongly related but meaningfully
different: Pearson correlations are roughly 0.88 and Spearman
correlations are around 0.71.

The primary specification is `sudachi_c_num`. The main representation
robustness alternative is `sudachi_c_raw`. Sudachi A/B variants remain
secondary robustness checks.

Stage 5 also produces a representation-independent
`pair_diagnostics.csv` using the exact Stage 3 MD&A character counts. It
contains `prevMdnaLength`, `currMdnaLength`, `lengthRatio`,
`logLengthChange`, and `absLogLengthChange`.

The baseline C-num novelty measure is strongly related to absolute MD&A
length change:

``` text
Pearson corr(novelty, absLogLengthChange)  = 0.797
Spearman corr(novelty, absLogLengthChange) = 0.592
```

The relationship remains material after trimming extreme length changes:

  Sample           Pearson   Spearman
  -------------- --------- ----------
  Full sample        0.797      0.592
  Drop top 1%        0.758      0.580
  Drop top 5%        0.633      0.526
  Drop top 10%       0.491      0.454

Source-text spot checks show that absolute-maximum novelty cases can
reflect large but genuine changes in disclosure scope or structure,
while observations around the 99th percentile generally contain coherent
and economically meaningful textual change rather than extraction
failure.

The empirical treatment is now fixed:

-   keep C-num novelty intact as the main measure;
-   include `absLogLengthChange` as a main control;
-   use signed `logLengthChange` as a robustness specification;
-   rerun key models after excluding the top 5% and top 10% of absolute
    length changes;
-   do not residualize novelty against document length;
-   do not winsorize the baseline novelty measure at this stage.

Final Stage 5 validation confirms **33,046 rows**, **0 duplicate
pairs**, **0 missing diagnostic values**, and exact reproduction of all
six variant summary statistics after code cleanup.

#### Stage 5 visualization and temporal diagnostics

A dedicated Stage 5 visualization/QC layer has now been added so that
descriptive figures are generated reproducibly from the frozen novelty
outputs rather than assembled manually. The plotting stage keeps
computation separate from presentation and writes publication-oriented
and diagnostic figures, together with an annual novelty summary.

The figure set includes the baseline/raw novelty distributions, C-num
versus C-raw comparison, novelty by fiscal year, raw versus normalized
novelty by year, novelty versus absolute log MD&A length change, and
year-by-year novelty distributions.

The annual diagnostics reveal a pronounced 2018 discontinuity. For
fiscal-year 2018 reports, mean C-num novelty is approximately **0.146**
and the median is approximately **0.134**, compared with approximately
**0.052** and **0.038** in 2019. The increase is broad-based rather than
being driven only by extreme observations. Its timing coincides with the
Japanese FSA narrative-disclosure reform effective for fiscal years
ending on or after March 31, 2018, which reorganized MD&A-related
disclosure content and increased the emphasis on management-perspective
analysis. This provides institutional face validity for the novelty
measure, while also identifying 2018 as a regulatory-transition year
that requires explicit robustness treatment.

The current empirical treatment is to retain 2018 in the baseline with
year fixed effects and rerun key specifications excluding the 2018
regulatory-transition observations. A further diagnostic should test how
much of the 2018 novelty spike is explained by contemporaneous MD&A
length changes and whether 2018 remains unusually novel conditional on
length change.

The novelty-versus-length diagnostic is also now treated as a potential
paper/appendix figure rather than only internal QC because the strong
relationship directly anticipates a likely measurement concern.

#### Local SSD scratch design

Stage 4 also exposed a significant infrastructure issue: tens of
thousands of small files are slow to process directly over the NAS. The
canonical corpus remains on the NAS, but high-I/O stages can use a
configurable local SSD scratch directory.

The Stage 4 pipeline therefore distinguishes between:

``` text
canonical logical paths:
data/interim/paper2/...

physical scratch paths:
~/paper2_stage4/...
```

Manifests continue to store canonical repo-relative paths rather than
machine-specific scratch paths.

This design improved Sudachi tokenization throughput dramatically. On
the M1 Max, the best practical worker setting was **8 processes**:

    Workers     Throughput
  --------- --------------
          6   569.1 docs/s
          8   744.3 docs/s
         10   751.1 docs/s

The 8-worker setting captures essentially all available speedup while
avoiding unnecessary process overhead. Future stages that perform heavy
small-file I/O should reuse the same canonical-NAS / local-scratch
pattern.

### Stage 6A --- analysis-panel foundation

Stage 6A is now **complete and validated**. It begins from the frozen
Stage 3 research-eligible pair manifest and creates the canonical
pair-level analysis-panel foundation without recomputing upstream text
measures.

The stage performs strict one-to-one joins on
`edinetCode + prev_docID + curr_docID` and incorporates:

-   baseline `sudachi_c_num` cosine similarity and textual novelty;
-   robustness `sudachi_c_raw` cosine similarity and textual novelty;
-   `prevMdnaLength`, `currMdnaLength`, `lengthRatio`,
    `logLengthChange`, and `absLogLengthChange`.

The canonical output is:

``` text
data/interim/paper2/analysis/analysis_panel.csv
```

Final Stage 6A validation confirms **33,046 rows × 41 columns**, exactly
preserving the frozen research sample. All nine joined Stage 5 novelty
and length-diagnostic fields have **zero missing values**. The resulting
C-num and C-raw novelty means and medians exactly reproduce the frozen
Stage 5 summaries.

Stage 6A should therefore be treated as **complete and frozen as the
initial analysis-panel foundation**. Sentiment measures and later
market-reaction variables should be constructed independently and joined
to this foundation rather than embedded in the upstream novelty stages.

### Stage 6B --- LMMD lexical sentiment benchmark

Stage 6B is **complete and frozen**. The LMMD lexical benchmark includes
dictionary/token compatibility QC, full-sample document scoring,
distribution/time-series diagnostics, and a focused investigation of the
FY2017→FY2018 structural break. The implementation uses the translated
Paper 1 LMMD resource against the validated Stage 4 `sudachi_c_raw`
corpus.

Key dictionary results:

-   **86,553** total LMMD rows;
-   **2,692** sentiment-bearing source rows: 347 positive and 2,345
    negative;
-   **2,689 / 2,692 (99.89%)** sentiment-bearing rows have Japanese
    `GPT_JA` translations;
-   **1,827** unique translated Japanese sentiment terms;
-   **582** Japanese translations correspond to multiple English source
    entries;
-   one positive/negative translated-term collision: `決定的に`.

The three untranslated sentiment-bearing entries are `AVERSELY`,
`BRIBERIES`, and `CLAIMING`. They are intentionally left untranslated
rather than manually imputed after observing the corpus. Diagnostic
searches found negligible plausible usage for the first two; `CLAIMING`
is semantically ambiguous in Japanese financial text, with `請求`
occurring 1,122 times but often carrying ordinary claims/billing
meanings rather than unambiguously negative sentiment.

Corpus compatibility is strong at the document level. QC read all
**37,473** Stage 4 C-raw documents, found **0 missing token files**, and
observed exactly **105,993,616 tokens**. Of 1,827 unique translated
sentiment terms, 466 occur in the corpus (**25.51% dictionary-term
coverage**), while **99.9546% of documents contain at least one
sentiment hit**. Positive and negative document coverage are
**99.6798%** and **99.7331%**, respectively. The corpus contains
**1,230,083 positive hits** and **1,094,758 negative hits**, with a
median of **56 sentiment hits per document**.

Manual inspection of frequent matches identified both plausible
financial sentiment terms and expected translation-induced semantic
broadening, including `BREAKDOWN -> 内訳`, `CONFINES -> 領域`,
`EXCEPTIONALLY -> 特に`, and `PERSISTENT -> 持続的`. These are
documented as limitations of the translated lexical benchmark rather
than corrected post hoc, avoiding corpus-driven dictionary tuning and
preserving continuity with Paper 1.

The LMMD compatibility QC is therefore treated as **passed**. Production
scoring reuses the validated Stage 4 `sudachi_c_raw` tokens rather than
retokenizing raw MD&A text independently.

Production LMMD scoring has now completed for all **37,473** unique
research-universe documents and exactly reproduces the standalone QC
totals. The full sample contains **1,230,083 positive hits** and
**1,094,758 negative hits**, with mean `lmmdNet = 0.000992` and median
`lmmdNet = 0.001538`. The production output is keyed by
`edinetCode + docID` and retains token counts, positive/negative counts
and rates, and net sentiment.

Reproducible LMMD distribution and fiscal-year diagnostics reveal a
pronounced FY2017→FY2018 break. Mean LMMD sentiment changes from
approximately **−0.00536** in FY2017 to **+0.00298** in FY2018; the
median changes from approximately **−0.00442** to **+0.00361**.

A standalone matched-firm diagnostic confirms that this is not a
sample-composition artifact. Among **3,362 firms** observed once in both
years, mean within-firm sentiment changes by **+0.00833**, median
sentiment changes by **+0.00756**, and **84.98%** of firms become more
positive. Positive-word incidence rises by **0.00371** on average while
negative-word incidence falls by **0.00461**. Median log token-count
change is approximately **1.028** (roughly **2.8×** the prior token
count), but the correlation between sentiment change and log token-count
change is only **0.239**.

Term decomposition shows that `実績` (positive) and `減少` (negative)
explain much of the measured break. In document-level leave-one-term-out
tests, excluding both reduces the mean within-firm FY2017→FY2018 change
from **0.00833** to **0.00230**, a reduction of approximately **72%**.
The median change remains **+0.00192**, and **67.67%** of firms still
become more positive. The break is therefore broad-based but
substantially amplified by two context-insensitive lexical
classifications.

The LMMD dictionary will **not** be edited post hoc. Preserving the
Paper 1 specification avoids corpus-driven tuning and makes the observed
behavior an informative limitation of the lexical benchmark rather than
something optimized away. The planned empirical treatment is to retain
year fixed effects, include an FY2018-exclusion robustness
specification, and test whether Japanese Financial BERT and GPT exhibit
a comparable 2018 discontinuity. This diagnostic directly strengthens
the motivation for comparing context-insensitive and contextual
sentiment methods.

### Stage 6C --- Japanese Financial BERT contextual sentiment

Stage 6C is now **complete, full-corpus scored, diagnostically validated, canonicalized, and frozen for downstream analysis**. Model development is deliberately separated from routine pipeline inference: chABSA preparation and fine-tuning produce frozen model artifacts, while the production pipeline consumes those frozen checkpoints to score the same **37,473-document** research universe used by Stage 4 and Stage 6B.

The contextual backbone is `izumi-lab/bert-base-japanese-fin-additional`. Following the dual-binary design aligned with Nakatsuka & Suimon (2024), two independent sentence classifiers are fine-tuned on chABSA:

1. positive opinion present / absent;
2. negative opinion present / absent.

A sentence may therefore be positive, negative, both, or neither rather than being forced into a single mutually exclusive sentiment class.

The chABSA preparation step uses **230 Annual Securities Report annotation files** containing **6,119 sentences**. The prepared labels contain **2,210 positive sentences** and **1,746 negative sentences**, with joint states of 1,397 positive-only, 933 negative-only, 813 both, and 2,976 neither. A duplicated outer/inner copy in the downloaded Kaggle package was verified byte-for-byte with SHA-256; only one 230-file copy is used.

The fixed split is joint-stratified across the four joint label states:

  Split          Sentences
  ------------ -----------
  Train              4,895
  Validation           612
  Test                 612

Fine-tuning uses maximum sequence length 512, learning rate `5e-5`, weight decay `0.01`, 100 warmup steps, training batch size 16, evaluation batch size 32, seed 42, and a maximum of 10 epochs. Only the classification head and final BERT encoder layer are trainable (**7,089,410 / 110,618,882 parameters**). The selected model for each binary task is the checkpoint with minimum validation loss rather than automatically the final epoch.

Held-out test performance is:

  -----------------------------------------------------------------------
  Classifier     Accuracy   Precision   Recall           F1   Weighted F1
  ------------ ---------- ----------- -------- ------------ -------------
  Positive         0.9526      0.9251   0.9459   **0.9354**        0.9527

  Negative         0.9461      0.9277   0.8800   **0.9032**        0.9456
  -----------------------------------------------------------------------

Production scoring re-reads the original Stage 2 MD&A text and applies the BERT model's own Japanese tokenizer; Stage 4 `sudachi_c_raw` is used only to define the exact frozen document universe. MD&A is segmented into source sentences. Source sentences exceeding the model limit are split into model pieces, scored, and reaggregated before thresholding so that an overlong source sentence still counts once in the document-level measure.

The primary document score is:

``` text
bertNet = (positiveSentenceCount - negativeSentenceCount) / sentenceCount
```

The output also preserves positive/negative sentence rates, positive-only/negative-only/both/neither counts, mean positive and negative probabilities, `bertProbabilityNet`, model-piece counts, overlong-sentence diagnostics, and unknown-token rates.

End-to-end smoke testing on 25 real MD&A documents showed sensible sentence-level aggregation, meaningful positive and negative document scores, and near-zero unknown-token rates. A subsequent **250-document validation sample** produced mean `bertNet = 0.0277`, median `0.0248`, standard deviation `0.0792`, and a range from approximately `-0.227` to `+0.367`.

As a non-tuning diagnostic, the same 250 documents were compared with the frozen LMMD scores. The measures show moderate agreement rather than redundancy: **Pearson correlation 0.444**, **Spearman correlation 0.472**, and **72.8% sign agreement** among documents with nonzero scores under both methods. The largest standardized disagreements include documents for which BERT and LMMD assign opposite sentiment signs. These cases are reserved for later qualitative validation; the comparison was not used to alter or tune either sentiment measure.

Production inference is wired into `run_pipeline.py` as Stage 6C and uses the frozen positive and negative checkpoints, `threshold = 0.5`, `inference_batch_size = 32`, automatic hardware selection, and resumable block-level output. Full production inference completed successfully for the complete **37,473-document** research universe.

The canonical frozen Stage 6C artifacts are:

``` text
data/interim/paper2/sentiment/financial_bert/
    financial_bert_sentiment.csv
    financial_bert_sentiment.metadata.json
```

Hardware-specific production directories are no longer part of the active pipeline. The canonical output is the validated CUDA production result, while an independent full-corpus Apple MPS run was used only as an implementation-reproducibility check.

The independent MPS and CUDA runs contained identical **37,473-document** universes and identical preprocessing, sentence counts, token diagnostics, and document construction. Continuous sentiment probabilities differed only at numerical floating-point precision (approximately `1e-8` to `1e-7`). Only one document exhibited a one-sentence threshold classification difference near the `0.5` decision boundary. Corpus-level sentiment distributions were unchanged. This check therefore provides strong evidence that Stage 6C results are not materially hardware-dependent; it is retained as implementation validation rather than as part of the paper's substantive empirical narrative.

Full-sample diagnostics show that BERT and LMMD are related but nonredundant: across all 37,473 documents, Pearson `corr(bertNet, lmmdNet) = 0.4031` and Spearman `= 0.4445`; using `bertProbabilityNet` gives Pearson `0.4118` and Spearman `0.4513`.

The FY2017→FY2018 comparison differs sharply across methods. Mean LMMD rises from approximately **−0.00536** to **+0.00298**, while mean BERT falls from approximately **+0.08722** to **+0.05747**. Among **3,362 matched firms**, mean within-firm BERT change is **−0.03091**, median change is **−0.02350**, and **55.92%** become less positive.

A pre-specified diagnostic on all **33,046 research-eligible pairs** examined whether BERT/LMMD measurement disagreement varies with frozen C-num textual novelty. With `absLogLengthChange` and fiscal-year fixed effects, the novelty coefficient is **0.912 (t = 6.65)**; excluding FY2018 it remains **0.879 (t = 5.25)**.

Directional disagreement behaves differently. Raw sign disagreement is **26.20%** and declines with novelty. A centered standardized-magnitude diagnostic shows that much of this is concentrated among relatively weak signals. Requiring both centered standardized magnitudes to exceed **0.50** reduces sign disagreement to **15.04%**, with rates broadly stable across novelty deciles (approximately **13.7%--16.3%**).

The frozen diagnostic conclusion is therefore narrow: **greater textual novelty is associated with somewhat greater divergence in measured sentiment intensity, while directional classification remains broadly consistent when both sentiment signals are sufficiently strong**. No BERT or LMMD parameters, thresholds, dictionary terms, or model-selection decisions were changed in response to these diagnostics.

Stage 6C should now be treated as **complete, reproducible, canonicalized, and frozen**.

## Current hypothesis structure

### H1 --- Conditional contextual advantage

**The relative economic informativeness of contextual and generative
sentiment measures, compared with a domain-specific lexical measure,
increases with textual novelty.**

This is now the core hypothesis and maps directly to the research gap.

### H2 --- Novelty and market relevance

**Sentiment measured in more textually novel disclosure is more strongly
associated with market reactions than sentiment measured in more
persistent disclosure.**

This is a supporting hypothesis rather than the main contribution.

### Secondary / robustness analyses

The following should not be headline hypotheses unless further
literature development justifies them:

-   year-over-year sentiment innovation
-   model rankings across full / novel / persistent text
-   numerical-change robustness
-   alternative novelty definitions

Model-ranking changes should be treated as an empirical implication of
H1 rather than as a separate hypothesis.

## Preliminary novelty sniff tests

Before formalizing Stage 4 novelty measurement, consecutive-year MD&A
disclosures were examined for three large Japanese firms with very
different business models: Toyota, MUFG, and Sony.

The purpose was not to establish the final novelty methodology, but to
determine whether simple year-over-year textual similarity produces
economically interpretable variation before committing to full-sample
implementation.

A rough TF-IDF cosine similarity measure based on Japanese character
3--5-grams was applied to successive MD&A disclosures. Both raw text and
a version with numerical strings normalized were examined.

Several preliminary lessons emerged:

-   **Year-over-year textual novelty is clearly present and economically
    interpretable.** Large similarity breaks often correspond to
    identifiable events such as COVID-related disruption,
    accounting-regime changes, reporting-segment reorganizations, or
    major changes in business perimeter.
-   **Numerical changes materially affect measured similarity.**
    Normalizing numbers substantially increases similarity in many
    firm-years, especially for highly quantitative disclosures. Raw
    novelty and linguistically normalized novelty therefore capture
    related but distinct concepts and should both be retained during
    methodological development.
-   **Baseline textual persistence differs substantially across firms
    and industries.** Toyota exhibits relatively stable but visibly
    changing operational MD&A; MUFG shows greater annual structural and
    financial-statement variation; Sony contains unusually persistent
    accounting and valuation language, with normalized similarity often
    close to one.
-   **Whole-document similarity can be dominated by persistent
    boilerplate.** Sony provides a particularly clear example:
    economically meaningful changes can occur within an MD&A whose large
    accounting-policy sections remain almost unchanged.
-   **Structural disclosure changes can generate apparent novelty that
    is not purely economic information.** Examples include
    accounting-standard changes, segment reorganizations, and changes in
    consolidation perimeter. These cases should be diagnosed rather than
    automatically treated as errors.
-   **Novelty and sentiment appear conceptually distinct.** Large
    textual changes can accompany either deterioration or improvement in
    business conditions, supporting the planned use of novelty as a
    conditioning variable rather than a directional sentiment measure.

These observations strengthen the motivation for including **industry
fixed effects** in the empirical specification. They also suggest that
absolute textual novelty may not be directly comparable across all
industries because normal disclosure persistence appears to differ
systematically by business type.

Accordingly, Stage 4 should preserve a simple absolute novelty measure
as the baseline while also retaining the possibility of robustness
specifications based on:

-   numerical normalization;
-   industry-relative novelty;
-   firm-relative novelty where sufficient longitudinal history exists;
-   alternative treatment of persistent versus changed portions of the
    MD&A;
-   explicit flags for major accounting, segment, or
    disclosure-structure changes.

The three-firm exercise should be treated as a methodological diagnostic
rather than evidence for the paper's hypotheses.


### Stage 7 --- Market-reaction construction

Stage 7A --- market-event eligibility and price coverage --- is **implemented, validated, complete, and frozen**. It begins from the frozen **33,046 research-eligible filing events** and determines whether each event has sufficient Tokyo Stock Exchange price history to support the Paper 1 event-study architecture.

The retained baseline design is:

``` text
estimation window = [-120, -20] trading days
minimum usable estimation observations = 60
market benchmark = TOPIX
```

Raw J-Quants Stock Prices (OHLC) data are stored under:

``` text
data/raw/paper2/prices/
```

The archive contains 136 compressed daily-price files and covers September 2016 through September 2026.

Final Stage 7A eligibility is:

| Classification | Events |
|---|---:|
| Eligible for market-reaction construction | **32,134** |
| Non-TSE regional-exchange security | **814** |
| Not yet in TSE price universe at event | **57** |
| Insufficient post-listing estimation history | **15** |
| TSE delisted before event | **26** |
| **Total research-eligible events** | **33,046** |

Thus **32,134 / 33,046 = 97.24%** of the frozen research sample survives Stage 7A.

The **814** regional-exchange exclusions correspond to securities listed on the Nagoya, Fukuoka, or Sapporo exchanges rather than the TSE. Historical exchange information from EDINET filings was used to reconcile these observations. The **72** observations initially classified as having insufficient historical price coverage were investigated individually and resolve to **57** events occurring before the security entered the TSE price universe and **15** events with insufficient post-listing estimation history. The **26** observations with no price on or after the filing date were reconciled to securities already delisted from the TSE before the event.

The canonical Stage 7A outputs are:

``` text
data/interim/paper2/market_reaction/
    stage7_event_eligibility.csv
    stage7_exclusion_audit.csv
    stage7_eligibility_summary.json
    diagnostics/
```

Stage 7A is wired into the production pipeline through:

``` text
src/market_reaction/event_eligibility.py
src/pipeline/stages/market_reaction_eligibility.py
```

and should be treated as **complete and frozen**.

#### Stage 7B --- timestamp-aware event trading dates

Stage 7B is now **implemented, validated, complete, and frozen**. It takes the **32,134 Stage 7A-eligible events** and assigns the canonical trading date using the EDINET submission timestamp and the observed TSE trading calendar derived from the J-Quants price archive.

The event-date rule is:

``` text
trading-day filing at or before market close -> same trading day
trading-day filing after market close        -> next TSE trading day
non-trading-day filing                       -> next TSE trading day
```

The applicable TSE cash-market close is:

``` text
through 2024-11-01   15:00 JST
from 2024-11-05      15:30 JST
```

The production implementation uses the global union of J-Quants trading dates rather than security-specific dates so that individual-security suspensions or missing observations cannot be misclassified as exchange holidays.

Stage 7B produces:

``` text
data/interim/paper2/market_reaction/
    stage7_event_dates.csv
    stage7_event_dates_summary.json
```

The final validated Stage 7B results are:

| Event-date rule | Events |
|---|---:|
| Same trading day at or before close | **21,980** |
| Next trading day after close | **10,154** |
| Next trading day from non-trading submission date | **0** |
| **Total** | **32,134** |

Thus approximately **68.4%** of eligible filings are assigned to the filing-day trading session and **31.6%** roll to the next TSE session because they were submitted after the close.

The derived trading calendar contains **2,457 TSE sessions** from **2016-09-01 through 2026-09-25**. All 32,134 eligible EDINET submission dates fall on observed TSE trading dates, so no baseline observation requires the non-trading-date rule. Event-date shifts range from 0 to 11 calendar days. The five 11-day shifts correspond to after-close filings on April 26, 2019 followed by the extended Golden Week / imperial-succession exchange closure, providing a useful calendar sanity check.

A dedicated cutoff-boundary QC confirmed that the regime-specific close rule is mechanically consistent. Filings stamped exactly at the close are assigned to the same trading day, including observed post-November-2024 filings at exactly **15:30:00**. The QC reports zero rule mismatches.

The Stage 7B production implementation is:

``` text
src/market_reaction/event_trading_dates.py
src/pipeline/stages/market_reaction_event_dates.py
```

Stage 7B should therefore be treated as **complete and frozen**.


#### Stage 7C --- TOPIX market-return preparation

Stage 7C is now **implemented, validated, complete, and frozen**.

The production stage acquires official daily TOPIX OHLC data from the current J-Quants v2 API and stores the raw benchmark series separately from the event-study outputs:

``` text
data/raw/paper2/market/topix_daily.csv
```

The canonical processed market-return series is:

``` text
data/interim/paper2/market_reaction/topix_returns.csv
```

with QC metadata at:

``` text
data/interim/paper2/market_reaction/topix_returns_summary.json
```

The retained columns are `tradingDate`, TOPIX OHLC, `topixReturnSimple`, and `topixReturnLog`.

The J-Quants subscription currently provides TOPIX history from **2016-09-28** onward. The final TOPIX series contains **2,440 observations** from **2016-09-28 through 2026-09-25**, with **0 missing close values** and exactly one missing simple/log return at the first observation, as expected.

Coverage against Stage 7B is complete:

- Stage 7B eligible events: **32,134**
- unique Stage 7B event trading dates: **1,341**
- missing TOPIX event dates: **0**
- events with at least 60 TOPIX observations in `[-120,-20]`: **32,134**
- events failing the TOPIX estimation-history requirement: **0**
- TOPIX observations in the inclusive `[-120,-20]` window for every event: **101**

Thus the subscription-history cutoff is empirically irrelevant for the retained event sample, and no supplemental pre-2016-09-28 TOPIX data are required.

The Stage 7C production implementation is:

``` text
src/market/topix.py
src/pipeline/stages/market_reaction_topix.py
```

Stage 7C should therefore be treated as **complete and frozen**.

The next task is **Stage 7D --- event-specific market-model estimation**.


The next market-reaction task is **Stage 7C --- TOPIX ingestion and market-return preparation**. Stages 7D--7F will then estimate event-specific market models, calculate abnormal returns/CARs, and produce the final market-reaction table and QC.


## Open empirical decisions

The main remaining design decisions are:

-   whether sentence-level or changed-text analyses are needed beyond
    the frozen document-level baseline;
-   how to define persistent versus novel portions of text in secondary
    analyses;
-   whether grouped novelty portfolios/bins add value beyond the planned
    continuous interaction;
-   how LMMD, Japanese Financial BERT, and GPT sentiment are normalized
    for comparison;
-   exact market-reaction window;
-   the remaining control set and fixed-effects structure;
-   which robustness analyses are sufficiently informative to include in
    the final paper;
-   whether firm- or industry-relative novelty transformations improve
    interpretation beyond the absolute baseline.

## Immediate next steps

1. **Stage 7D --- estimate event-specific market models.**
   - Preserve the Paper 1 `[-120,-20]` estimation window and minimum 60-observation rule.
   - Confirm the Paper 1 stock-return convention before freezing the Paper 2 implementation.
2. **Stages 7E/7F --- abnormal returns, CARs, final market-reaction table, and QC.**
3. **Stage 6D --- GPT / generative sentiment.**
4. **Then finalize the empirical specification, robustness suite, and paper-writing tasks.**

## Current bottleneck

The literature review, MD&A extraction, longitudinal matching, baseline novelty construction, LMMD scoring, Financial BERT scoring, Stage 7A market-event eligibility, and Stage 7B timestamp-aware event-date construction are no longer the primary bottlenecks.

The bottleneck has shifted to:

> **constructing market-adjusted return outcomes and integrating them with the frozen novelty and sentiment measures in a regression-ready panel**

Stages 1--5 are complete through the baseline word-token novelty layer. Stage 6A is complete and frozen, Stage 6B LMMD is complete and frozen, and Stage 6C Financial BERT is complete, canonicalized, and frozen. Stages 7A, 7B, and 7C are complete and frozen, leaving **32,134** eligible filing events with validated timestamp-aware `eventTradingDate` assignments and complete TOPIX benchmark coverage. The immediate bottleneck is now event-specific market-model estimation and CAR construction through Stages 7D--7F. Stage 6D GPT sentiment remains intentionally deferred while market-reaction construction proceeds.

Further literature review should now be driven primarily by unresolved methodological or theoretical questions that emerge from the empirical work rather than by broad literature searching.

## Rough completion estimate

**Overall paper:** approximately 62--66%

Approximate status by component:

-   Related Work / research gap: 80--85%
-   Research question / hypotheses: 65--75%
-   Empirical design: 60--65%
-   Data acquisition / reference-data infrastructure: 97%
-   Coding / pipeline adaptation: 95%
-   MD&A extraction core / batch pipeline: 100%
-   Longitudinal matching: 100%
-   Novelty implementation: 90--95%
-   Main results: 5--15%
-   Robustness tests: 0%
-   Final Introduction / Abstract / Conclusion: 20--30%
-   Final tables / polishing / submission readiness: 10--20%
