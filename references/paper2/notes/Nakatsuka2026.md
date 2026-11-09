# Nakatsuka and Suimon (2026)

## Local filename
Nakatsuka2026HowDoesTheMarketRespond.pdf

## Citation
@article{nakatsuka2026does,
  title={How does the market respond to textual novelty: Joint analysis of similarity and sentiment on Japanese corporate disclosures},
  author={Nakatsuka, Hiromasa and Suimon, Yoshiyuki},
  journal={Economics Letters},
  volume={265},
  pages={113002},
  year={2026},
  doi={10.1016/j.econlet.2026.113002},
  publisher={Elsevier}
}

## Research question
What does the paper ask?

Does the market respond more strongly to the sentiment of semantically novel statements than to the sentiment of statements resembling a firm's prior disclosures? Specifically, for Japanese financial results briefings, do sentiments measured separately in low-similarity ("novel" or "dissimilar") and high-similarity ("sticky" or "similar") sentences have different associations with post-event abnormal stock returns? The authors hypothesize that the sentiment of dissimilar sentences has greater market impact than the sentiment of similar sentences (p. 2).

## Data
The authors study transcripts of Japanese firms' financial results briefings obtained from logmi Finance, with coverage extending through October 18, 2024. These are firm-specific, nonmandatory oral presentations following earnings announcements, **not** statutory EDINET Annual Securities Reports or annual MD&A filings. They compare each briefing with the same firm's preceding briefing to measure sentence-level similarity (pp. 1–2).

The sample excludes financial and banking firms under the TOPIX 17-sector classification. A briefing is linked to a financial statement when its event date occurs within 15 days after the corresponding earnings announcement. The authors require contemporaneous and prior-year financial fundamentals, stock returns covering trading days [−130, +10] relative to the briefing event, and an earlier briefing for textual comparison; the first briefing for each firm is removed. After these restrictions, the sample contains **3,045 briefing events from 703 firms** (p. 2).

Firms are separated by a market-capitalization threshold of ¥100 billion: **1,634 large-cap events** and **1,411 small-cap events** (p. 2; Table 3, p. 4). Accounting controls include year-over-year sales growth and the change in operating margin, along with log market capitalization and price-to-book ratio. The sales-growth and margin-change variables are winsorized at the 1st and 99th percentiles. The article does not state a starting calendar date for the transcript sample (p. 2).

## Methods
**Semantic similarity and novelty.** The authors encode Japanese sentences using the GLuCoSE Japanese Sentence-BERT model (footnote 2, p. 2). For each sentence in the current briefing, they calculate its **maximum cosine similarity** to any sentence in the preceding briefing. Within each current transcript, they then divide sentences at that transcript's **median similarity** into a high-similarity ("similar"/"sticky") group and a low-similarity ("dissimilar"/"novel") group (Eq. 3, p. 2). This is a *relative, continuous-similarity split*, not a semantic classification into unchanged, revised, and genuinely new propositions. For descriptive graphs, the authors also divide sentences into four similarity-ranked groups (p. 3).

**Sentiment.** They start with the Japanese financial BERT model of Suzuki et al. (2023), `izumi-lab/bert-base-japanese-fin-additional`, and fine-tune it using chABSA annotations of positive and negative business-performance language from Japanese securities reports. For a sentence subset, they average predicted positive and negative probabilities over its sentences and define sentiment as `(average positive probability − average negative probability) / (average positive probability + average negative probability)` (Eq. 4, p. 2). They separately calculate sentiment for the similar and dissimilar subsets. A further differential, ΔS, subtracts the similar-group score from the dissimilar-group score **after scaling each by its cross-event standard deviation** (Eq. 5, p. 2). The paper does not spell out all model fine-tuning or sentence-segmentation parameters.

**Event study.** Abnormal returns are estimated using a **Japanese Fama–French three-factor model** (market, size, and value), with factor exposures fitted over the 120-trading-day interval **[−130, −11]**. The main regression outcome is the five-trading-day, post-event cumulative abnormal return **CAR[0,4]**, where day 0 is the financial results briefing date (pp. 2–3). The authors also inspect daily returns and pre-event/post-event cumulative windows.

**Statistical tests and regressions.** For the event-group comparisons, they sort observations into the top and bottom halves of a given sentiment measure and apply Welch's t-tests to compare returns; this procedure is applied to similar sentiment, dissimilar sentiment, and the standardized sentiment differential. They also regress CAR[0,4] jointly on similar- and dissimilar-sentence sentiment, average transcript similarity, sales growth, margin change, log market cap, and price-to-book ratio, including TOPIX 17-sector industry dummies and year-quarter dummies (Eq. 6, p. 3). Table 3 reports the regressions separately for large- and small-cap firms. The paper does **not** report a formal test of the null hypothesis that the two regression sentiment coefficients are equal; the Welch tests and differential-sentiment sorts should not be mistaken for such a coefficient-equality test.

## Main findings
The central finding is that the sentiment of semantically dissimilar sentences has a stronger reported association with post-briefing returns than the sentiment of similar sentences, particularly among small-cap firms. This is consistent with the authors' stated hypothesis, although the comparative statistical evidence must be interpreted in light of the tests actually reported (pp. 3–4).

**Table 3 (p. 4), regressions for CAR[0,4]:**
- **Large-cap events (N = 1,634):** dissimilar sentiment coefficient **1.6** (SE 0.54; p < 0.01); similar sentiment coefficient **1.1** (SE 0.57; p < 0.05). Both are positive and individually significant. Adjusted R² is **0.037**.
- **Small-cap events (N = 1,411):** dissimilar sentiment coefficient **3.3** (SE 0.92; p < 0.01); similar sentiment coefficient **−1.6** (SE 0.92; p < 0.10). The opposite signs make the distinction especially pronounced in this subsample. Adjusted R² is **0.027**.
- The mean-similarity coefficients are not individually significant in either size subsample. Sales growth and changes in operating margin are included as controls; the margin-change coefficients are significant in both subsamples (Table 3).

**Table 2 (p. 4), positive-versus-negative event-group comparisons:**
- Over **CAR[0,4]**, the Welch t-statistic is **5.51** (p < 0.01) when events are sorted on dissimilar-sentence sentiment, versus **3.53** (p < 0.01) when sorted on similar-sentence sentiment. Sorting on the standardized differential ΔS yields **2.22** (p < 0.05). These are *different event-group tests*, not direct comparisons of the two regression slopes.
- In the **pre-event [−5,−1]** window, similar-sentence sentiment produces a significant t-statistic of **2.63** (p < 0.01), whereas dissimilar-sentence sentiment produces **1.04** (not significant). The authors interpret this pattern as consistent with sticky statements being more readily anticipated or reflected in prices before the briefing.
- In the daily comparisons, the ΔS event sort is significant on trading days **+1** and **+2** (t = **2.50** and **2.21**, respectively), but not on day 0 (Table 2).

**Table 1 (p. 4):** Sentiment from similar sentences is more correlated with contemporaneous fundamental changes than sentiment from dissimilar sentences: correlation with sales growth **0.39 versus 0.24**, and with operating-margin change **0.24 versus 0.17**. This provides a potential explanation for why the similar-sentence component has less incremental explanatory power once fundamentals are controlled for, particularly in the small-cap regression.

**Fig. 1 (p. 3)** shows greater sentiment-score polarization among similar sentences than among dissimilar sentences. **Fig. 2 (p. 3)** plots mean CAR paths across four sentence-similarity groups, and **Fig. 3 (p. 4)** compares sticky/novel and positive/negative sentiment paths for large- and small-cap firms. These figures support the qualitative patterns discussed in the text but do not substitute for tests of differences between regression coefficients.

## Relevance to my paper
This is **one of the closest published precedents** to my EDHEC Paper 2. Both studies use Japanese corporate communications, align current language with prior language using semantic representations, separate sentiment by the persistence or novelty of underlying text, and examine abnormal stock returns. The conceptual overlap is substantial and should be acknowledged directly rather than treated as a peripheral sentiment-model comparison.

However, the questions and designs are not identical. Nakatsuka and Suimon study **nonmandatory financial results briefings** and measure novelty using **low versus high maximum sentence similarity**, split at each transcript's median. My paper studies **statutory annual-report MD&A**, uses candidate sentence alignment followed by **context-aware classification of unchanged, substantively revised, and new propositions**, and estimates the three component-sentiment coefficients jointly. Semantic similarity alone cannot reliably determine whether a highly similar sentence contains an economically important numerical revision, or whether a low-similarity sentence is actually new in substantive meaning. This motivates my finer three-way decomposition; it does not demonstrate that my classification is necessarily more accurate.

The outcome and identification choices also differ. Nakatsuka and Suimon use Japanese Fama–French three-factor abnormal returns and emphasize **post-event CAR[0,4]** around a results briefing; my primary event window is **[−1,+1]** around an **EDINET filing**, with TOPIX market-model abnormal returns. They report industry and year-quarter effects in size-split cross-sectional regressions; my principal specification includes firm and fiscal-year fixed effects and firm-clustered standard errors. Their headline evidence is stronger for novel sentiment, especially in small caps, whereas my main three-component specification finds a positive new-sentiment coefficient but **does not reject equality** of the new and unchanged coefficients or joint equality across all three components.

The most valuable positioning is therefore **direct antecedent plus sharper decomposition and more cautious statistical interpretation**, not a claim to be the first to connect Japanese disclosure novelty, sentiment, and market reactions. My paper extends the application to a much larger longitudinal annual-report setting and directly distinguishes revisions of earlier propositions from wholly new passages. Neither study establishes that language new relative to the selected previous document represents information first becoming available to investors.

## How I might cite it
- Nakatsuka and Suimon (2026) provide direct Japanese evidence that **sentence-level semantic similarity and sentiment can be analyzed jointly** to study market reactions to corporate communications.
- Their main sample consists of **3,045 Japanese financial results briefings from 703 firms**; the disclosure setting is therefore **different from annual MD&A**.
- They classify sentences as **similar or dissimilar relative to the previous briefing**, using a within-transcript median split of maximum semantic similarity. This is not the same as detecting revised versus newly introduced economic propositions.
- They report a **positive, statistically significant coefficient on dissimilar-sentence sentiment** in five-day post-briefing CAR regressions for both large- and small-cap firms; the similar-sentence coefficient is positive for large caps and negative for small caps (Table 3, p. 4).
- They find evidence of pre-event return associations for **similar** sentence sentiment but not for **dissimilar** sentence sentiment in their event-group tests (Table 2, p. 4).
- Their paper motivates separating textual novelty from overall document sentiment, while leaving open whether **substantively revised statements** behave differently from both unchanged and newly introduced passages.
- **Avoid claiming** that they formally reject `β_dissimilar = β_similar` in their regression model: an explicit coefficient-equality test is not reported in the article.

Possible literature-review wording:

> Nakatsuka and Suimon (2026) examine Japanese financial results briefings by combining sentence-level semantic similarity to prior presentations with contextual sentiment measurement. They report a stronger association between the sentiment of dissimilar sentences and post-briefing abnormal returns, particularly among smaller firms. Their similarity-based division distinguishes relatively persistent from relatively novel language but does not separately identify substantive revisions of earlier propositions. We instead analyze regulated annual-report MD&A and distinguish unchanged, revised, and newly introduced language, estimating their sentiment associations jointly and assessing differences using formal coefficient-equality tests.

## Possible literature-review section
Primary: **Section 2.4 — Japanese financial disclosures and related evidence** (directly after the discussion of Okada et al. 2025, or as a bridge to the research-gap subsection).

Secondary: **Section 2.1 — Disclosure persistence, textual change, and market reactions**; **Section 2.2 — Information location and the content of disclosure**. It is less central to Section 2.3 on comparing sentiment technologies because the contribution concerns **where sentiment occurs**, not which sentiment model wins.

## Important quotes / page numbers
- **p. 1, abstract:** The authors state that sentiment in novel, low-similarity statements has stronger explanatory power for post-event returns than sentiment in sticky, high-similarity statements. This is the paper's overarching claim.
- **p. 1, Introduction:** Financial results briefings are described as firm-specific, nonmandatory oral presentations and contrasted with more standardized annual filings; this is critical for explaining the different disclosure setting.
- **p. 2, Section 2.1:** Sample construction, exclusions, required return coverage, and final **3,045 events / 703 firms**. The source ends October 18, 2024; it does not provide a sample start date.
- **p. 2, Section 2.2:** The explicit hypothesis is that sentiment in novel/dissimilar sentences has a greater effect on returns than sentiment in sticky/similar sentences.
- **p. 2, Eq. (3), Section 2.3:** Maximum prior-transcript sentence-embedding cosine similarity measures current-sentence novelty; classification uses the **transcript's median** similarity.
- **p. 2, Eqs. (4)–(5):** Finance-BERT sentiment is constructed from average positive and negative probabilities; ΔS contrasts sentiment components normalized by their standard deviations.
- **pp. 2–3, Section 2.4, Eq. (6):** Japanese Fama–French three-factor event study, estimation window **[−130,−11]**, outcome **CAR[0,4]**, two sentiment components, controls, industry dummies, and year-quarter dummies.
- **p. 3, Fig. 1:** Similar sentences exhibit a more polarized sentiment-score distribution than dissimilar sentences.
- **p. 3, Fig. 2:** Return paths by sentiment and four similarity levels; useful visualization of the timing argument.
- **p. 4, Table 1:** Correlations with sales growth and margin change are higher for similar-sentence sentiment.
- **p. 4, Table 2:** Post-event and pre-event Welch test statistics, including **CAR[0,4]** and **CAR[−5,−1]**; distinguish these group sorts from coefficient-equality tests.
- **p. 4, Fig. 3 and Table 3:** Large/small-cap heterogeneity and numerical regression coefficients on similar versus dissimilar sentiment.
- **p. 4, Conclusion:** The authors identify annual reports and other disclosure types as possible extensions—directly pertinent to positioning my study.
- **p. 5, Data availability:** The authors say they **do not have permission to share the data**.

## Caveats / limitations
**Similarity is not substantive novelty.** A within-transcript median split necessarily creates relatively similar and dissimilar groups even if the overall transcript is largely repetitive or largely new. Maximum semantic similarity can identify close phrasing or paraphrases but does not, by itself, distinguish economic revisions from unchanged statements. Numerical updates, negation, and changes in outlook may remain embedded in highly similar sentences. The article does not provide a manual validation study of the similarity split as a measure of genuinely new information.

**Novelty is relative to one source.** Dissimilar content is new only in comparison with the prior briefing, not necessarily new to the market. Because briefings follow earnings announcements and may occur within 15 days of them, relevant information could already have appeared in earnings releases, exchange filings, forecasts, or news. The paper's price-response evidence is associative, not causal proof of first information arrival.

**No reported formal regression coefficient-equality test.** Table 3 reports separate individual t-tests and coefficient estimates but does not give a test of the difference between the dissimilar and similar sentiment coefficients. Nor does a significant result for one coefficient and an insignificant result for another establish that the coefficients differ. The significant ΔS event-group tests in Table 2 answer a different question. The stronger-novelty conclusion should be cited as the **authors' reported pattern**, not as proof of an explicitly tested slope difference.

**Econometric reporting is concise.** The regression specification includes industry and year-quarter indicators, but the paper does not report firm fixed effects or specify firm-clustered standard errors. The paper does not explain in detail how within-firm dependence or repeated briefing observations affect inference. Adjusted R² values are low (0.037 and 0.027), so the regressions explain relatively little cross-event CAR variation. This does not invalidate the reported associations but limits the strength of broad predictive claims.

**Disclosure and horizon specificity.** The study examines oral, nonmandatory financial results briefings, rather than annual MD&A, and focuses on **CAR[0,4]**, rather than a symmetric three-day filing window. Their evidence should not automatically be generalized to annual-report filing reactions. The differing large- and small-cap signs for similar-sentence sentiment also caution against a single homogeneous interpretation.

**Reproducibility and measurement reporting.** The article is only five published pages and gives limited detail about sentence segmentation, model training hyperparameters, matching failures, and alternative similarity thresholds. The authors explicitly state on p. 5 that they cannot share the data. These are limits on independent replication and on assessing measurement robustness, not proof that any particular step was implemented incorrectly.

**Interpretive caution.** Similar-sentence sentiment correlates more strongly with contemporaneous fundamentals and shows a pre-event return association. These findings are consistent with earlier information incorporation, but they do not uniquely identify that mechanism: other timing, selection, omitted-information, or measurement explanations remain possible. My literature review should acknowledge this important precedent while making clear what its empirical design does and does not establish.
