# Tetlock, Saar-Tsechansky, and Macskassy (2008)

## Local filename
Tetlock, Saar-Tsechansky, and Macskassy (JF 2008).pdf

## Citation
@article{tetlock2008morethanwords,
  title={More Than Words: Quantifying Language to Measure Firms' Fundamentals},
  author={Tetlock, Paul C. and Saar-Tsechansky, Maytal and Macskassy, Sofus},
  journal={The Journal of Finance},
  volume={63},
  number={3},
  pages={1437--1467},
  year={2008},
  doi={10.1111/j.1540-6261.2008.01362.x}
}

## Research question
What does the paper ask?

Can a simple quantitative measure of language in firm-specific financial news predict individual firms' future accounting earnings and stock returns, and do stock prices fully and immediately incorporate the information embedded in that language?

The paper extends Tetlock (2007) from aggregate market sentiment to firm-level news. It asks whether negative language contains incremental information about firms' fundamentals beyond accounting data, analyst forecasts, and recent stock returns; whether investors underreact to that information; and whether predictability is strongest when news stories are explicitly about firm fundamentals.

## Data
The study analyzes firm-specific Wall Street Journal (WSJ) and Dow Jones News Service (DJNS) stories about S&P 500 firms from 1980 through 2004.

The authors retrieve more than 350,000 qualifying stories from Factiva:
- more than 260,000 DJNS stories,
- more than 90,000 WSJ stories,
- covering more than 100 million words.

They identify at least one qualifying story for 1,063 of 1,110 firms that were members of the S&P 500 during the sample period.

Stories must mention the firm's official name within the first 25 words, mention its popular name at least twice, contain at least 50 words, and contain at least five positive or negative dictionary words, of which at least three must be unique.

Financial data come from:
- CRSP for stock returns and S&P 500 membership,
- Compustat for accounting data, and
- I/B/E/S for analyst forecasts.

For earnings tests, the authors aggregate news over the period from 30 to 3 trading days before an earnings announcement.

## Methods
The paper uses a bag-of-words dictionary approach based on the Harvard-IV-4 General Inquirer dictionary, the same general framework used in Tetlock (2007).

The primary textual measure is the fraction of negative words in each firm's news stories. All qualifying stories for a firm on a given day are combined into a single composite story.

The raw negative-word fraction is standardized using the previous calendar year's mean and standard deviation, producing a standardized `neg` variable. This time-varying standardization is intended to reduce problems from changes in news coverage or writing style over time.

Positive words are examined in robustness tests, but negative words provide substantially stronger results, so the paper focuses primarily on negativity.

### Earnings predictability
To test whether language captures fundamentals, the authors aggregate negative words in stories from trading days [-30,-3] before an earnings announcement.

Future earnings are measured using:
- standardized unexpected earnings (SUE), based on a seasonal random-walk benchmark, and
- standardized analyst forecast errors (SAFE).

OLS regressions control for:
- lagged earnings,
- firm size,
- book-to-market,
- share turnover,
- recent and longer-horizon abnormal returns,
- analyst forecast revisions, and
- analyst forecast dispersion.

Standard errors are clustered by calendar quarter.

### Return predictability
The return tests examine whether negative words published on day 0 predict the firm's return on day +1.

For DJNS stories, the authors exclude stories updated after 3:30 p.m. so investors have at least 30 minutes to react before the market close.

Expected returns are benchmarked using the Fama-French three-factor model. Regressions include extensive controls for prior returns, earnings, size, book-to-market, and turnover, with standard errors clustered by trading day.

### Fundamentals interaction
To distinguish fundamental news from other news, the authors create a `Fund` indicator equal to one when a news story contains the word stem `earn`.

They interact `Fund` with negative-word intensity and test whether negativity in earnings-related stories is especially predictive of:
- future earnings,
- contemporaneous returns, and
- next-day returns.

### Trading strategy
The authors also form a daily long-short strategy:
- long firms with relatively positive DJNS stories,
- short firms with relatively negative DJNS stories,
- hold for one trading day,
- rebalance daily.

They evaluate raw and factor-adjusted performance and explicitly analyze the impact of transaction costs.

## Main findings
Negative language in firm-specific news predicts lower future accounting earnings.

The result survives controls for analyst forecasts, recent returns, lagged earnings, and firm characteristics. Negative words therefore contain information about firms' fundamentals that is not redundant with conventional quantitative measures.

The economic magnitude is meaningful. Moving from two standard deviations below to two standard deviations above the mean level of negative news is associated with roughly a 0.255-standard-deviation decline in expected SUE in the authors' full specification.

Negative news also predicts slightly lower stock returns on the following trading day. For DJNS stories, a one-standard-deviation increase in negative words predicts approximately **3.2 basis points lower next-day abnormal returns**.

The return predictability is much weaker and statistically insignificant for WSJ stories. The authors interpret this difference as a timing effect: DJNS stories arrive intraday and contain fresher information, whereas many WSJ stories summarize information that may already have been incorporated into prices.

The underreaction is short-lived. Most of the market response occurs on the initial news day, with a relatively small continuation on the following day.

News about fundamentals is substantially more informative than other news. Negative words in stories mentioning earnings predict both earnings and returns far more strongly than negative words in other stories.

For earnings-related DJNS stories, the contemporaneous market response to negative language is about five times larger than for other stories. The next-day underreaction is also substantially larger for earnings-related stories.

A simple daily DJNS long-short strategy generates large gross returns before costs, but the profitability disappears under reasonable transaction-cost assumptions. The paper therefore does not claim an easily exploitable trading anomaly.

Overall, the authors conclude that linguistic news content captures difficult-to-quantify information about firms' fundamentals and that investors incorporate most—but not all—of this information into prices quickly.

## Relevance to my paper
This paper is a foundational firm-level extension of Tetlock (2007) and is highly relevant to the economic-validation portion of my study.

It shows that a text-based sentiment measure constructed independently of market outcomes can predict both **fundamentals and stock returns**. The negative-word score is created entirely from a predetermined dictionary; earnings and returns are then used as external validation criteria.

This is methodologically important for my study because it differs sharply from approaches such as Frankel et al. and Siano, where text models are trained directly on abnormal returns. Tetlock et al. provide a cleaner example of asking whether an independently constructed linguistic measure contains economically meaningful information.

The paper also demonstrates that economic relevance depends on **what the text is about**. Negative language in stories explicitly related to earnings is far more informative than negativity in other stories. This supports the idea that sentiment alone may be insufficient unless the surrounding informational context is considered.

At the same time, the method is deliberately simple: a bag-of-words count based on a general-purpose Harvard dictionary. Loughran and McDonald (2011) later show that such general dictionaries can misclassify many words in financial contexts. Tetlock et al. therefore provide an important historical benchmark but not necessarily an optimal modern sentiment measure.

The paper also helps distinguish different notions of market relevance. The language predicts fundamentals over quarterly horizons but stock-price underreaction lasts only about one day. Thus, a measure can contain substantial fundamental information even when market inefficiency is small and short-lived.

For my paper, the setting differs because Tetlock et al. analyze English-language financial news rather than issuer-authored Japanese regulatory disclosures. Nonetheless, the external-validation logic is directly relevant.

## How I might cite it
- Firm-specific negative news language predicts lower future accounting earnings and stock returns.
- Textual sentiment can contain incremental information about fundamentals beyond analyst forecasts, accounting data, and recent returns.
- Tetlock et al. (2008) provide early firm-level evidence that independently constructed dictionary sentiment has economic relevance.
- Stock prices incorporate most negative linguistic information quickly but exhibit a small next-day underreaction.
- Negative language in stories about firm fundamentals is substantially more informative than negative language in other stories.
- Linguistic measures can improve forecasts of firm fundamentals even after controlling for the market's own price response to news.
- The study provides an important firm-level extension of Tetlock's (2007) aggregate-media sentiment results.
- The paper's use of a predetermined dictionary makes returns an external validation target rather than a training label.
- Trading profits from linguistic underreaction are highly sensitive to transaction costs.
- Useful contrast for my paper: Tetlock et al. study English media coverage using a general-purpose dictionary, while my study examines Japanese issuer disclosures and compares domain-specific dictionaries with contextual models.

## Possible literature-review section
Primary: Economic validation: sentiment and market outcomes

Secondary: Dictionary-based financial sentiment; firm-specific financial news; investor underreaction; sentiment and fundamentals

## Important quotes / page numbers
- p. 1437 (PDF p. 1) — Abstract: negative words in firm-specific news forecast lower earnings; prices briefly underreact; predictability is strongest in stories focused on fundamentals.
- pp. 1438–1439 (PDF pp. 2–3) — Contribution: the paper extends Tetlock (2007) from aggregate media sentiment to individual S&P 500 firms and tests whether language predicts both cash flows and returns.
- p. 1439 (PDF p. 3) — Interpretation: negative language contains information beyond analyst forecasts and accounting data, and the market incorporates it with a small one-day delay.
- pp. 1440–1441 (PDF pp. 4–5) — Method: the study uses the Harvard-IV-4 General Inquirer negative-word category and emphasizes the transparency, objectivity, and replicability of simple word counts.
- pp. 1441–1443 (PDF pp. 5–7) — Data: more than 350,000 WSJ and DJNS stories about S&P 500 firms over 1980–2004, containing more than 100 million words.
- pp. 1444–1448 (PDF pp. 8–12) — Earnings tests: negative words from days [-30,-3] predict lower SUE and analyst forecast errors after extensive controls.
- p. 1449 (PDF p. 13) — Economic comparison: negative words retain meaningful forecasting power even after controlling for recent stock returns, which already summarize investors' response to news.
- pp. 1452–1454 (PDF pp. 16–18) — Return test: DJNS negative words predict approximately 3.2 bps lower next-day abnormal returns per one-standard-deviation increase in negativity.
- pp. 1456–1460 (approximately PDF pp. 20–24) — Trading strategy: gross returns appear large, but reasonable transaction costs eliminate profitability.
- pp. 1462–1464 (approximately PDF pp. 26–28) — Fundamentals interaction: negative words in earnings-related stories produce much larger contemporaneous and next-day return responses than negativity in other stories.
- pp. 1464–1465 (approximately PDF pp. 28–29) — Conclusion: linguistic media content captures otherwise hard-to-quantify fundamentals, but market underreaction is slight and trading profits are fragile after costs.

## Caveats / limitations
The sentiment measure uses the general-purpose Harvard-IV-4 psychosocial dictionary rather than a finance-specific lexicon. Later work by Loughran and McDonald (2011) shows that general dictionaries can misclassify common financial terminology, so Tetlock et al.'s negative-word measure likely contains substantial measurement error.

The method is a pure bag-of-words approach. It ignores syntax, negation, sentence structure, word order, and broader semantic context. All negative words are effectively treated as equally informative.

The paper itself explicitly describes its linguistic measure as crude and noisy. The authors argue that this noise should bias estimates toward zero, but it also means the measure cannot precisely distinguish different kinds of negative information.

The sample is restricted to S&P 500 firms. These are large, highly followed companies with substantial analyst coverage and news attention, so the results may not generalize to smaller firms or less informationally efficient settings.

The textual source is financial media, not issuer-authored disclosure. Journalists select which events to cover and how to describe them, so media tone may reflect both underlying firm information and editorial/intermediary interpretation.

The `Fund` measure is very simple: a story is classified as fundamental if it contains the stem `earn`. This captures an intuitive distinction but is an imperfect proxy for whether a story genuinely concerns firm fundamentals.

Return predictability is economically small and short-lived. The estimated next-day effect is only a few basis points per standard deviation of negativity, and most information appears to be incorporated on the same trading day.

The apparent trading strategy profitability is not robust to reasonable transaction costs. The authors explicitly show that a 10-basis-point round-trip cost can eliminate the abnormal profits.

The paper's return tests establish predictive association and are consistent with investor underreaction, but they do not cleanly identify the behavioral mechanism. Market microstructure, stale pricing, news timing, or other frictions may contribute.

The sample ends in 2004. News dissemination, algorithmic trading, natural-language processing, social media, and market speed have changed substantially since then, so the magnitude of delayed incorporation may be smaller in modern markets.

Finally, the paper predicts future returns rather than examining abnormal returns around scheduled issuer disclosures. Its economic-validation logic is highly relevant, but its event structure differs from a regulatory filing event study.
