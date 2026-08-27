# Tetlock (2007)

## Local filename
Tetlock-journal-of-finance-2007.pdf

## Citation
@article{tetlock2007giving,
  title={Giving content to investor sentiment: The role of media in the stock market},
  author={Tetlock, Paul C},
  journal={The Journal of Finance},
  volume={62},
  number={3},
  pages={1139--1168},
  year={2007},
  publisher={Wiley Online Library}
}

## Research question
What does the paper ask?

Does the tone of financial media capture investor sentiment in a way that predicts stock-market activity? More specifically, Tetlock asks whether pessimistic language in a prominent Wall Street Journal market column predicts subsequent market returns and trading volume, whether market returns themselves affect subsequent media pessimism, and whether the observed relationships are more consistent with investor-sentiment/noise-trader theories than with the media simply conveying new fundamental information.

## Data
The textual data consist of the daily Wall Street Journal “Abreast of the Market” column over the 16-year period from 1984 through 1999.

The paper analyzes roughly 3,700 trading-day observations. The column was also distributed electronically through Dow Jones Newswires, allowing Tetlock to study timing relative to market activity.

Market data include:
- daily Dow Jones Industrial Average returns,
- NYSE trading volume,
- the Fama-French small-minus-big (SMB) factor,
- measures of past market volatility, and
- intraday Dow Jones data used in robustness tests.

The Dow Jones return sample runs from January 1, 1984 through September 17, 1999.

## Methods
Tetlock uses the General Inquirer (GI) content-analysis program and the Harvard IV-4 psychosocial dictionary to convert each day’s WSJ column into word-category counts.

The General Inquirer classifies words into 77 predetermined psychological and semantic categories. Tetlock then applies principal components analysis to those category counts and extracts the first principal component, which captures the largest amount of common variation across categories.

The first factor loads heavily on categories such as Negative, Weak, Fail, and Fall and negatively on Positive words, so Tetlock interprets it as a **media pessimism factor**.

Importantly, the factor is constructed without using stock-market outcomes. To limit look-ahead bias, factor loadings for year t are estimated using the preceding year and then applied to the text in year t.

The paper then estimates vector autoregressions (VARs) linking:
- media pessimism,
- Dow Jones returns, and
- detrended NYSE trading volume.

The models include up to five daily lags and controls for past returns, volume, volatility, day-of-week effects, January effects, and the 1987 crash. Newey-West standard errors are used.

Tetlock also tests two simpler dictionary measures—the GI Negative and Weak categories—to ensure that the results do not depend only on the PCA-derived pessimism factor.

Additional tests examine:
- whether pessimism predicts returns on the Fama-French SMB factor,
- whether market declines predict subsequent pessimistic media tone,
- whether extreme pessimism predicts trading volume,
- whether the results survive a delayed return window beginning at 10 a.m. the next day, and
- whether a simple sentiment-based trading strategy would have produced economically meaningful returns.

## Main findings
Higher media pessimism predicts lower stock-market returns on the following trading day.

A one-standard-deviation increase in the pessimism factor predicts an approximately 8.1-basis-point decline in the next day’s Dow Jones return. This effect is economically meaningful relative to the average daily market return.

The decline is temporary. Returns reverse over the following several trading days, and the cumulative reversal is close to the size of the initial decline. Tetlock interprets this temporary price pressure and subsequent reversal as more consistent with investor-sentiment or liquidity/noise-trader models than with the media revealing new fundamental information.

The simpler Negative and Weak word categories produce similar results, strengthening the interpretation of the PCA factor as a measure of pessimistic sentiment.

The relationship is bidirectional: poor market returns also predict more pessimistic language in the next day’s WSJ column. Thus, media tone both reflects recent market performance and predicts subsequent market activity.

Unusually high or low levels of pessimism predict increased trading volume. The absolute value of pessimism is particularly informative for next-day NYSE volume, consistent with sentiment or disagreement generating trading activity.

Pessimism also predicts lower returns for small stocks, with effects that appear somewhat more persistent than for the Dow Jones.

Robustness tests using a return window beginning at 10 a.m. on the following day produce similar results. This weakens the explanation that the findings simply reflect slow overnight incorporation of fundamental information contained in the column.

A simple hypothetical strategy that trades the Dow based on the previous day’s Negative-word measure earns an annualized return of about 7.3% before transaction costs. Tetlock explicitly cautions that trading costs, market impact, and taxes could eliminate much or all of this profitability.

## Relevance to my paper
Tetlock (2007) is a foundational paper linking automated textual sentiment measures to realized market outcomes. It provides early evidence that a relatively simple dictionary-based measure of financial-media tone contains economically meaningful information about subsequent returns and trading activity.

The paper is particularly relevant to the economic-validation component of my study. Tetlock does not evaluate sentiment only against human labels or linguistic classification accuracy; he asks whether the textual measure predicts observable market behavior.

The design is also methodologically clean in an important respect: the pessimism measure is constructed from text without using returns to train the sentiment model. Market returns are therefore an external validation target rather than an input to the construction of the sentiment measure. This contrasts with approaches such as Frankel et al., where supervised text models are trained directly on abnormal returns.

Tetlock also provides an early benchmark for dictionary-based sentiment prior to the finance-specific Loughran-McDonald dictionary. His use of the Harvard General Inquirer illustrates both the usefulness and the limitations of general-purpose sentiment lexicons in financial applications.

The temporary price decline followed by reversal is conceptually important. It shows that textual sentiment may be economically informative without representing new fundamental information; instead, sentiment may capture temporary investor beliefs, attention, or noninformational trading pressure.

For my study, the setting differs substantially because Tetlock analyzes English-language financial news at the aggregate market level, whereas my study examines Japanese corporate disclosures and firm-level market reactions.

## How I might cite it
- Tetlock (2007) provides early evidence that automated textual sentiment measures are associated with realized stock-market outcomes.
- Pessimistic financial-media language predicts short-horizon declines in market returns followed by subsequent reversal.
- Extreme levels of media pessimism predict increased trading volume.
- Market returns also influence subsequent media sentiment, indicating a bidirectional relationship between media tone and market activity.
- Dictionary-based text measures can capture economically meaningful investor sentiment even when they are constructed independently of market returns.
- The return reversal following pessimistic media coverage is more consistent with temporary sentiment-driven price pressure than with the disclosure of new fundamental information.
- Tetlock provides an important early market-based validation of textual sentiment before the development of finance-specific dictionaries such as Loughran-McDonald.
- Useful contrast for my paper: Tetlock studies aggregate English-language financial news, whereas my study examines Japanese corporate disclosures and firm-level market outcomes.

## Possible literature-review section
Primary: Economic validation: sentiment and market outcomes

Secondary: Dictionary-based financial sentiment; financial news sentiment; investor sentiment and behavioral finance

## Important quotes / page numbers
- p. 1139 (PDF p. 1) — Abstract: high media pessimism predicts downward pressure on market prices followed by reversal, while unusually high or low pessimism predicts high trading volume.
- p. 1140 (PDF p. 2) — Main contribution: Tetlock describes the paper as early evidence that media content can predict broad measures of stock-market activity.
- pp. 1140–1141 (PDF pp. 2–3) — Data and sentiment construction: General Inquirer word counts from the WSJ “Abreast of the Market” column are reduced through PCA to a single pessimism factor.
- pp. 1144–1146 (PDF pp. 6–8) — Methodology: the General Inquirer uses 77 Harvard IV-4 categories, and the first principal component loads strongly on Negative, Weak, Fail, and Fall.
- pp. 1148–1150 (PDF pp. 10–12) — Main return result: a one-standard-deviation increase in pessimism predicts an 8.1-basis-point decline in the next day’s Dow return, followed by a 6.8-basis-point reversal over days two through five.
- p. 1151 (PDF p. 13) — Reverse relationship: a 1% decline in the prior day’s Dow return predicts a significant increase in next-day media pessimism.
- pp. 1152–1153 (PDF pp. 14–15) — Volume result: the absolute value of pessimism significantly predicts increased next-day NYSE volume.
- p. 1154 (PDF p. 16) — Small-stock result: negative sentiment predicts lower SMB returns over the following week.
- pp. 1156–1157 (PDF pp. 18–19) — Timing robustness: pessimism still predicts returns when the return window begins at 10 a.m. the next trading day, weakening the delayed-information explanation.
- pp. 1163–1164 (approximately PDF pp. 25–26) — Economic significance: a simple pessimism-based trading strategy earns about 7.3% annualized before costs, but the author warns that transaction costs, price impact, and taxes may eliminate profitability.
- p. 1166 onward (conclusion section) — Interpretation: the evidence is most consistent with pessimistic media content proxying for investor sentiment, risk aversion, or noninformational trading rather than new fundamental information.

## Caveats / limitations
The textual source is extremely narrow: a single Wall Street Journal column, “Abreast of the Market.” The findings therefore do not necessarily generalize to other newspapers, corporate disclosures, firm-specific news, or modern information channels.

The sentiment dictionary is not finance-specific. The General Inquirer and Harvard IV-4 categories were developed for general psychological and social-language analysis rather than financial text. Later work, especially Loughran and McDonald (2011), shows why general dictionaries can misclassify finance-specific terminology.

The pessimism factor is not a direct sentiment label but the first principal component of 77 dictionary-category counts. Although it loads heavily on intuitive negative categories and simpler Negative/Weak measures produce similar results, the factor’s interpretation as “investor sentiment” remains an inference.

The paper studies aggregate market returns rather than firm-level disclosure reactions. Its evidence therefore establishes a relation between broad media tone and market activity, not whether sentiment in a particular company disclosure causes a firm-specific return response.

The relation between media tone and market returns is bidirectional. Negative returns predict more pessimistic media tone, while pessimistic tone predicts subsequent returns. This makes causal interpretation difficult: the paper documents dynamic association but cannot cleanly establish that media pessimism itself causes the market movement.

Tetlock's preferred interpretation is that the return reversal is consistent with sentiment-driven temporary price pressure, but other behavioral or liquidity mechanisms may produce similar patterns. The paper therefore cannot uniquely identify the underlying mechanism.

The sample ends in 1999. Media distribution, electronic trading, information speed, algorithmic trading, social media, and the financial-news ecosystem have changed dramatically since then, so the magnitude and timing of the effects may not generalize to modern markets.

The hypothetical trading strategy is not evidence of readily exploitable abnormal profit. Tetlock explicitly notes that transaction costs, market impact, financing constraints, and taxes could erase its apparent profitability.

Finally, the paper's predictive effects operate over daily horizons and concern financial news rather than scheduled corporate filings. Its economic-validation logic is highly relevant, but the specific return dynamics are not directly comparable with event-study CARs around annual-report disclosures.
