# Glasserman and Mamaysky (2019)

## Local filename
Glasserman_Mamaysky_2019_Unusual_News.pdf

## Citation
@article{glasserman2019does,
  title={Does unusual news forecast market stress?},
  author={Glasserman, Paul and Mamaysky, Harry},
  journal={Journal of Financial and Quantitative Analysis},
  volume={54},
  number={5},
  pages={1937--1974},
  year={2019},
  publisher={Cambridge University Press}
}

## Research question
What does the paper ask?

Can the textual content of financial news forecast future market stress, and does combining sentiment with the unusualness or novelty of language provide more information than sentiment alone? The paper also asks whether news is incorporated differently into company-specific versus aggregate market volatility and whether limited investor attention can explain any delayed response.

## Data
The main text dataset contains 367,331 unique Thomson Reuters news stories from January 1996 through December 2014 concerning 50 large global banks, insurance companies, and real-estate firms selected by market capitalization as of February 2015.

The raw Reuters archive contains more than 600,000 articles. The authors remove repeated rewrites, PR Newswire releases, regulatory-filing stories, and data-table stories to obtain the final sample. Nearly 90% of the remaining articles are Reuters stories, with the balance coming from 16 other news services.

The sample is internationally distributed but heavily weighted toward English-speaking markets. U.S. firms account for roughly 44% of the articles. Only two Japanese firms—Mitsubishi UFJ and Sumitomo Mitsui—are included.

Market data come from Bloomberg. Firm-level variables include U.S.-dollar total returns, 30-day at-the-money option-implied volatility where available, and 20-day realized volatility. Aggregate variables include the VIX and 20-day realized volatility for the S&P 500.

Because construction of the unusualness measure requires a historical training window, the first entropy observation is April 1998. Firm-level analyses using implied volatility begin in January 2005.

## Methods
The paper combines a traditional financial sentiment measure with a measure of textual unusualness based on an n-gram language model.

The basic textual unit is a 4-word sequence (4-gram), rather than an individual word. Each month, the authors examine the 5,000 most frequently occurring 4-grams. Across the sample this produces roughly 1.14 million monthly 4-gram observations and 394,778 distinct 4-grams.

Unusualness is measured using cross-entropy. For each evaluation month t, the language model is trained on articles from months t-27 through t-4. A phrase is more unusual when it occurs frequently in the current month but had a low conditional probability in the historical training corpus. High entropy therefore indicates more unusual language.

Sentiment is based on the Loughran-McDonald financial dictionary. A 4-gram is classified as positive if it contains at least one positive word and no negative word, and negative if it contains at least one negative word and no positive word. Positive and negative sentiment measures are the fractions of 4-grams classified in each category.

The authors then interact sentiment with unusualness:
- `ENTSENT_NEG = ENTNEG × SENTNEG`
- `ENTSENT_POS = ENTPOS × SENTPOS`

These interaction terms are intended to identify news that is simultaneously unusual and strongly positive or negative.

At the firm level, panel regressions test whether lagged text measures forecast future implied and realized volatility after controlling for lagged implied volatility, realized volatility, negative returns, and news volume.

At the aggregate level, the authors estimate vector autoregressions (VARs) containing the VIX, S&P 500 realized volatility, and the aggregate news measures. Impulse-response functions are used to trace the effect of shocks to unusual positive and negative news over subsequent months.

The paper also estimates panel VARs at the company level to compare the speed of firm-specific and aggregate information incorporation.

Finally, the authors test economic significance using a simple S&P 500 put-selling strategy whose exposure is reduced when unusual negative news is elevated and increased when unusual positive news is elevated.

## Main findings
The interaction between sentiment and unusualness is consistently more informative than either sentiment or unusualness by itself.

At the company level, unusual negative news predicts higher future implied and realized volatility, while unusual positive news predicts lower volatility. These relations remain statistically and economically significant after controlling for standard volatility predictors. A one-standard-deviation increase in unusual positive or negative news over the included lags changes subsequent volatility by roughly 1.3 to 2.9 volatility points, which the authors characterize as economically large.

At the aggregate level, shocks to unusual negative news generate significant increases in both VIX and S&P 500 realized volatility. The response is hump-shaped: it peaks roughly four months after the news shock and remains significant for many months. Unusual positive news produces a corresponding decline in volatility, although the negative-news effect is stronger.

News is incorporated more quickly at the individual-company level than at the aggregate-market level. The authors interpret this as evidence that markets are more informationally efficient with respect to micro information than macro information.

Articles explicitly about the overall market do not forecast volatility, whereas aggregate signals constructed by combining information from many company-specific articles do. The authors argue that this pattern is consistent with limited investor attention: aggregate information is harder to collect and process because it is dispersed across many individual-company stories.

The text measures also have economic value in the authors' S&P 500 put-selling exercise. A strategy that conditions option exposure on unusual positive and negative news outperforms the baseline put-selling strategy, particularly during the financial crisis.

The main aggregate results are not simply driven by the 2008 crisis. A pre-crisis VAR produces qualitatively similar results, and adding the longer-horizon Mid-Term VIX does not eliminate the delayed hump-shaped response to unusual news.

## Relevance to my paper
This paper is useful because it shows that the economic information in financial text may depend not only on sentiment but also on whether the language is unusual relative to prior text. This provides an important conceptual bridge between the sentiment literature and the textual-novelty literature.

The paper demonstrates that two passages with similar dictionary sentiment can have very different economic significance if one contains routine language and the other contains an unusual combination of words. In this sense, sentiment and novelty are complementary rather than competing textual characteristics.

This is particularly relevant to any analysis combining sentiment with a measure of textual change or novelty. Glasserman and Mamaysky provide evidence that interacting the two dimensions can produce substantially stronger market signals than either dimension separately.

The paper is also important as an example of economic validation that goes beyond return direction. The authors validate textual measures against future realized and implied volatility and through an options-based trading exercise. This broadens the concept of “market relevance” beyond CARs or future returns alone.

Methodologically, the paper provides a pre-transformer example of contextual information being recovered from sequences of words. Its 4-gram entropy approach captures phrase novelty that a pure bag-of-words sentiment measure misses, offering a useful historical step between word-count methods and modern contextual language models.

For my study, however, the setting differs substantially: Glasserman and Mamaysky analyze English-language financial news rather than Japanese corporate disclosures, and their principal economic outcome is volatility rather than abnormal stock returns.

## How I might cite it
- Sentiment becomes more informative when combined with a measure of textual unusualness or novelty.
- Financial text can contain economically relevant information that is missed by standard positive/negative word counts.
- Unusual negative news predicts higher future implied and realized volatility, while unusual positive news predicts lower volatility.
- Interactions between sentiment and novelty can outperform either textual characteristic considered separately.
- N-gram entropy provides an early method for measuring contextual textual novelty beyond individual-word frequencies.
- Financial markets may incorporate dispersed textual information only gradually, particularly when extracting aggregate information requires processing many individual-company stories.
- Text-based measures can be economically validated using volatility and option-market outcomes as well as stock returns.
- The study provides a useful conceptual link between the sentiment literature and the textual-novelty literature.
- Useful contrast for my paper: Glasserman and Mamaysky study English financial news and forecast volatility, while my study examines Japanese corporate disclosures and market-return outcomes.

## Possible literature-review section
Primary: Economic validation: sentiment and market outcomes

Secondary: Textual novelty; financial news sentiment; unusualness and incremental information; investor attention

## Important quotes / page numbers
- p. 1937 (PDF p. 1) — Abstract: unusual negative news predicts higher stock-market volatility, unusual positive news predicts lower volatility, and the interaction of unusualness and sentiment forecasts volatility several months ahead.
- pp. 1938–1939 (PDF pp. 2–3) — Core motivation: sentiment word counts treat routine and genuinely unusual negative phrases similarly; the authors argue that sentiment becomes more informative when interacted with unusualness.
- p. 1941 (PDF p. 5) — Methodological contribution: unusualness is measured using entropy in consecutive 4-word sequences rather than individual words.
- pp. 1942–1944 (PDF pp. 6–8) — Formal construction of the n-gram probability and cross-entropy measure; unusual texts have high entropy because currently frequent phrases were historically improbable.
- p. 1950 (PDF p. 14) — Illustrative examples: phrases associated with the Lehman collapse and euro-zone crisis rank among the most unusual negative 4-grams in their respective months.
- pp. 1956–1957 (approximately PDF pp. 20–21) — Economic magnitude: a one-standard-deviation change in unusual positive/negative news produces roughly a 1.3–2.9 point decrease/increase in subsequent volatility.
- p. 1962 (approximately PDF p. 26) — Aggregate dynamics: the effect of unusual negative news on volatility peaks about four months later and persists for months, unlike much prior sentiment-return evidence operating over days.
- pp. 1962–1963 (approximately PDF pp. 26–27) — Robustness and economic value: results survive inclusion of the Mid-Term VIX and a pre-crisis subsample; the news measures are also used to improve an S&P 500 put-selling strategy.
- pp. 1968–1969 (approximately PDF pp. 32–33) — Investor-attention interpretation: readily accessible market-specific news does not forecast volatility, whereas dispersed information aggregated from company-specific articles does.
- Conclusion — The authors interpret the slower aggregate response as evidence consistent with attention constraints and greater informational efficiency at the micro than macro level.

## Caveats / limitations
The sample consists only of 50 very large financial-sector firms—banks, insurers, and real-estate companies—selected by market capitalization as of February 2015. This sharply limits generalization to smaller firms, nonfinancial industries, or broader corporate-disclosure settings.

Selection of firms using 2015 market capitalization creates survivorship bias because firms that disappeared before 2015, including Bear Stearns, Lehman Brothers, and Washington Mutual, are absent. The authors argue that this bias works against their volatility-prediction result because excluded failed firms experienced especially high crisis volatility, but it remains an important sample-design limitation.

The news dataset is heavily weighted toward Reuters and English-speaking markets. Nearly 90% of articles come from Reuters, and U.S., U.K., Australian, and Canadian firms account for a large share of coverage. Generalization to other languages, information environments, and news sources is uncertain.

The entropy measure depends on several researcher choices: 4-grams, a rolling t-27 to t-4 training window, the top 5,000 monthly phrases, and the specific handling of unseen n-grams. The authors provide robustness analysis, but textual unusualness is not a uniquely defined construct.

Sentiment remains dictionary-based. Although the use of 4-grams incorporates local phrase context for the novelty calculation, sentiment itself is determined by whether a phrase contains Loughran-McDonald positive or negative words. The method therefore still cannot capture the full semantic context, negation structure, irony, or more complex meaning available to modern contextual language models.

The central investor-attention explanation is plausible and supported by the contrast between company-specific, aggregate, and market-specific news, but it is not uniquely identified. Slow information incorporation could potentially arise from mechanisms other than limited attention.

The analysis forecasts volatility rather than the direction of stock returns. It is therefore most directly relevant as evidence that textual measures contain economically useful information, not as direct evidence that novelty improves sentiment-based CAR prediction.

The effects unfold at monthly horizons and the aggregate entropy measure begins only in 1998; firm-level implied-volatility analysis begins in 2005. The effective time-series sample is therefore substantially shorter than the raw 1996–2014 news archive suggests.

Finally, the strongest results come from an interaction between two constructed variables—entropy and sentiment. This is valuable evidence that the dimensions are complementary, but interpreting the coefficient as a clean causal effect of “novel sentiment” is not warranted.
