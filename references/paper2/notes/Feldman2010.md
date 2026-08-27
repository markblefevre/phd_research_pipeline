# Feldman et al (2010)

## Local filename
Feldman_et_al_2010_Tone_Change.pdf

## Citation
@article{feldman2010management,
  title={Management’s tone change, post earnings announcement drift and accruals},
  author={Feldman, Ronen and Govindaraj, Suresh and Livnat, Joshua and Segal, Benjamin},
  journal={Review of Accounting Studies},
  volume={15},
  number={4},
  pages={915--953},
  year={2010},
  publisher={Springer}
}

## Research question
What does the paper ask?
Does the change in management tone in the MD&A section of 10-Q and 10-K filings contain incremental information about contemporaneous and future stock returns beyond quantitative signals such as earnings surprises and accruals? The paper also asks whether the usefulness of tone change depends on the strength of the firm's information environment.

## Data
153,988 firm-quarter observations with computable tone-change measures, drawn from U.S. 10-Q and 10-K filings on EDGAR from Q3 1994 through Q2 2007. The broader filing sample begins in Q4 1993, but at least three prior periodic filings are required to calculate tone change. SEC filings are matched to Compustat, CRSP, IBES, S&P Filing Dates, and point-in-time Compustat data from Charter Oak. The analysis uses only timely initial filings and applies size, price, and data-availability screens.

## Methods
Extract the MD&A section from 10-Q and 10-K filings and measure tone using dictionary counts of positive and negative words, primarily the Loughran-McDonald financial word lists, with the Harvard General Inquirer used as a robustness check. Three signals are constructed: positive tone, negative tone, and differential tone (positive minus negative), each scaled by total words.

Rather than use tone levels, the authors measure tone change relative to the same firm's periodic filings during the preceding 400 days. Each current tone measure is standardized by subtracting the firm's prior mean and dividing by its prior standard deviation, requiring at least three prior filings. They argue that changes are more informative than levels because tone levels are highly persistent and dictionary classifications can be affected by industry- or firm-specific vocabulary.

They test whether tone-change signals explain short-window excess returns around the SEC filing date and longer-horizon post-filing drift beyond standardized unexpected earnings (SUE) and accruals. The short window is days [-1,+1] around the filing date; the drift window runs from two days after the filing through one day after the subsequent quarter's preliminary earnings announcement. Tests use quarterly cross-sectional regressions in the spirit of Fama-MacBeth and buy-and-hold hedge portfolios formed from signal quintiles. They also examine whether results vary with analyst following, firm size, and book-to-market as proxies for the information environment.

## Main findings
Changes in MD&A tone contain incremental information beyond earnings surprises and accruals. Positive, negative, and differential tone-change signals are associated with short-window stock returns around SEC filings, and tone change also predicts subsequent abnormal-return drift. Negative tone change is particularly useful in the longer-horizon tests.

The tone signal remains informative after controlling for SUE and accruals and adds to portfolio returns based on those quantitative signals. Part of the return drift reflects tone change's ability to predict the subsequent quarter's earnings surprise, but this does not fully explain the effect.

Tone change is more informative when the firm's information environment is weaker, particularly for smaller firms and firms with less analyst following. Tone levels themselves generally do not have significant associations with filing-date returns or subsequent drift once the quantitative controls are included.

## Relevance to my paper
This is a particularly important antecedent for my study because it directly links dictionary-based financial sentiment extracted from corporate disclosures to market reactions. It provides early large-sample evidence that management tone contains economically meaningful information beyond conventional accounting variables and that market-based validation can distinguish useful textual signals.

The paper is also highly relevant to the choice between sentiment levels and sentiment changes. Feldman et al. explicitly argue that changes in tone relative to a firm's own prior disclosures may be more informative than absolute tone levels because disclosure language is persistent and firm- or industry-specific vocabulary can distort cross-sectional dictionary scores. This provides a strong precedent for considering whether changes in sentiment should be tested alongside sentiment levels.

Finally, the paper provides an important benchmark for comparing traditional dictionary methods with newer NLP, transformer, and LLM-based sentiment measures. If a relatively simple domain-specific dictionary produces sentiment signals associated with abnormal returns, more complex models should be judged on whether they provide incremental economic information rather than merely appearing linguistically more sophisticated.

## How I might cite it
- Changes in management tone in MD&A contain incremental information beyond earnings surprises and accruals.
- Dictionary-based financial sentiment measures can be validated using contemporaneous and subsequent market returns.
- Tone change may be more informative than tone level because disclosure tone is highly persistent across consecutive filings.
- Measuring change relative to the same firm's prior disclosures can mitigate firm- and industry-specific vocabulary effects that distort absolute dictionary scores.
- Tone-change signals are associated with short-window filing-date returns and with subsequent return drift.
- Textual information appears to be more valuable when the firm's information environment is weaker, such as for smaller firms or firms with less analyst coverage.
- The study provides an early benchmark showing that relatively simple dictionary-based sentiment measures can produce economically meaningful signals from corporate disclosures.
- Useful contrast for my paper: Feldman et al. study English-language U.S. MD&A disclosures using dictionary sentiment, whereas my study examines Japanese financial disclosures and compares multiple sentiment-measurement approaches.

## Possible literature-review section
Primary: Financial sentiment and market reactions
Secondary: Dictionary-based textual analysis; MD&A and corporate disclosure; changes versus levels of textual measures; market validation of sentiment

## Important quotes / page numbers
- p. 5 (PDF p. 7) — Contribution: the authors state that, to their knowledge, this is the first paper to show that management's tone change in MD&A is associated with immediate market reactions and can predict future stock prices beyond established measures of company performance.
- p. 18 (PDF p. 20) — Key methodological motivation: they argue that the relevant variable is not the level of optimism or pessimism in the current filing, but its change from the recent past.
- pp. 19–20 (PDF pp. 21–22) — Why changes rather than levels: adjacent tone levels are highly autocorrelated, and absolute dictionary scores can be distorted by firm- and industry-specific vocabulary; using tone changes mitigates these problems.
- p. 44 (PDF p. 46) — Main conclusion: tone-change signals are significantly related to short-window filing-date returns and subsequent excess-return drift even after controlling for accruals and earnings surprises.
- p. 44 (PDF p. 46) — Limitation acknowledged by the authors: they describe word-frequency tone classification as a “crude measure” of whether qualitative information is favorable or unfavorable.

## Caveats / limitations
The sentiment measure is dictionary-based and relies on word frequencies, so it does not capture context, negation, word order, semantic nuance, or the meaning of sentences. The authors themselves describe the approach as a crude measure and note that the incremental explanatory power of qualitative information is statistically significant but small relative to the total unexplained variation in returns.

The study uses historical U.S. 10-Q and 10-K MD&A disclosures, limiting direct generalization to other languages, disclosure regimes, document types, and modern information environments. Requiring at least three prior periodic filings to construct the standardized tone-change measure also reduces the usable sample and favors firms with sufficient filing history.

Although the use of tone changes mitigates some dictionary problems, results still depend on the chosen word classification. The authors obtain broadly similar results with the Loughran-McDonald and Harvard General Inquirer lists, but differences remain for some tests. Market reactions also occur to the entire SEC filing, not only the MD&A, so the regressions cannot fully isolate the causal contribution of MD&A tone from all other information released in the filing.