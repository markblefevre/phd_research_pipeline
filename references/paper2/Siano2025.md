# Siano (2025)

## Local filename
Siano_MS2025(20260827-044510).pdf

## Citation
@article{siano2025news,
  title={The news in earnings announcement disclosures: Capturing word context using {LLM} methods},
  author={Siano, Federico},
  journal={Management Science},
  volume={71},
  number={11},
  pages={9831--9855},
  year={2025},
  publisher={INFORMS}
}

## Research question
What does the paper ask?

How much economically relevant information is contained in the text of earnings-announcement disclosures once word context is modeled with a transformer-based language model, and under what circumstances is that textual information especially valuable to investors?

More specifically, the paper asks whether a contextual language model can explain short-window market reactions better than dictionary-based measures, conventional machine-learning text models, and financial-statement surprises; how quickly textual information is incorporated into prices; whether conference-call text adds information beyond press releases; and which parts of disclosure text contain the most market-relevant information.

## Data
The primary sample contains 98,171 U.S. firm-quarter earnings-announcement press releases over 2014–2023.

The authors begin with 253,727 EDGAR 8-K filings containing quarterly earnings-announcement press releases over 2006–2023. After excluding very short disclosures, observations without return data, penny stocks, very low-volume stocks, and very small firms, 229,905 disclosures remain for model development and out-of-sample prediction.

The model-development sample contains:
- 109,687 observations from 2006–2013 used for fine-tuning, and
- 120,218 observations from 2014–2023 used for out-of-sample prediction.

After requiring I/B/E/S and Compustat data, the final primary empirical sample contains 98,171 firm-quarter press releases.

A subsample of 56,670 observations has same-day earnings conference-call transcripts from Capital IQ.

For minute-level analyses, the paper uses 63,249 observations with 5-minute, 30-minute, and 2-hour price data.

Data sources include:
- SEC EDGAR for earnings press releases,
- Capital IQ for conference-call transcripts,
- CRSP for daily returns,
- TAQ for intraday returns,
- Compustat for accounting variables,
- I/B/E/S for analyst estimates and earnings-announcement timestamps, and
- I/B/E/S Guidance for management forecasts.

## Methods
The paper uses **RoBERTa**, a BERT-family transformer model, and fine-tunes it to predict the magnitude and direction of short-window stock returns from disclosure text.

The key dependent variable used for fine-tuning is the two-day cumulative abnormal return around the earnings announcement, `CAR[0,1]`. Each earnings press release is therefore labeled with its contemporaneous market reaction, and the model learns a mapping from words and word context to the signed abnormal return.

The resulting prediction, `CAR LLM EA`, is interpreted as a single summary measure of the market-relevant “news” contained in the earnings-announcement text.

The design is explicitly out of sample. Observations from 2006–2013 are used to fine-tune the model, while the 2014–2023 disclosures are used for prediction. The paper also performs analyses on 2021–2023 data, entirely after RoBERTa's 2019 pretraining period, to address possible pretraining information leakage.

Because BERT-family models have a 512-token sequence limit, the paper splits long press releases into 512-token segments, generates a return prediction for each segment, and averages the segment predictions.

A linear output layer is added so that the transformer performs a regression task rather than the more common positive/negative classification task.

SHAP values are used to identify the words and word contexts that contribute most to the model's predictions.

The LLM measure is benchmarked against several alternatives.

Dictionary-based measures include:
- tone, measured as `(positive - negative) / total words` using the Henry (2008) dictionary,
- disclosure length,
- Fog readability,
- number-related text, and
- forward-looking language.

Non-LLM machine-learning benchmarks include:
- gradient boosting,
- random forest,
- support-vector regression, and
- supervised LDA,

using the most relevant unigrams and bigrams as textual inputs. Gradient boosting is the primary non-LLM benchmark.

The paper also compares the text models with 12 analyst-forecast and management-guidance surprise variables, modeled both linearly and with machine learning.

Economic validation focuses on explanatory power for:
- two-day `CAR[0,1]`,
- 5-minute, 30-minute, and 2-hour price reactions,
- conference-call returns,
- abnormal trading volume,
- return volatility, and
- future earnings outcomes.

The paper further partitions disclosure text to determine where the information is located. It compares:
- sentences discussing numbers versus other sentences,
- text near the beginning versus the end of the disclosure, and
- novel versus stale/repeated text relative to the prior quarter's earnings release.

Novel sentences are identified through string similarity to the firm's previous-quarter disclosure; sentences with greater than 90% similarity are classified as stale/repeated.

## Main findings
The contextual transformer measure explains substantially more variation in short-window stock returns than conventional textual measures.

The LLM-derived disclosure-news measure explains about **15% of the variation in two-day abnormal returns**, compared with approximately 4.5% reported for non-LLM machine-learning approaches in prior work and about 3.1% for the paper's own n-gram gradient-boosting benchmark.

Relative to conventional dictionary and narrative attributes, which explain roughly 1.6% of return variation, the LLM measure provides almost a tenfold increase in explanatory power.

The LLM measure also provides substantial incremental explanatory power beyond financial-statement surprises. Textual news remains strongly informative even when analyst and management forecast surprises are modeled using nonlinear machine-learning techniques.

The economic information in disclosure text is incorporated rapidly. Press-release text explains about 8% of 5-minute price reactions, and the incremental contribution of text increases as the immediate window expands from 5 minutes to 30 minutes and two hours.

Conference-call text adds further information. LLM-modeled conference calls explain roughly 19% of two-day returns and increase explanatory power by as much as 30% relative to press-release text and financial surprises in shorter call-specific windows.

Text is particularly informative when reported earnings are less useful for valuation, including for firms with losses, high intangible intensity, or less persistent earnings. Textual information also becomes especially valuable during periods of heightened aggregate uncertainty such as financial crises, the COVID-19 shock, and the U.S.-China trade war.

Within the disclosure, the most informative text tends to:
- discuss reported numbers,
- appear near the beginning of the press release, and
- contain **novel information relative to the prior quarter's disclosure**.

The novelty result is particularly relevant: sentences that differ materially from the prior-quarter earnings release explain more market reaction than stale/repeated sentences.

The importance of word context is supported by additional tests. Randomizing word order reduces explanatory power by roughly 80%, and restricting the transformer to a small set of conventional performance or tone words substantially reduces predictive ability.

Overall, the paper argues that prior dictionary and n-gram methods substantially underestimate the amount of economically relevant information in corporate disclosures because they cannot fully capture sequential word context.

## Relevance to my paper
This is one of the most important recent methodological benchmarks for my study because it directly compares context-sensitive transformer methods with dictionary-based and classical machine-learning text measures using **short-window market reactions as the economic outcome**.

The paper strongly supports the argument that word context can matter for economic inference. Siano's example is directly relevant to sentiment measurement: a word such as “loss” cannot always be interpreted as negative without understanding why the loss occurred and the language surrounding it. This is precisely the type of limitation that contextual models are designed to address.

It is also highly relevant because the paper focuses on **event-window returns**, unlike studies that validate text measures mainly through long-horizon portfolios. This makes Siano especially close to a study asking whether alternative textual measures produce different conclusions about abnormal returns around a disclosure event.

The finding that novel text is more informative than stale text also creates an important bridge between the sentiment and textual-novelty literatures. It suggests that a contextual sentiment signal may be especially useful when applied to changed or newly introduced disclosure language rather than to the entire repeated document.

However, the central methodological caveat is crucial: **the LLM is trained directly on the same type of market outcome used to evaluate it**. RoBERTa is fine-tuned using `CAR[0,1]` as its label. Thus, the resulting variable is not an independently constructed measure of linguistic sentiment; it is a supervised text-based predictor of contemporaneous abnormal returns.

This makes the paper conceptually closer to Frankel et al. than to FinBERT-style sentiment classification. Its headline result demonstrates the power of contextual text for predicting market reactions, but it does not establish that an independently measured contextual “sentiment” variable would outperform a dictionary sentiment measure.

That distinction may be central to my paper. If my dictionary, transformer, and LLM sentiment measures are constructed independently of realized returns and are then compared using the same event-study outcome, the comparison asks a different and arguably cleaner question: **which independently measured notion of sentiment is most economically informative?**

The paper is also U.S.- and English-specific. Its results do not establish whether contextual models retain the same advantage for Japanese regulatory disclosures, where morphology, tokenization, financial terminology, and disclosure structure differ substantially.

## How I might cite it
- Context-sensitive transformer models can explain substantially more short-window market-return variation than dictionary and n-gram text measures.
- Modeling word context materially improves the measurement of economically relevant information in corporate disclosures.
- Siano (2025) finds that LLM-modeled earnings-announcement text explains about 15% of two-day abnormal-return variation.
- Contextual textual information adds substantial explanatory power beyond analyst and management forecast surprises.
- Investors begin incorporating textual information within minutes of earnings-announcement releases.
- Conference-call text contains incremental information beyond press releases and financial-statement surprises.
- Textual information is especially valuable when accounting earnings are less persistent or aggregate uncertainty is elevated.
- Novel disclosure text is more informative than language repeated from the prior-quarter disclosure.
- Randomizing word order sharply reduces explanatory power, providing direct evidence that sequential context matters.
- The paper provides strong evidence that bag-of-words and n-gram approaches may underestimate the market-relevant information contained in financial text.
- Important methodological caveat: Siano's transformer is fine-tuned directly on abnormal returns, so the resulting “news” measure is a supervised text-based market-reaction predictor rather than an independently measured sentiment variable.
- Useful contrast for my paper: Siano studies English earnings press releases and trains the model on contemporaneous CARs, whereas my study can compare independently constructed sentiment measures on Japanese regulatory filings using market returns only for external economic validation.

## Possible literature-review section
Primary: Large language models / contextual transformer methods

Secondary: Economic validation: sentiment and market outcomes; machine learning and transformer-based financial text; textual novelty

## Important quotes / page numbers
- p. 1 — Abstract: LLM-modeled earnings press releases explain roughly three times more short-window return variation than dictionary and non-LLM machine-learning textual measures.
- pp. 2–3 — Motivation: traditional methods ignore sequential word context, creating measurement error and potentially underestimating the information content of disclosure text.
- p. 3 — Core method: the LLM is fine-tuned directly to predict the magnitude and direction of two-day abnormal stock returns from earnings-announcement text.
- pp. 11–13 — Model rationale: BERT-family models capture ordered word context; RoBERTa is used as the contextual transformer.
- pp. 15–17 — Sample and implementation: 98,171 primary out-of-sample press releases; 2006–2013 observations are used for fine-tuning and 2014–2023 for prediction; 512-token segments are averaged for long documents.
- pp. 17–18 — Benchmarks: the paper compares the transformer against dictionary attributes, gradient boosting, random forest, SVR, supervised LDA, and financial-statement surprise models.
- pp. 20–22 — Main return result: the LLM variable explains about 15% of two-day abnormal-return variation and substantially outperforms conventional and n-gram-based alternatives.
- pp. 22–24 — Intraday result: textual disclosure explains price revisions within five minutes, and its contribution increases over 30-minute and two-hour windows.
- pp. 24–25 — Conference-call result: conference-call text provides significant incremental explanatory power beyond press releases and financial surprises.
- Later within-document analysis / Table on textual partitions — Most market-relevant text discusses numbers, appears early in the disclosure, and is novel relative to the prior quarter.
- Robustness section — Restricting the analysis to 2021–2023 produces similar results, addressing concern that RoBERTa pretraining through 2019 overlaps with part of the primary out-of-sample period.
- Robustness section — Randomizing word order reduces explanatory power by approximately 80%, directly demonstrating the importance of sequential context.
- Conclusion — The author concludes that prior text-analysis methods materially understate the economic relevance of disclosure text because they fail to capture complex word context.

## Caveats / limitations
The most important limitation for comparison with independent sentiment measures is that the RoBERTa model is **fine-tuned directly on abnormal returns**. `CAR[0,1]` is the target used to train the textual model. Therefore, the resulting `CAR LLM EA` variable is optimized specifically to predict market reactions and should not be interpreted as a generic linguistic sentiment measure.

The out-of-sample design is a major strength and substantially reduces conventional overfitting concerns, but it does not make the comparison with dictionary sentiment conceptually symmetric. The contextual model is explicitly trained to predict CARs, while dictionary tone is not.

Relatedly, the paper's main construct is **“disclosure news,” not sentiment**. The model can exploit any linguistic feature that historically maps to stock returns, including numeric discussion, strategic language, risk information, guidance, or other semantic content unrelated to conventional positive/negative tone. Its superior R² therefore does not demonstrate that contextual sentiment itself is superior to dictionary sentiment.

The paper itself notes that it does not produce an ex-ante trading signal: the object of interest is contemporaneous rather than future market reaction. The results should therefore be interpreted as evidence about information content and explanatory power rather than as return predictability or investment profitability.

The paper uses RoBERTa, which is a BERT-family encoder model rather than a modern generative chat-oriented LLM. Calling it an “LLM” is consistent with the author's terminology, but for my literature review it may be clearer to describe the method as a **contextual transformer / BERT-family language model** to avoid conflating it with GPT-style generative LLMs.

The 512-token limit is handled by segmenting disclosures and averaging segment-level return predictions. This is pragmatic but assumes that information can be aggregated adequately across independent chunks; interactions across distant parts of the document are not modeled directly.

The strongest benchmark comparison is against dictionary attributes and n-gram-based classical machine learning rather than against modern instruction-tuned generative models. The paper therefore establishes the value of contextual transformer representations relative to less contextual approaches, not necessarily the superiority of RoBERTa over contemporary LLM families.

RoBERTa's pretraining period extends through 2019, overlapping with part of the 2014–2023 out-of-sample empirical period. The paper explicitly recognizes the potential look-ahead/information-leakage concern and reports similar results using 2021–2023 disclosures, which are fully after the pretraining period, but the issue remains worth noting when interpreting the primary sample.

The intraday analysis does not always use market-adjusted abnormal returns. For very short windows, the author argues that market shocks are unlikely to dominate and that constructing a contemporaneous benchmark from a changing sample could introduce noise. This is reasonable, but the intraday results are not directly comparable with conventional event-study CARs.

Tables and generic cautionary statements are excluded from the primary press-release text. Including them lowers explanatory power by about 1.5 percentage points but does not change the broad conclusions. The resulting construct therefore reflects a curated subset of disclosure content rather than every element visible to investors.

The study focuses on U.S. English-language earnings press releases and conference calls. Generalization to annual reports, regulatory filings, other languages, and especially Japanese EDINET disclosures is not established.

Finally, high explanatory power does not itself establish causality. The model identifies textual patterns associated with contemporaneous price revisions, but those patterns may proxy for underlying economic information, management expectations, numerical news, or other latent disclosure content rather than causing returns through linguistic tone itself.
