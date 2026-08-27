# Lopez-Lira (2026)

## Local filename
LopezLiraTang2026.pdf

## Citation
@article{lopez2026can,
  title={Can chatgpt forecast stock price movements? return predictability and large language models},
  author={Lopez-Lira, Alejandro and Tang, Yuehua},
  journal={Journal of Financial Economics},
  volume={184},
  pages={104335},
  year={2026},
  publisher={Elsevier}
}

## Research question
The paper asks whether large language models—especially GPT-4—can infer the economic implications of firm-specific news headlines well enough to predict (1) the market's immediate reaction and (2) subsequent short-horizon return drift. More broadly, it asks whether LLMs can be used as instruments for studying how efficiently markets process information, and whether predictive ability varies with model sophistication, firm characteristics, and news type.

## Data
- U.S. common stocks listed on NYSE, NASDAQ, and AMEX.
- Sample period: October 2021 to May 2024, deliberately after the September 2021 knowledge cutoff of the GPT-4 snapshot used in the study.
- 159,137 firm-headline-date observations covering 4,123 companies (about 85% of CRSP firms over the period).
- Roughly 67.5% of headlines are press releases and 32.5% are news articles.
- About 82% of headlines are classified as overnight news and 18% as intraday news.
- Primary datasets: CRSP daily prices/returns, TAQ intraday prices, web-scraped news headlines, and RavenPack for matching, timestamps, relevance, and benchmark sentiment.
- The main sample requires RavenPack relevance = 100 and filters repeated/similar news and headlines that simply describe stock gains/losses.

## Methods
- GPT-4 is prompted zero-shot to classify each headline as good, bad, or uncertain for the named company's short-term stock price. Responses are mapped to +1, -1, and 0; temperature is set to 0.
- Main GPT-4 model: `gpt-4-0314`, whose training data cutoff is September 2021.
- The authors separately evaluate the immediate market reaction and the subsequent price drift. Overnight news is emphasized because it dominates the sample; intraday news is analyzed with TAQ data.
- Portfolio tests form daily long-short strategies that buy stocks with positive LLM assessments and short stocks with negative assessments. Performance is evaluated with hit rates, mean returns, Sharpe ratios, transaction-cost scenarios, and Fama-French 5-factor alphas.
- Predictive regressions relate initial returns and subsequent drift to GPT-4 scores, with firm and date fixed effects and standard errors double-clustered by firm and date.
- Heterogeneity tests examine small vs. larger stocks and positive vs. negative news.
- A topic/interpretability framework clusters news into themes and compares GPT-4 alignment with the immediate reaction versus subsequent drift to identify where markets incorporate information quickly or slowly.
- The study compares GPT-4 with GPT-3.5, GPT-1, GPT-2, BERT, BERT-Large, BART-Large, DistilBART-MNLI, Llama2 variants, and FinBERT, as well as RavenPack and dictionary methods including Loughran-McDonald.
- The authors also compare zero-shot conversational LLM classification with supervised embedding-based return-prediction models trained in rolling windows.
- A theoretical model treats LLMs as information processors and combines heterogeneous information-processing capacity, delayed information diffusion, noise trading, limits to arbitrage, and LLM adoption.

## Main findings
- GPT-4 predicts the direction of the immediate market reaction extremely well at the daily portfolio level: about 93.3% for overnight news and 88.8% for intraday news.
- GPT-4 scores also predict subsequent one-to-two-day price drift, although much less strongly than the immediate response. The overnight long-short drift strategy earns about 34 bps per day before transaction costs with an annualized Sharpe ratio of 2.97; the corresponding intraday Sharpe ratio is 2.63.
- Predictability is substantially stronger for smaller stocks and negative news, consistent with delayed information diffusion and limits to arbitrage.
- In regressions, GPT-4 remains significant for subsequent drift even when RavenPack sentiment is included; RavenPack becomes insignificant for drift in the joint specification.
- Market processing differs by news topic. Earnings/revenue reports, strategic partnerships, and clinical-trial announcements show strong initial alignment with GPT-4 but little subsequent drift, suggesting relatively efficient incorporation. Insider transactions, dividend announcements, and healthcare-conference presentations show significant drift in the direction GPT-4 identifies, suggesting underreaction.
- Model capability matters. GPT-4 has the strongest overall performance, with GPT-3.5 and DistilBART-MNLI below it and basic models such as GPT-1, GPT-2, and BERT performing poorly. The authors interpret this as evidence of a threshold in model sophistication/semantic reasoning ability.
- FinBERT is an especially useful comparison: it achieves about 90% hit rate for the initial reaction but only 48% for subsequent drift, with a negative drift-strategy Sharpe ratio (-0.33). It also assigns neutral to about 75% of overnight news, so its directional coverage is much lower than GPT-4's.
- RavenPack is the strongest non-LLM benchmark, with an initial-reaction hit rate around 88% and drift-strategy Sharpe ratio of 2.50, while traditional dictionary methods perform substantially worse.
- Zero-shot GPT-4 is relatively robust when the labeled training sample is small, whereas supervised embedding approaches deteriorate materially in the smaller intraday sample.
- Trading profitability is sensitive to implementation costs. The zero-cost strategy is extremely strong, remains profitable at 5-10 bps round-trip assumptions, but becomes unprofitable at 20 bps; daily turnover is very high.
- Strategy performance declines over time as LLM capabilities and adoption increase. The authors view this as suggestive evidence that wider use of LLMs improves price efficiency and erodes the very mispricing that makes the signal profitable.

## Relevance to my paper
This is highly relevant because it directly links modern LLM-based financial-text interpretation to observed market reactions rather than evaluating sentiment classification only against human labels.

The paper provides strong evidence that a general-purpose LLM can extract economically meaningful sentiment/information from financial news without domain-specific fine-tuning and that this signal is associated both with the contemporaneous market response and subsequent abnormal return behavior. That makes it a useful benchmark for arguing that financial-text models should ultimately be evaluated by economic outcomes, not merely NLP classification accuracy.

It is also particularly useful for positioning comparisons among dictionaries, domain-specific transformers, and LLMs. Lopez-Lira and Tang explicitly compare GPT-4 with FinBERT, BERT-family models, Llama2, RavenPack, and Loughran-McDonald-style dictionary measures. Their results suggest that domain-specific training alone is not sufficient: FinBERT captures much of the immediate directional reaction but fails to predict subsequent drift, whereas GPT-4 performs strongly on both dimensions.

For my study, the important distinction is that Lopez-Lira and Tang analyze short U.S. news headlines and very short-horizon U.S. equity returns, whereas I analyze Japanese financial text/documents. Their findings therefore motivate, but do not establish, that LLM superiority will carry over to longer Japanese corporate disclosures or to my specific return horizon and experimental setup.

## How I might cite it
- To motivate LLMs in financial sentiment/return prediction: Lopez-Lira and Tang (2026) show that an off-the-shelf GPT-4 model can infer the short-term economic implications of firm-specific news headlines and that its classifications predict both immediate market reactions and subsequent return drift.
- To support economic validation of NLP sentiment measures: Rather than evaluating text classifications only against labeled sentiment, Lopez-Lira and Tang (2026) validate LLM assessments directly against realized market reactions and post-announcement returns.
- To compare modern models with domain-specific transformers: Lopez-Lira and Tang (2026) find that GPT-4 substantially outperforms simpler language models and that FinBERT, despite strong accuracy for the initial market reaction, does not predict subsequent drift.
- To connect textual sentiment with market underreaction: Lopez-Lira and Tang (2026) interpret return continuation following GPT-4-classified news as evidence of delayed information incorporation, particularly for small firms and negative news.
- To motivate model-complexity tests: Their cross-model evidence suggests that the ability to interpret economically relevant news emerges only after language models exceed a threshold of sophistication.

## Possible literature-review section
Primary: **3. Large language models**

Secondary: **4. Sentiment and market reaction**

Possible cross-reference in **2. Transformers and domain-specific language models**, because the paper directly compares GPT-4 with BERT and FinBERT and provides a useful bridge from domain-specific transformers to general-purpose LLMs.

## Important quotes / page numbers
- pp. 2-3: Framing of LLMs as an instrument for studying market information processing; GPT-4 is used to compare the economic implications of news with the market's initial response and subsequent drift.
- pp. 3-4: Core result that GPT-4 assessments predict one-to-two-day return drift and that underreaction is stronger for smaller firms and negative news.
- pp. 13-16: Data construction and exact GPT-4 prompting methodology, including use of post-knowledge-cutoff data and the YES/NO/UNKNOWN classification.
- pp. 18-23: Main portfolio and regression evidence for immediate reaction and subsequent drift.
- pp. 24-25: Topic-level evidence distinguishing news that is incorporated efficiently from news associated with underreaction.
- pp. 26-28 / Table 5: Most important model-comparison section. GPT-4 has the strongest overall performance; FinBERT has about 90% initial-reaction hit rate but only 48% drift hit rate and a -0.33 Sharpe ratio; RavenPack is the strongest non-LLM benchmark.
- pp. 37-38: Conclusion: the authors summarize the evidence as showing that financial reasoning is an emerging capability of more sophisticated LLMs and argue that wider LLM adoption may improve price discovery.

## Caveats / limitations
- The paper studies U.S. equity news headlines, not long-form corporate disclosures; results may not generalize to longer documents, other languages, or other markets.
- The principal task is highly specific: classify whether one headline is good or bad for one company's stock price in the short term. This is not identical to general financial sentiment classification.
- Immediate-reaction hit rates near 90% are portfolio-day hit rates, not headline-level classification accuracy. They should not be interpreted as saying GPT-4 correctly classifies 90%+ of individual headlines.
- The strongest trading results are before transaction costs and involve very high turnover. Performance deteriorates materially as trading costs rise, and a 20 bps round-trip assumption makes the baseline strategy unprofitable.
- Much of the drift profitability comes from the short side and is strongest in smaller stocks, where shorting, liquidity, and price-impact constraints are particularly relevant.
- FinBERT's comparison with GPT-4 is not perfectly apples-to-apples because FinBERT assigns neutral to roughly 75% of overnight observations, producing far fewer directional signals.
- RavenPack's methodology is proprietary and not fully disclosed, so its benchmark is difficult to interpret precisely and may itself contain sophisticated machine-learning components.
- The authors take considerable care with the LLM knowledge-cutoff issue, but memorization/look-ahead bias is an inherent concern in retrospective LLM studies. Their post-cutoff sample and GPT-4-vs.-GPT-3.5 tests mitigate rather than conceptually eliminate that concern.
- The claim that larger/more complex models perform better should be treated cautiously because model families differ in architecture, training data, instruction tuning, and objectives—not only parameter count. The paper's within-family Llama2 comparisons provide cleaner evidence than cross-family size comparisons.
- The reported GPT-4 parameter count is an external estimate rather than an officially disclosed figure, so model-size interpretations should not depend too literally on that number.
- The evidence that wider LLM adoption improves market efficiency is suggestive rather than causal: declining strategy Sharpe ratios coincide with increasing LLM adoption, but other market changes could also contribute.
