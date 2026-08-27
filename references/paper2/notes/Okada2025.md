# Okada et al. (2025)

## Local filename
Okada_et_al_2025_From_Words_to_Returns.pdf

## Citation
@article{okada2025words,
  title={From words to returns: sentiment analysis of Japanese 10-K reports using advanced large language models},
  author={Okada, Katsuhiko and Nakasuji, Moe and Tsukioka, Yasutomo and Yamasaki, Takahiro},
  journal={PeerJ Computer Science},
  volume={11},
  pages={e3349},
  year={2025},
  publisher={PeerJ Inc.}
}

## Research question
What does the paper ask?

Can advanced large language models extract sentiment from Japanese annual securities reports that predicts future stock returns better than traditional dictionary methods and a domain-adapted transformer? More specifically, the paper compares GPT-4o-mini, Claude 3 Haiku, and Gemini 1.5 Flash with a Japanese financial polarity dictionary and a chABSA-fine-tuned DeBERTaV2 model, asking whether the resulting sentiment signals contain economically meaningful information not already reflected in stock prices.

## Data
The study analyzes Japanese annual securities reports filed through EDINET over 2014–2023, focusing on firms with March fiscal year-ends.

From 2014–2022 the sample universe is the former Tokyo Stock Exchange First Section; for 2023 it changes to the Prime Market following the TSE market restructuring. The paper reports a final MD&A analysis sample of 11,135 firm-year observations and more than 70 million words.

The paper also states that the initial EDINET collection produces 16,363 firm-year observations and more than 90 million words before the final MD&A analysis sample. The exact reconciliation between the 16,363 initial observations and the reported 11,135 final observations is not clearly explained in the main text and should therefore be treated cautiously.

The average MD&A section in the final sample contains roughly 6,636 Japanese characters.

## Methods
The authors extract the Management Discussion and Analysis-equivalent section from Japanese annual securities reports and calculate sentiment using six measures from five methodological approaches:

- **Tone Ratio:** based on the University of Tokyo Financial Polarity Dictionary, comparing positive and negative word counts.
- **Tone Score:** the sum of real-valued sentiment scores assigned by the same dictionary.
- **DeBERTaV2:** a Japanese-language DeBERTaV2 model fine-tuned on the chABSA financial sentiment dataset. The authors restrict the training sample to 2,227 sentences with uniformly positive or negative annotations and construct a document score from sentence-level sentiment probabilities.
- **GPT-4o-mini:** specifically `gpt-4o-mini-2024-07-18`.
- **Claude 3 Haiku:** specifically `claude-3-haiku-20240307`.
- **Gemini 1.5 Flash:** `gemini-1.5-flash`.

The three generative LLMs receive Japanese prompts instructing them to act as securities analysts and assign positive and negative sentiment scores summing to 100. The prompt contains explicit rules and positive/negative examples. The original Japanese MD&A text is passed directly to the models rather than translated or summarized.

For economic validation, firms are ranked each June using sentiment from the preceding March fiscal-year filing. The primary portfolios are value-weighted: the top 20% by sentiment form the long portfolio and the bottom 20% form the short portfolio, held from July of year t through June of year t+1.

The authors then evaluate monthly portfolio returns using the Fama-French three-factor model, Carhart four-factor model, and Fama-French five-factor model. Newey-West standard errors with a six-month lag are used.

Robustness tests vary portfolio thresholds (top/bottom 10%, 20%, and 30%), use equal-weighted portfolios, examine TOPIX 100 firms, incorporate a 1.5% round-trip transaction cost, and exclude 2023 to remove the effect of the TSE market restructuring.

## Main findings
The paper finds a strong and unexpected **contrarian** relation between LLM-derived sentiment and future returns.

The authors initially hypothesize that more positive sentiment should predict higher future returns. Instead, firms receiving the most positive scores from GPT-4 and Claude subsequently underperform, while firms receiving the most negative scores outperform. As a result, the conventional high-sentiment-minus-low-sentiment portfolios generate significantly negative returns.

Under the Fama-French three-factor specification, the high-minus-low GPT-4 portfolio has an annualized alpha of approximately -5.95%, while the Claude portfolio has an annualized alpha of approximately -9.15%. These negative alphas remain significant under Carhart four-factor and Fama-French five-factor specifications.

Dictionary-based Tone Ratio and Tone Score measures do not generate significant abnormal returns. The DeBERTaV2 measure also fails to produce significant alpha. Gemini produces weaker results than GPT-4 and Claude and is generally insignificant after risk adjustment.

The authors interpret the negative relation as evidence that strongly positive managerial sentiment may be associated with overvaluation that subsequently corrects, while negatively worded disclosures may identify firms that are subsequently underpriced.

Robustness is mixed but generally supportive. Results remain similar under alternative portfolio cutoffs and when 2023 is excluded. However, equal-weighted tests reveal meaningful model differences: GPT-4 becomes insignificant in the full equal-weighted sample while Claude retains some significance; among TOPIX 100 stocks, GPT-4 becomes strongly significant while Claude becomes insignificant. The authors attribute part of Claude's instability to highly concentrated portfolios.

When the authors reverse the trading direction to exploit the discovered contrarian relation—buying low-sentiment stocks and shorting high-sentiment stocks—and impose a 1.5% round-trip transaction cost, GPT-4 and Claude strategies remain profitable over the sample.

## Relevance to my paper
This is one of the most directly relevant papers to my study because it combines all three dimensions central to my research: Japanese financial disclosures, competing sentiment methodologies, and market-based economic validation.

The paper provides a particularly close benchmark because it directly compares a Japanese finance-specific dictionary, a domain-adapted transformer, and several general-purpose LLMs on the same underlying Japanese disclosure texts. This creates a methodological progression from dictionary methods to transformers to generative LLMs that closely parallels the structure of my literature review.

It also strongly supports the argument that classification sophistication and economic usefulness are not the same thing. The DeBERTaV2 model is specifically fine-tuned on Japanese financial sentiment labels, yet it does not produce significant return-predictive alpha, while GPT-4 and Claude do. This suggests that performance on a labeled sentiment task does not guarantee stronger economic validity.

The paper is also important because its main result is not simply that LLM sentiment predicts returns, but that the sign is opposite the intuitive hypothesis. Positive LLM sentiment predicts lower subsequent returns. This raises an important interpretation issue for my study: a sentiment measure can be economically informative even when its relationship with returns is contrarian rather than monotonic in the expected direction.

Unlike Frankel et al., the LLM sentiment measures are not trained directly on realized returns. The market tests therefore function as an external economic-validation exercise rather than as the outcome used to construct the sentiment measure itself.

The paper also provides strong precedent for evaluating model families using portfolio and asset-pricing tests rather than only short-window event-study regressions.

## How I might cite it
- Advanced general-purpose LLMs can extract return-predictive information from Japanese corporate disclosures that is not detected by traditional dictionary methods.
- Japanese LLM sentiment measures can be economically informative even when the relation with future returns is contrarian.
- GPT-4 and Claude sentiment extracted from Japanese annual-report MD&A sections predicts subsequent abnormal returns, whereas dictionary measures and a chABSA-fine-tuned DeBERTaV2 model do not.
- Performance on supervised financial sentiment labels does not necessarily imply stronger market-based economic validity.
- General-purpose LLMs can outperform a Japanese domain-adapted transformer in extracting economically relevant information from financial disclosures.
- Market-based validation may reveal information about a sentiment measure that conventional classification benchmarks cannot.
- The study provides direct evidence from a non-English financial market and is an important benchmark for Japanese financial NLP.
- The paper provides a useful comparison between dictionary, transformer, and generative-LLM approaches on the same underlying disclosure setting.
- Useful contrast for my paper: Okada et al. focus on one-year return predictability and portfolio alphas, whereas my analysis may emphasize event-window abnormal returns and differences among sentiment measures around disclosure dates.

## Possible literature-review section
Primary: Japanese financial-text research

Secondary: Large language models; sentiment and market reaction; transformer versus LLM sentiment; economic validation of NLP measures

## Important quotes / page numbers
- p. 1 — Abstract: the authors report that dictionary sentiment has no significant relationship with subsequent returns, while LLM-derived sentiment has a significant negative relationship with future stock returns.
- pp. 2–3 — Core interpretation: high LLM sentiment is associated with subsequent underperformance, which the authors interpret as possible overvaluation followed by correction.
- p. 3 — Contribution: the authors describe the study as the first large-scale comparison of GPT-4, Claude, and Gemini on Japanese annual securities-report narrative disclosures.
- pp. 6–7 — Data: the analysis covers 2014–2023 Japanese EDINET filings and the final reported MD&A sample contains 11,135 firm-years.
- pp. 7–11 — Sentiment methods: the paper details the Japanese financial polarity dictionary, chABSA-fine-tuned DeBERTaV2, GPT-4o-mini, Claude 3 Haiku, and Gemini 1.5 Flash.
- p. 10 and pp. 26–27 — LLM prompt: models are instructed in Japanese to act as securities analysts and allocate positive and negative scores summing to 100 using supplied examples.
- pp. 12–14 — Portfolio construction and main visual result: firms are sorted annually into sentiment portfolios; GPT-4 and Claude high-sentiment-minus-low-sentiment portfolios produce sustained negative cumulative returns.
- pp. 15–16 — Asset-pricing result: GPT-4 and Claude produce statistically significant negative alphas under FF3, Carhart four-factor, and FF5 models; dictionary, DeBERTaV2, and Gemini alphas are insignificant.
- pp. 18–22 — Robustness: alternative cutoffs, equal-weighted portfolios, TOPIX 100 tests, transaction costs, and exclusion of 2023 are examined.
- p. 22 — Transaction-cost result: after reversing the strategy to exploit the contrarian signal and applying a 1.5% round-trip cost, GPT-4 and Claude remain profitable over the sample.
- pp. 22–23 — Conclusion: the authors argue that advanced LLMs extract information from Japanese public disclosures that is not fully incorporated into prices.

## Caveats / limitations
The paper reports two different sample sizes: an initial EDINET collection of 16,363 firm-year observations and a final analysis sample of 11,135 firm-years, but the main text does not clearly reconcile the reduction. This should be checked carefully before citing the sample construction in detail.

The sample is narrower than the phrase “all companies listed on the Tokyo Stock Exchange” can imply. It is restricted to firms with March fiscal year-ends, uses the TSE First Section from 2014–2022, and switches to the Prime Market in 2023. The authors show that excluding 2023 does not alter the main result, but the universe is not the full Japanese listed market.

The LLM comparison is sensitive to the specific model versions and prompt used. GPT-4o-mini, Claude 3 Haiku, and Gemini 1.5 Flash are proprietary models whose behavior may change across versions, and their relative rankings should not automatically be generalized to newer LLM generations.

The LLM sentiment scores are heavily prompt-conditioned. The prompt explicitly instructs the model to behave as a securities analyst, forces positive and negative scores to sum to 100, and supplies examples of fully positive and fully negative disclosures. The resulting measure is therefore not a model-independent notion of “sentiment”; it reflects the interaction of model, prompt, examples, and scoring rule.

The scoring scales differ substantially across methods. Dictionary Tone Ratio, cumulative Tone Score, DeBERTaV2 probabilities, and LLM 0–100 allocations are not directly comparable as cardinal sentiment measures. The portfolio-ranking approach mitigates this issue but does not eliminate differences in score dispersion or ties.

The DeBERTaV2 comparison may disadvantage the transformer benchmark. It is fine-tuned on only 2,227 unambiguously positive or negative chABSA sentences, while the LLMs bring much broader pre-training and are prompted at the full-document level. Thus, the comparison is informative but not a controlled test of transformer architecture versus generative LLM architecture.

The robustness results are not uniform across weighting schemes and firm-size subsets. GPT-4 loses significance in the full equal-weighted sample, while Claude loses significance in the TOPIX 100 equal-weighted test. This suggests that the return signal depends partly on portfolio construction, firm size, and model-specific score distributions.

The sample spans only ten annual portfolio formations. Although the underlying panel contains many firm-years, asset-pricing inference is ultimately based on a relatively short 2014–2023 time period, limiting the number of independent market regimes available for testing long-horizon predictability.

The paper's interpretation in terms of market inefficiency should be treated cautiously. Predictive portfolio alpha is consistent with delayed incorporation of qualitative information, but it does not by itself establish why the anomaly exists or rule out omitted risk, model-selection effects, data-snooping, or other explanations.

Transaction-cost analysis uses a uniform 1.5% round-trip assumption. This is useful as a robustness check but does not fully model stock-specific shorting costs, borrow availability, market impact, turnover, or implementation constraints.

Finally, the authors repeatedly refer to Japanese annual securities reports as “10-K reports.” They are analogous to U.S. 10-K filings but are not literally SEC Form 10-Ks. For my paper, it is safer to refer to them as Japanese annual securities reports (有価証券報告書) or EDINET filings and note the MD&A-equivalent section.
