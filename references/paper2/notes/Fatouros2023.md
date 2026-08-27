# Fatouros et al. (2023)

## Local filename
Transforming-sentiment-analysis-in-the-financia_2023_Machine-Learning-with-A.pdf

## Citation
@article{fatouros2023transforming,
  title={Transforming sentiment analysis in the financial domain with ChatGPT},
  author={Fatouros, Georgios and Soldatos, John and Kouroumali, Kalliopi and Makridis, Georgios and Kyriazis, Dimosthenis},
  journal={Machine Learning with Applications},
  volume={14},
  pages={100508},
  year={2023},
  publisher={Elsevier}
}

## Research question
What does the paper ask?
Can a general-purpose LLM using zero-shot prompting outperform a domain-specific transformer (FinBERT) at measuring financially meaningful sentiment?

## Data
2,291 manually labeled forex news headlines, Jan–May 2023, five major FX pairs, with sentiment defined relative to the expected short-term market impact on the relevant currency pair.

## Methods
Zero-shot GPT-3.5 sentiment classification under multiple prompt designs, benchmarked against FinBERT; evaluated using both conventional sentiment-classification metrics and market-based measures including return correlation and directional accuracy.

## Main findings
- ChatGPT substantially outperformed FinBERT on the manually labeled forex-headline sentiment task. The best GPT prompts achieved about 0.79 accuracy/F1, versus roughly 0.56 for FinBERT.
- Prompt design mattered a lot. Prompts that explicitly framed ChatGPT as a financial expert or forex trader and told it which currency pair to evaluate performed better than more generic sentiment-analysis prompts.
- ChatGPT’s sentiment was more closely related to actual market returns than FinBERT’s across most currency pairs, suggesting that its classifications were not only more accurate against human labels but also more economically meaningful.
- The authors conclude that zero-shot LLMs can be competitive or superior to finance-specific transformers without additional fine-tuning, especially when the prompt supplies the relevant financial context.

## Relevance to my paper
Provides evidence that zero-shot LLMs can outperform finance-specific transformers in financial sentiment analysis and that model quality can be evaluated using market returns. It motivates comparing LLMs with traditional approaches, while providing a useful contrast if the relative performance of models differs for Japanese corporate disclosures.

## How I might cite it
- LLMs may outperform domain-specific transformers such as FinBERT in financial sentiment classification.
- Prompt design and financial context can materially affect LLM sentiment performance.
- Financial sentiment models can be evaluated using both human-labeled sentiment and market-based validation such as return correlation and directional accuracy.
- ChatGPT sentiment showed stronger relationships with market returns than FinBERT across most of the tested FX pairs.
- Different setting from my study: short English-language FX news headlines rather than Japanese corporate disclosures.
- Useful contrast if my results do not show LLM superiority, suggesting model performance may depend on language, document type, and financial context.

## Possible literature-review section
Large language models for financial sentiment analysis / LLM-based financial sentiment measurement.

## Important quotes / page numbers
- p. 1 — headline result: “35% enhanced performance in sentiment classification and a 36% higher correlation with market returns.”
- p. 8 — model comparison: they report that the GPT variants consistently outperform FinBERT across all metrics; this is probably the strongest result to cite when contrasting LLMs with domain-specific transformers.
- p. 11 — prompt sensitivity: they conclude that performance depends materially on effective prompt engineering and rigorous evaluation of prompts before deployment.
- p. 7/9 — economic validation: they explicitly evaluate sentiment using both Pearson correlation with returns and directional accuracy, which is particularly relevant for your market-based validation argument.

## Caveats / limitations
- Short sample period: 86 days from January-May 2023.
- Sentiment labels represent expected short-term impact on the relevant FX pair, rather than purely linguistic positive/negative tone.
- English forex news headlines are much shorter and structurally different from long Japanese corporate disclosures.
- Market validation uses daily raw returns, Pearson correlation, and directional accuracy rather than abnormal returns/event-study methodology.
- Results are based on GPT-3.5-turbo and may be model- and prompt-specific.