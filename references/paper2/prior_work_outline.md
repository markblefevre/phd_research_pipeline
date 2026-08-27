# Prior Work

## 1. Dictionary-based financial sentiment
- [x] Loughran & McDonald (2011)
- [x] Henry & Leone (2016)

## 2. Machine learning and transformer-based financial sentiment
- [x] Araci (2019)
- [x] Suzuki et al. (2023)
- [x] Frankel, Jennings & Lee (2021)
- [x] Siano (2025)
- [x] Huang, Wang & Yang (2023)

## 3. Large language models
- [x] Fatouros et al. (2023)
- [x] Li et al. (2023)
- [x] Chiu & Hung (2025)
- [x] Lopez-Lira & Tang (2026)

## 4. Economic validation: sentiment and market outcomes
- [x] Tetlock (2007)
- [x] Tetlock, Saar-Tsechansky & Macskassy (2008)
- [x] Feldman et al. (2010)
- [x] Glasserman & Mamaysky (2019)
- [x] Frankel, Jennings & Lee (2021)
- [x] Okada et al. (2025)
- [x] Chiu & Hung (2025)
- [x] Huang, Wang & Yang (2023)

## 5. Japanese financial-text research
- [ ] Manabe et al.
- [ ] Nakatsuka
- [x] Suzuki et al. (2023)
- [x] Okada et al. (2025)

## 6. Textual novelty
- [x] Brown & Tucker (2011)
- [x] Cohen, Malloy & Nguyen (2020)
- [x] Dyer, Lang & Stice-Lawrence (2017)
- [ ] Nakatsuka & Suimon (2026)

## 7. Research gap
We know domain-specific dictionaries matter.
We know contextual models can improve NLP performance.
We have evidence that LLMs sometimes outperform older methods economically.
But some of the strongest “contextual model” results train directly on returns.
We have relatively little comparable evidence in Japanese regulatory disclosure.
And disclosure novelty/change may interact with sentiment measurement.

## 8 Overall story arc
- Section 1: Domain matters. Generic dictionaries can measure financial tone poorly. Loughran–McDonald establish this; Henry–Leone reinforce it and show that measurement choice affects event-study power.
- Section 2: More sophisticated methods attempt to overcome dictionary limitations. FinBERT introduces contextual representation; Suzuki shows Japanese financial-domain adaptation matters; Frankel and Siano show supervised contextual methods can explain market reactions extremely well — but crucially, Frankel and Siano train on the market outcome itself, which complicates comparison with independent sentiment measures. Frankel explicitly trains its ML models on CAR[0,1], as does Siano.
- Section 3: Generative LLMs remove the need for traditional supervised task training in some settings. Fatouros shows zero-shot GPT can beat FinBERT; Li provides the taxonomy; Chiu uses finance-adapted LLaMA.
- Section 4: This becomes the conceptual center of the literature review: How should we decide whether a sentiment measure is actually good? Tetlock, Feldman, Henry, Glasserman, Frankel, Siano, Chiu and Okada give you different forms of economic validation. And they don't all ask exactly the same thing.
- Section 5: Then comes the key restriction: very little of that evidence is Japanese. Suzuki establishes Japanese domain adaptation; Okada comes closest to your model comparison but studies long-horizon predictability rather than your specific event-study question.
- Section 6: Finally, Brown/Cohen/Dyer/Glasserman tell you that what changed may matter separately from whether the document sounds positive or negative. Brown directly measures year-over-year MD&A modifications, while Cohen compares successive filings and Dyer documents massive disclosure stickiness.