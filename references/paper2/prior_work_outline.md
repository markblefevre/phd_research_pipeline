# Prior Work

## 1. Foundations of financial sentiment measurement
- [x] Tetlock (2007)
- [x] Tetlock, Saar-Tsechansky & Macskassy (2008)
- [x] Loughran & McDonald (2011)
- [x] Feldman et al. (2010)
- [x] Henry & Leone (2016)
- [x] Glasserman & Mamaysky (2019)

This section establishes the basic premise that financial language contains economically meaningful information, while also showing that measuring that information is not straightforward. Tetlock (2007) and Tetlock, Saar-Tsechansky & Macskassy (2008) provide early evidence that textual tone in financial news is related to market returns, trading activity, firm fundamentals, and subsequent price behavior. Feldman et al. extend the analysis to management disclosure and show that changes in tone can contain information beyond conventional quantitative signals.

The methodological progression then shifts from whether tone matters to how it should be measured. Loughran & McDonald show that generic sentiment dictionaries can perform poorly in financial text because many apparently negative words have neutral or specialized financial meanings. Henry & Leone reinforce the importance of domain-specific measurement and show that the choice of textual measure can materially affect statistical power and economic inference. Importantly, they also caution against assuming that more complex methods are automatically superior. Glasserman & Mamaysky broaden the concept further by showing that sentiment can interact with textual unusualness, suggesting that both the direction and novelty of information may matter.

The central lesson from this section is therefore that financial text contains economically relevant information, but the usefulness of a textual measure depends critically on how financial language is represented and on whether the measure is validated against economically meaningful outcomes.

---

## 2. Machine learning, contextual models, and domain adaptation
- [x] Araci (2019)
- [x] Huang, Wang & Yang (2023)
- [x] Frankel, Jennings & Lee (2022)
- [x] Suzuki et al. (2023)
- [x] Siano (2025)

This section examines methods designed to overcome a fundamental limitation of dictionary approaches: individual word counts generally ignore context. Araci introduces FinBERT and demonstrates that pre-trained transformer models can achieve strong performance on financial sentiment benchmarks. Huang, Wang & Yang further develop the finance-specific transformer approach using large-scale financial-domain pre-training and show that contextual representations can improve the extraction of financially relevant information.

The literature then moves from linguistic classification toward market-based prediction. Frankel, Jennings & Lee use supervised machine-learning methods trained on disclosure text to explain market reactions and generally find that learned textual measures outperform dictionary measures. Siano pushes this approach further by fine-tuning a RoBERTa-based contextual model directly on short-window abnormal stock returns and showing that contextual models can extract substantial market-relevant information from earnings-announcement disclosures.

Suzuki et al. introduce an additional issue that is especially important for Japanese text: domain adaptation is not only about financial terminology but also about language-specific tokenization and representation. Their results show that Japanese financial-domain pre-training and tokenizer adaptation can improve downstream NLP performance, providing a direct reason not to assume that English-language financial models transfer seamlessly to Japanese disclosures.

A key distinction emerges within this literature. Araci and Suzuki primarily evaluate models using supervised NLP labels, while Frankel and Siano train textual models directly on market outcomes. Strong performance in either setting does not necessarily establish that an independently constructed sentiment measure contains incremental economic information. This distinction becomes important when comparing methods under a common economic-validation framework.

---

## 3. Generative LLMs and financial reasoning
- [x] Fatouros et al. (2023)
- [x] Li et al. (2023)
- [x] Chiu & Hung (2025)
- [x] Lopez-Lira & Tang (2026)

This section considers whether generative large language models provide an additional improvement over dictionaries, conventional machine learning, and domain-specific transformers. Unlike many earlier supervised approaches, generative LLMs can often be applied through zero-shot or few-shot prompting without training directly on the target outcome.

Fatouros et al. provide an early comparison between ChatGPT and FinBERT and show that zero-shot GPT-based sentiment can outperform a finance-specific transformer in a financial-news setting. Their results also demonstrate that prompt design and the provision of financial context materially affect performance. Li et al. provide a broader taxonomy of financial LLM applications, distinguishing zero-shot and few-shot prompting, fine-tuning, tool augmentation, and finance-specific pre-training.

Chiu & Hung extend this literature to long financial disclosures by combining summarization with a finance-adapted LLaMA model and comparing its sentiment signals with FinBERT, dictionary approaches, and traditional machine-learning methods. Lopez-Lira & Tang provide one of the strongest direct model comparisons, examining GPT-4 alongside earlier GPT models, BERT-family models, FinBERT, Llama variants, RavenPack, and dictionary methods. Their results suggest that greater model sophistication can translate into stronger economic performance, particularly when models are evaluated against immediate market reactions and subsequent return drift.

The progression in this section therefore raises a central empirical question: do modern LLMs genuinely extract more economically meaningful financial information, or do their apparent advantages depend on the language, document type, prompt, return horizon, and evaluation design?

---

## 4. Japanese financial text and closest empirical precedents
- [ ] Manabe et al.
- [ ] Nakatsuka
- [x] Suzuki et al. (2023)
- [x] Okada et al. (2025)

This section narrows the discussion from financial NLP generally to the Japanese setting. The purpose is not simply to collect studies using Japanese text, but to establish what is already known about the transfer of financial sentiment methods to Japanese financial disclosures and where the closest empirical precedents lie.

Suzuki et al. provide the methodological foundation by showing that Japanese financial-domain pre-training and tokenizer construction matter for downstream NLP performance. Their results demonstrate that Japanese financial NLP poses language-specific problems that cannot necessarily be addressed by directly importing English-language models or tokenization choices. However, their evaluation is primarily based on supervised NLP tasks rather than realized stock-market outcomes.

Okada et al. provide the closest direct comparison to the current study. They analyze Japanese annual securities reports and compare a Japanese financial polarity dictionary, a domain-adapted DeBERTaV2 model, and several general-purpose generative LLMs, including GPT, Claude, and Gemini. Their market-based results show that the relative ranking of methods can differ substantially when evaluated economically rather than through conventional sentiment labels.

Okada therefore substantially narrows the available research gap. The remaining question is not whether LLM-based sentiment can be applied to Japanese regulatory disclosure. Rather, the opportunity is to examine competing sentiment methodologies under a more directly comparable event-study framework and to determine whether more sophisticated models provide incremental information about the market reaction surrounding disclosure itself, rather than primarily longer-horizon return predictability.

---

## 5. Textual change, novelty, and disclosure informativeness
- [x] Brown & Tucker (2011)
- [x] Dyer, Lang & Stice-Lawrence (2017)
- [x] Cohen, Malloy & Nguyen (2020)
- [ ] Nakatsuka & Suimon (2026)

This section introduces a related but distinct dimension of textual information: the amount and location of new information contained in a disclosure. Sentiment measures typically ask whether language is positive or negative, but long regulatory filings contain substantial repeated, boilerplate, and persistent text. A document-level sentiment score may therefore be dominated by material that has changed little from prior disclosures.

Brown & Tucker show that year-over-year changes in MD&A contain economically meaningful information and that greater textual modification is associated with stronger market reactions. Dyer, Lang & Stice-Lawrence document the increasing prevalence of disclosure length, redundancy, boilerplate, and stickiness, providing an important explanation for why the full level of disclosure text may contain substantial noise. Cohen, Malloy & Nguyen go further by showing that changes between successive filings predict future firm outcomes and returns, consistent with investors underreacting to information embedded in textual changes.

This literature suggests that sentiment and novelty are complementary rather than competing constructs. Sentiment describes the direction of the information, while textual novelty or change describes how much new information has been introduced. The economically relevant signal may therefore lie disproportionately in changed portions of a disclosure rather than in the document as a whole. This creates a natural motivation for considering whether sentiment measurement becomes more informative when combined with measures of textual change or novelty.

---

## 6. Research gap

The existing literature establishes several important findings. Financial language contains information associated with market outcomes, and domain-specific sentiment resources generally provide cleaner measures than generic dictionaries. Contextual machine-learning and transformer models can improve linguistic classification and, in some settings, explain market reactions more effectively than word-count approaches. More recent generative LLMs can sometimes outperform both dictionaries and domain-specific transformers, suggesting that richer semantic reasoning may provide incremental value.

However, several issues remain unresolved.

First, NLP benchmark performance and economic informativeness are not the same concept. A model may reproduce human sentiment labels accurately without producing a stronger relation with realized market outcomes. Conversely, models trained directly on abnormal returns, such as some supervised contextual approaches, may achieve strong market fit precisely because the market outcome is used to construct the textual measure. This makes them conceptually different from sentiment measures built independently and then evaluated against returns.

Second, the relative performance of dictionary, transformer, and generative-LLM approaches has rarely been examined under a common economic-validation design using the same underlying documents and the same market-response specification. Differences reported across studies may therefore reflect differences in text source, language, labeling scheme, prompt design, return horizon, or training objective rather than model quality alone.

Third, Japanese evidence remains considerably more limited than the English-language literature. Suzuki establishes the importance of Japanese financial-domain adaptation, while Okada provides an important direct comparison of modern sentiment methods on Japanese annual securities reports. However, Okada focuses primarily on longer-horizon return predictability and portfolio performance. This leaves room for a more direct comparison of competing sentiment measures using contemporaneous or short-window abnormal returns surrounding Japanese regulatory disclosures.

Finally, prior work on disclosure change suggests that economically relevant information may be concentrated in newly introduced or modified text. Document-level sentiment and textual novelty may therefore capture different dimensions of information, and their interaction may help explain why some sentiment measures are economically stronger than others.

The resulting research gap is therefore not simply whether modern models can perform sentiment analysis on Japanese financial text. It is whether increasingly sophisticated sentiment methods provide incremental economic information when compared on the same Japanese regulatory disclosures under a common market-based validation framework, and whether that information is affected by the novelty or year-over-year change in the underlying disclosure.

---

## 7. Overall story arc

- **Section 1 — Establish the measurement problem.** Financial text contains economically meaningful information, but generic sentiment measures can misinterpret specialized financial language. Tetlock establishes the importance of textual tone, while Loughran & McDonald and Henry & Leone show that domain-specific measurement matters. Henry & Leone also provide an early warning that methodological complexity does not automatically produce a better economic measure.

- **Section 2 — Introduce contextual modeling as a response to dictionary limitations.** FinBERT and related transformer approaches capture word context that dictionaries ignore. Huang strengthens the case for large-scale finance-specific pre-training, while Suzuki shows that Japanese financial NLP introduces additional domain and tokenization issues. Frankel and Siano demonstrate that contextual models can explain market reactions extremely well, but because these approaches use realized returns as training targets, their results are not directly equivalent to externally validating an independently constructed sentiment measure.

- **Section 3 — Ask whether generative LLMs represent another meaningful step forward.** Fatouros shows that zero-shot ChatGPT can outperform FinBERT in some settings, Li organizes the emerging financial-LLM landscape, Chiu applies finance-adapted LLaMA models to long disclosures, and Lopez-Lira & Tang provide broad evidence that more capable LLMs can extract economically relevant information that simpler models miss. The question shifts from whether LLMs can perform sentiment analysis to whether their greater semantic sophistication produces genuinely superior economic signals.

- **Section 4 — Bring the methodological debate into the Japanese setting.** Suzuki demonstrates that Japanese financial language requires specific adaptation, while Okada provides the closest existing comparison of dictionaries, transformers, and generative LLMs on Japanese regulatory disclosures. Okada therefore establishes that the comparison is economically meaningful while also sharpening the remaining contribution: a common event-study framework focused on the market reaction surrounding disclosure rather than primarily long-horizon portfolio predictability.

- **Section 5 — Add the information-content dimension.** Brown & Tucker, Dyer, and Cohen show that long financial disclosures contain substantial repeated and boilerplate language and that what changes from one disclosure to the next can itself be economically informative. This suggests that sentiment level alone may not identify where the economically relevant information resides. Sentiment captures direction; textual novelty captures newness.

- **Section 6 — Arrive at the research question.** The literature progresses from dictionaries, to contextual models, to generative LLMs, but model comparisons are frequently made across different datasets, languages, objectives, and validation schemes. The unresolved issue is whether these methods differ materially in their ability to capture economically relevant information when applied to the same Japanese regulatory disclosures and judged using the same market-based outcome. A further question is whether textual novelty helps explain or strengthen that relationship.