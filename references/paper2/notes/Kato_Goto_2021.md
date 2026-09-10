# Kato & Goto (2021)

## Local filename
kk40-3-2.pdf

## Citation
@article{加藤大輔2021有価証券報告書のテキスト分析,
  title={有価証券報告書のテキスト分析: 経営者による将来見通しの開示と将来業績},
  author={加藤大輔 and 五島圭一},
  journal={金融研究},
  volume={40},
  number={3},
  pages={45--75},
  year={2021},
  publisher={東京: 日本銀行金融研究所}
}

English title translation:
**Text Analysis of Annual Securities Reports: Managers' Disclosure of Future Outlook and Future Performance**

Authors: Daisuke Kato and Keiichi Goto.

## Research question
The paper asks whether qualitative information in the MD&A sections of Japanese annual securities reports (有価証券報告書) contains managers' forward-looking views that are informative about future firm performance.

More specifically:

1. Does MD&A tone contain information useful for predicting next-period firm performance?
2. After removing the component of tone that can be explained by currently observable firm information, does the remaining "adjusted tone" proxy for managers' qualitative future outlook?
3. Does the predictive information in this adjusted tone differ across industries or firm size?
4. Did the 2018 restructuring and expansion of Japanese MD&A disclosure requirements affect the informativeness of managers' future-outlook disclosure?

The paper explicitly follows Li (2010), but it does **not** identify forward-looking statements by extracting sentences containing forward-looking expressions. Instead, it measures tone over the full MD&A and statistically removes the portion explained by currently observable information. The residual is interpreted as a proxy for managers' qualitative future outlook.

## Data
- Japanese annual securities reports (有価証券報告書) obtained from the Financial Services Agency's EDINET system.
- Reports filed from May 14, 2014 through June 30, 2019.
- Approximately 3,400 firms and approximately 20,000 annual securities reports.
- The empirical regression sample is smaller after data requirements; the principal future-performance tests report 14,855 observations.
- The analysis focuses on MD&A-related narrative sections of the securities reports.
- Because the organization of the MD&A-related sections changed during the sample period, the authors construct comparable text blocks before and after the regulatory changes.
- Financial data are taken from EDINET where available, with additional financial and market data obtained from Bloomberg.
- Japanese text is extracted from HTML filings and tokenized using MeCab.

## Methods
### 1. Japanese financial sentiment dictionary
The authors use a polarity-dictionary approach based on a Japanese translation of the Loughran and McDonald (2011) financial sentiment dictionary.

After removing duplicates created by translation, the Japanese dictionary contains:
- 255 positive terms
- 1,374 negative terms
- 1,629 terms in total.

Tone is calculated as:

`TONE = (N_positive - N_negative) / (N_positive + N_negative)`

so the score lies between -1 and +1.

### 2. Constructing "adjusted tone" as a proxy for managers' future outlook
This is the most distinctive methodological step.

Following the logic of Li (2010), the authors assume that observed MD&A tone contains:
- a component explainable by information already observable at time t; and
- a component reflecting managers' qualitative information or subjective outlook about the future.

They regress current MD&A tone on currently observable financial, market, and firm characteristics. Controls include current ROA, stock return, accruals, size, market-to-book, return volatility, ROA volatility, segment information, firm age, special gains/losses, and readability.

The residual:

`TONE_ADJ = actual TONE - predicted TONE`

is interpreted as **adjusted tone**, a proxy for the qualitative component of managers' future outlook.

Importantly, this differs from Li (2010). Li extracts explicit Forward-Looking Statements (FLS) and measures their tone. Kato & Goto argue that managers' outlook may also influence the tone of statements discussing current or past performance. They therefore use the full MD&A and extract the forward-looking component statistically rather than lexically.

### 3. Economic validation
The principal test asks whether adjusted tone predicts next-period ROA after controlling for current observable information:

`ROA_(t+1) = controls_t + γ TONE_ADJ_t + error`

A positive and significant coefficient on adjusted tone is interpreted as evidence that MD&A contains managers' future outlook that helps predict future performance.

The authors estimate pooled OLS, firm fixed-effects/LSDV specifications, and dynamic-panel GMM. Because lagged ROA creates an endogeneity issue, their substantive interpretation emphasizes one-step system GMM.

### 4. Heterogeneity and regulatory change
The authors interact adjusted tone with:
- industry indicators;
- firm-size/sales indicators; and
- an indicator for the 2018 MD&A restructuring.

They also examine whether the regulatory effect differs across industries and firm size.

## Main findings
1. **MD&A tone contains predictive information about future performance.**  
   Adjusted tone is significantly positively associated with next-period ROA after controlling for current financial, market, and firm information. This is consistent with the idea that Japanese MD&A contains managers' qualitative future outlook.

2. **The result is broadly consistent with Li (2010).**  
   Despite major differences in language, institutional setting, sentiment measurement, and the construction of forward-looking information, the Japanese evidence supports the general proposition that MD&A tone contains information about future firm performance.

3. **The informativeness of MD&A differs across industries.**  
   The predictive relation between adjusted tone and future performance varies materially across industries.

4. **Smaller firms tend to exhibit stronger future-outlook information in MD&A.**  
   The authors find that the future-performance implications of adjusted tone tend to be stronger for firms with lower sales.

5. **The 2018 MD&A restructuring has heterogeneous effects.**  
   The authors do not find a simple universal improvement. They find evidence that the predictive information in MD&A improved particularly for service-sector firms following the restructuring.

6. **This was an early large-scale Japanese annual-report tone study.**  
   The authors characterize the paper as the first large-scale analysis using textual tone in Japanese annual securities reports and argue that it fills an important gap in the Japanese disclosure literature.

## Relevance to my paper
This paper is **extremely relevant** to my Paper 2 because the institutional setting, source documents, and sentiment methodology overlap directly with my research.

### 1. It is a Japanese analogue to Li (2010)
Kato & Goto explicitly build on Li (2010) and ask whether managers communicate useful information about future performance through MD&A tone. This establishes a direct literature path:

**Li (2010) → Kato & Goto (2021) → my research**

Li establishes the FLS/tone idea in U.S. filings. Kato & Goto demonstrate a closely related phenomenon in Japanese annual securities reports. My paper can extend this literature by asking whether the ability to extract economically relevant sentiment depends on both **where the information is located** and **which NLP technology is used to measure it**.

### 2. It uses essentially my document setting
The paper studies Japanese 有価証券報告書 obtained from EDINET and focuses on MD&A-related narrative text. This makes it substantially more directly relevant to my empirical setting than most U.S. 10-K literature.

### 3. It uses a Japanese translation of Loughran-McDonald
This is particularly important because my Paper 1/Paper 2 also uses a finance-specific dictionary approach. Kato & Goto provide prior Japanese evidence that a translated LM financial dictionary can extract economically meaningful tone from Japanese securities reports.

This strengthens the motivation for treating LMMD as a serious benchmark rather than merely a technologically primitive baseline.

### 4. It provides a different definition of "forward-looking"
Li (2010) identifies explicit FLS linguistically. Kato & Goto instead infer a forward-looking component statistically by removing from full-document tone the portion explained by current observable information.

This distinction is conceptually useful for Paper 2. There are now several different ways of locating potentially informative language:

- explicit **forward-looking statements**;
- statistically inferred **future-outlook tone**;
- **textually novel** versus persistent language;
- **sentiment innovation** (year-over-year change in sentiment).

These dimensions overlap but are not equivalent.

### 5. It reinforces the idea that full-document text may mix different information components
Their adjusted-tone construction explicitly assumes that observed MD&A tone combines information attributable to current conditions with a residual component potentially reflecting managers' private information or subjective future outlook.

My Paper 2 makes a related but different decomposition: persistent versus novel disclosure. Both approaches challenge the assumption that every sentence in a long disclosure is equally informative.

### 6. It creates an interesting bridge between textual novelty and forward-looking information
A possible secondary question for my paper is whether **novel forward-looking sentiment** is especially market relevant. For example, a repeated statement about future strategy may be forward-looking but contain little new information, whereas a newly introduced negative outlook may be both forward-looking and novel.

This could be a robustness or extension rather than part of the primary design.

### 7. It provides a benchmark that my paper can substantially extend
Kato & Goto use one lexical sentiment technology and predict future accounting performance. My paper can move beyond this by:
- comparing LMMD, Japanese Financial BERT, and GPT;
- decomposing text by novelty/persistence;
- distinguishing textual innovation from sentiment innovation;
- using market reaction as an external validation criterion;
- formally testing whether the relative performance of sentiment technologies changes with informational novelty.

Thus, the paper is close enough to establish that the Japanese setting matters, but different enough that it does not subsume my proposed contribution.

## How I might cite it
**For Japanese MD&A sentiment:**  
Kato and Goto (2021) provide large-sample evidence that the tone of MD&A disclosures in Japanese annual securities reports contains information about subsequent firm performance.

**For the Japanese extension of Li:**  
Extending the forward-looking disclosure logic of Li (2010) to Japan, Kato and Goto (2021) find that a component of MD&A tone unexplained by contemporaneously observable firm information predicts subsequent operating performance.

**For dictionary-based Japanese financial sentiment:**  
Kato and Goto (2021) apply a Japanese translation of the Loughran-McDonald financial sentiment dictionary to approximately 20,000 annual securities reports and document economically meaningful associations between disclosure tone and future performance.

**For information decomposition:**  
Rather than extracting explicit forward-looking sentences as in Li (2010), Kato and Goto (2021) estimate a forward-looking component of full-MD&A tone by removing the component explained by contemporaneously observable firm information.

**Possible bridge into my research gap:**  
Prior Japanese evidence establishes that annual-report tone contains information about future firm performance (Kato and Goto, 2021). However, it remains unclear whether such information is concentrated in text that is genuinely new relative to prior disclosures and whether the relative effectiveness of lexical and contextual sentiment measures varies with that textual novelty.

## Possible literature-review section
Primary: **Japanese financial-text research / Japanese annual securities reports**

Secondary: **Forward-looking disclosure and information location**

Also relevant to: **Dictionary-based financial sentiment** and **economic validation of textual measures**

This paper should probably receive more than a passing citation because it is unusually close to my institutional setting and directly connects Li (2010), Japanese MD&A, LM-style sentiment measurement, and future performance.

## Important quotes / page numbers
- **p. 45 (abstract):** The authors state that quantitative analysis of MD&A text shows that managers' disclosed future outlook has predictive power for future firm performance. They also summarize the industry, firm-size, and 2018-regulatory-change results.
- **pp. 47–48:** Literature review positioning. Li (2010) is identified as pioneering tone analysis of U.S. MD&A, and the authors argue that large-scale tone analysis of Japanese securities reports had not previously been conducted.
- **p. 49:** Definition of the tone measure and construction of the Japanese translation of the Loughran-McDonald financial sentiment dictionary.
- **pp. 49–51:** Critical methodological discussion of adjusted tone. The authors explain why they do not simply extract explicit FLS as Li does and instead estimate a future-outlook component from full-MD&A tone.
- **pp. 53–54:** EDINET sample and Japanese text-processing pipeline, including MeCab.
- **pp. 58–59:** Main result: adjusted tone significantly predicts next-period ROA across specifications; the authors interpret this as evidence of managers' future-outlook disclosure and explicitly state that the result is consistent with Li (2010).
- **pp. 65–67:** Regulatory-change results, conclusion, and future research directions.

Particularly important conceptual passage:
Kato & Goto explain that Li (2010) extracts only explicit Forward-Looking Statements, whereas they argue that managers' expectations can affect the tone even of descriptions of past performance. They therefore use the entire MD&A and statistically remove the component attributable to current conditions. This is worth citing when discussing alternative ways to identify economically forward-looking information.

## Caveats / limitations
- **Short sample period:** The reports cover only 2014–2019. The authors themselves note that adding earlier data, including recession periods, would provide an important robustness check.
- **Dictionary translation:** The sentiment dictionary is a Japanese translation of an English dictionary developed for U.S. 10-Ks. Translation may not perfectly capture Japanese financial-language polarity or context.
- **Lexical context limitation:** The tone measure is based on counts of positive and negative words and therefore cannot fully model negation, compositional meaning, sentence context, or nuanced financial semantics.
- **Adjusted tone is an inferred construct:** The residual from a regression of tone on observable information is interpreted as managers' qualitative future outlook. Residual variation can contain omitted variables, measurement error, or other unobserved influences; it is not a direct observation of management expectations.
- **Not explicit FLS extraction:** Despite its forward-looking interpretation, the paper does not actually classify sentences as forward-looking versus non-forward-looking. This is a major conceptual difference from Li (2010).
- **Outcome is future accounting performance, not immediate market reaction:** The primary economic validation is next-period ROA. My paper uses abnormal market returns, so the meaning of "informative sentiment" differs.
- **Regulatory-change identification is limited:** The 2018 reform occurs near the end of a short sample, leaving relatively little post-change data. The authors themselves suggest that future data could reveal longer-run firm responses.
- **Industry and firm-size heterogeneity complicate generalization:** The paper finds that disclosure informativeness varies across firm characteristics, suggesting that pooled estimates can conceal meaningful heterogeneity.
- **Other disclosure channels are omitted:** The authors explicitly note that quarterly reports, conference calls, annual reports, and other disclosure channels could provide additional information.
- **Tone is only one textual feature:** The authors suggest future work could examine other linguistic characteristics, including modality, to quantify the speaker's subjectivity.

## Connection to Li (2010)
The relationship is close but the methodology is not identical.

**Li (2010):**
1. Identify explicit forward-looking sentences.
2. Classify their tone/content using supervised Naive Bayes.
3. Test whether FLS tone predicts future fundamentals.

**Kato & Goto (2021):**
1. Measure dictionary tone over Japanese MD&A.
2. Regress tone on contemporaneously observable information.
3. Treat the unexplained residual as adjusted/future-outlook tone.
4. Test whether that component predicts next-period ROA.

**My Paper 2:**
1. Compare current disclosure with the firm's prior disclosure to measure textual novelty.
2. Separate or continuously characterize novel versus persistent information.
3. Apply multiple sentiment technologies (LMMD, Financial BERT, GPT).
4. Test whether the relative market relevance of the technologies depends on novelty.
5. Separately examine sentiment innovation (year-over-year change in sentiment).

This creates a useful conceptual progression from **temporal orientation (FLS)** to **inferred future outlook** to **informational novelty and technology choice**.
