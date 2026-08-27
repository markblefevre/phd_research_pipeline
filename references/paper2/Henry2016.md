# Henry and Leone (2016)

## Local filename
Henry and Leone_TAR2016(20260827-050105).pdf

## Citation
@article{henry2016measuring,
  title={Measuring qualitative information in capital markets research: Comparison of alternative methodologies to measure disclosure tone},
  author={Henry, Elaine and Leone, Andrew J},
  journal={The Accounting Review},
  volume={91},
  number={1},
  pages={153--178},
  year={2016},
  publisher={American Accounting Association}
}

## Research question
What does the paper ask?

Which methods for measuring disclosure tone are most effective in capital-markets research?

More specifically, Henry and Leone compare:
- finance-specific versus general-purpose sentiment wordlists,
- equal word weighting versus inverse-document-frequency weighting, and
- simple word-frequency tone measures versus a Naive Bayes machine-learning tone measure.

Their central question is not merely which method classifies text differently, but which measure is more useful for explaining economically meaningful outcomes such as market reactions to earnings announcements, post-announcement drift, and future earnings.

## Data
The paper uses two main empirical samples.

### Earnings-announcement sample
The primary event-study sample consists of earnings press releases filed with the SEC on Form 8-K between 2004 and 2012.

The authors begin with 143,972 8-K filings identified as earnings releases under Item 2.02. After matching to Compustat and CRSP and requiring the variables used in the regressions, 75,599 firm-quarter observations remain.

Because the paper's primary tone specification is the change in tone from the prior quarter, the main `DTone` analysis contains 63,357 firm-quarter observations.

The earnings-announcement analysis uses:
- SEC EDGAR filings for press-release text,
- Compustat for accounting variables,
- CRSP for returns, and
- I/B/E/S analyst forecasts for unexpected earnings.

### MD&A / machine-learning comparison sample
For the comparison with machine learning, Henry and Leone reproduce and extend the Li (2010) setting.

They begin with Li's 10-K/10-Q sample over 1995–2007 and extract forward-looking statements from MD&A disclosures. After matching to CRSP and Compustat and requiring sufficient data, the final comparison sample contains 104,863 observations.

Li's original machine-learning measure is based on a much larger corpus of forward-looking MD&A sentences classified using a Naive Bayes algorithm.

## Methods
The paper compares four word-frequency tone measures:

- **FD** — Henry's finance/disclosure-specific wordlist,
- **LM** — Loughran-McDonald finance-specific wordlist,
- **GI** — General Inquirer wordlist developed in social psychology,
- **DICTION** — general-purpose wordlists originating in political and mass-communication research.

The authors hypothesize that FD and LM should outperform GI and DICTION because words often have domain-specific meanings in financial disclosure.

For each dictionary, tone is calculated as:

`(positive words - negative words) / (positive words + negative words)`

The main empirical specification uses **change in tone**, `DTone`, defined as current-quarter tone minus prior-quarter tone. Tone variables are standardized before regression analysis.

### Short-window event study
The primary market-reaction regression explains cumulative abnormal returns from day -1 through day +1 around the earnings announcement.

Abnormal return is defined as the firm's raw return minus the value-weighted market return.

The regression controls for:
- unexpected earnings,
- firm size,
- whether the firm reports a loss, and
- the alternative `DTone` measure.

The authors compare model explanatory power using Vuong tests.

### Equal weighting versus inverse-document-frequency weighting
Each wordlist is implemented under:
- equal word-frequency weighting, and
- inverse-document-frequency (`idf`) weighting of the type advocated by Loughran and McDonald (2011).

Henry and Leone question whether idf is conceptually appropriate for tone measurement because idf was developed for information retrieval and makes a document's tone score depend on the composition of the broader sample.

### Small-sample power analysis
The authors repeatedly draw random samples ranging from 50 to 2,000 observations to compare the statistical power of the four wordlists.

This test is important because weaker tone measures may create Type II errors in empirical settings with limited sample size.

### Post-earnings-announcement drift
The authors form portfolios based jointly on:
- good versus bad earnings news, and
- positive versus negative tone.

They then examine abnormal returns over the 58 trading days beginning two days after the earnings announcement.

### Word-frequency versus machine learning
The paper compares an equal-weighted FD word-frequency measure with Li's (2010) Naive Bayes measure of forward-looking MD&A tone.

Li's machine-learning measure is constructed by:
1. manually classifying a training sample of forward-looking sentences,
2. training a Naive Bayes classifier,
3. classifying the larger MD&A sentence corpus, and
4. averaging sentence-level tone classifications within each filing.

Henry and Leone compare the two measures in regressions of future earnings on current disclosure tone and standard firm controls.

## Main findings
The main result is that **domain-specific wordlists outperform general-purpose dictionaries** in capital-markets applications.

All four tone measures are positively related to short-window market reactions, but the FD and LM models explain returns significantly better than GI and DICTION. The FD measure performs best in the primary specification.

A one-standard-deviation change in `DToneFD` and `DToneLM` corresponds to approximately 0.11 and 0.09 standard deviations of abnormal market reaction, respectively, versus roughly 0.05 and 0.07 for DICTION and GI.

The difference becomes especially important in smaller samples. Domain-specific FD and LM measures retain substantially greater statistical power, while general dictionaries produce much higher probabilities of Type II error.

The paper also finds that **change in tone is more informative than the level of tone**. Using current-quarter tone minus prior-quarter tone improves explanatory power, which the authors interpret as reducing noise in the textual measure.

Contrary to Loughran and McDonald's recommendation, **idf weighting provides little or no improvement over equal weighting** in the earnings-announcement setting. Henry and Leone argue that idf also has conceptual drawbacks because it makes a document's score sample-dependent and therefore reduces transparency and replicability.

The post-announcement drift tests provide additional support for the domain-specific measures. For FD and LM, positive tone strengthens the subsequent drift following good earnings news, while negative tone strengthens the negative drift following bad earnings news. The general dictionaries do not produce equally coherent patterns.

In the machine-learning comparison, both Li's Naive Bayes measure and the simple FD word-frequency measure significantly predict future earnings. Their future-earnings regressions have nearly identical explanatory power: approximately 71.0% versus 71.2%, and the Vuong test cannot reject equivalence.

The authors therefore conclude that in commonly studied disclosure settings, **simple, equal-weighted, domain-specific word-frequency measures can be as powerful as a more complex machine-learning tone measure while being much easier to understand, implement, and replicate**.

## Relevance to my paper
This is one of the most important methodological papers for my study because its central research question closely parallels mine: **does the choice of sentiment measurement method materially change economic inference?**

Henry and Leone explicitly evaluate textual measures according to their ability to explain market reactions rather than merely by linguistic classification performance. This makes the paper a direct precedent for comparing sentiment methodologies using abnormal returns as an external economic-validation criterion.

The finding that domain-specific dictionaries outperform general-purpose wordlists reinforces the central lesson of Loughran and McDonald (2011): financial language is a specialized domain, and generic sentiment resources can create substantial measurement error.

For my Japanese setting, the implication is even stronger. If domain specificity matters within English financial text, both domain and language adaptation are likely important when applying sentiment analysis to Japanese financial disclosures.

The paper is also important because it cautions against assuming that methodological complexity automatically yields better economic measurement. In the Li (2010) comparison, a simple finance-specific word-frequency measure performs essentially as well as the Naive Bayes machine-learning measure in predicting future earnings.

That result provides an important historical counterpoint to later papers such as Siano (2025), Chiu and Hung (2025), and Okada et al. (2025), which report advantages for contextual transformer or LLM approaches. Together, these papers create a natural empirical question for my study: **do modern contextual models genuinely provide incremental economic information over a well-designed domain-specific dictionary in Japanese regulatory disclosures?**

The finding that **change in tone outperforms tone level** is also highly relevant. It provides a direct link between sentiment measurement and the textual-novelty/change literature: what matters economically may be the change in the disclosure's tone relative to the firm's previous communication rather than the unconditional tone level.

The study's market-validation design is also conceptually cleaner than papers that train textual models directly on returns. Henry and Leone's dictionary measures are constructed independently of CARs; market reaction is used only to compare their economic informativeness.

## How I might cite it
- Domain-specific financial tone dictionaries outperform general-purpose sentiment lexicons in explaining market reactions to disclosure.
- Henry and Leone (2016) show that measurement choice materially affects statistical power in disclosure-tone event studies.
- Finance-specific dictionaries reduce Type II error relative to generic wordlists, particularly in smaller samples.
- Changes in disclosure tone can be more informative than unconditional tone levels.
- Equal weighting performs at least as well as inverse-document-frequency weighting for measuring disclosure tone in their setting.
- Idf weighting can reduce interpretability and replicability because a document's score depends on the surrounding sample.
- Simple domain-specific word-frequency measures can perform as well as a Naive Bayes machine-learning tone measure in predicting future earnings.
- Greater methodological complexity does not necessarily produce stronger economically relevant textual measures.
- The study provides direct precedent for comparing alternative sentiment measures using short-window abnormal returns.
- Useful framing for my paper: if method choice changes the measured relationship between tone and market reaction in English disclosures, the concern may be especially important in Japanese financial text.

## Possible literature-review section
Primary: Dictionary-based financial sentiment

Secondary: Economic validation: sentiment and market outcomes; sentiment-measurement methodology; machine learning versus dictionaries

## Important quotes / page numbers
- p. 153 (PDF p. 1) — Abstract: domain-specific word-frequency measures better predict market reactions, have greater event-study power, and produce more economically coherent post-announcement drift than general wordlists.
- p. 153 (PDF p. 1) — Core conclusion: equal-weighted domain-specific word-frequency tone measures are generally as powerful as more complex techniques in financial disclosure settings and are easier to replicate.
- pp. 154–155 (PDF pp. 2–3) — Four-way dictionary comparison: FD and LM outperform GI and DICTION in explaining earnings-announcement market reactions.
- p. 155 (PDF p. 3) — Small-sample result: domain-specific measures substantially reduce Type II error relative to general dictionaries.
- pp. 154–157 (PDF pp. 2–5) — Critique of idf: the authors argue that sample-dependent inverse-document-frequency weighting is not naturally suited to tone measurement and impedes replication.
- pp. 159–160 (PDF pp. 7–8) — Main event-study design: `DTone` is current-quarter tone minus prior-quarter tone, and CAR is measured over days [-1,+1].
- pp. 163–164 (PDF pp. 11–12) — Main event-study result: FD and LM produce greater explanatory power than GI and DICTION; FD performs best in the primary specification.
- p. 164 (PDF p. 12) — Tone-change result: change in tone produces greater explanatory power than tone level.
- Later drift analysis / Figure 2 — For FD and LM, post-announcement drift moves in the economically expected direction when earnings news and tone reinforce one another.
- pp. 171–173 (approximately PDF pp. 19–21) — Machine-learning comparison: Li's Naive Bayes measure and the FD word-frequency measure both significantly predict future earnings.
- p. 173 (approximately PDF p. 21) — Key comparison: the future-earnings models have almost identical R² values, approximately 71.0% and 71.2%, and Vuong tests cannot reject equal explanatory power.
- Conclusion — The authors recommend equal-weighted, domain-specific word-frequency measures for many capital-markets tone applications because they are powerful, transparent, and replicable.

## Caveats / limitations
The machine-learning comparison is much narrower than the paper's title can imply. The authors compare word-frequency measures with **one specific Naive Bayes implementation from Li (2010)**, not with modern supervised learning, transformers, BERT, or LLMs. The conclusion that simple dictionaries are “as powerful as machine learning” should therefore not be generalized to contemporary contextual models.

The two principal method comparisons use different disclosure settings and different outcomes. Dictionary comparisons are conducted on earnings press releases using short-window CARs, whereas the machine-learning comparison uses forward-looking MD&A statements and future earnings. The paper therefore does not perform a single controlled horse race between dictionaries and machine learning using the same documents and market outcome.

The finance-specific FD and LM dictionaries were developed in different financial-text settings. FD was designed around earnings announcements, while LM was developed from 10-K filings. FD's superior performance in earnings press releases may partly reflect closer matching between the dictionary's development corpus and the test setting rather than a universally superior lexicon.

The paper explicitly assumes semi-strong market efficiency because “true” disclosure tone is unobservable. Market reaction is therefore treated as a criterion for comparing tone measures, but this does not establish that the measure with the highest return association is necessarily the linguistically most accurate measure of sentiment.

The event-study sample is restricted to earnings releases correctly coded as Item 2.02 in SEC 8-K filings. The authors acknowledge that firms sometimes miscode 8-K items, so the sample is not a complete census of earnings press releases.

The primary tone variable is change from the previous quarter. This improves explanatory power but requires a prior comparable disclosure and may exclude younger firms, firms with missing releases, or observations around changes in disclosure practice.

The paper's criticism of idf is specific to tone measurement. IdF may remain useful for information retrieval, feature construction, novelty measurement, or other textual tasks where the objective differs from estimating document-level sentiment.

The post-announcement drift analysis provides economically intuitive support for the domain-specific dictionaries, but it does not uniquely establish that the tone measure captures unbiased managerial information. Drift can reflect a variety of information-processing and risk mechanisms.

The sample periods—2004–2012 for earnings releases and 1995–2007 for the MD&A machine-learning comparison—predate modern transformer-based NLP and today's much faster algorithmic processing of disclosures. Relative method performance could differ materially in more recent markets.

Finally, the paper is entirely based on English-language U.S. disclosures. It provides strong evidence that domain specificity matters, but does not establish how those results transfer to Japanese language, morphology, tokenization, or the EDINET disclosure environment.
