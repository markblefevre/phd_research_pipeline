# Frankel et al. (2022; working paper 2021)

## Local filename
Frankel_Jennings_Lee_2021_Disclosure_Sentiment.pdf

## Citation
@article{frankel2022disclosure,
  title={Disclosure sentiment: Machine learning vs. dictionary methods},
  author={Frankel, Richard and Jennings, Jared and Lee, Joshua},
  journal={Management Science},
  volume={68},
  number={7},
  pages={5514--5532},
  year={2022},
  publisher={INFORMS}
}

## Research question
What does the paper ask?

Do supervised machine-learning methods provide more powerful and reliable measures of financial-disclosure sentiment than dictionary-based methods such as the Loughran-McDonald and Harvard dictionaries? The paper also asks whether machine-learning sentiment measures remain informative across time and disclosure settings, and whether they capture information that investors initially overlook.

## Data
The main 10-K sample contains 75,363 firm-year observations from 1996–2019. The authors also use a restricted 1996–2008 subsample of 40,922 observations to replicate the Loughran and McDonald (2011) sample period.

The conference-call sample contains 106,151 firm-quarter conference-call transcripts from Factiva’s FD Wire over 2004–2019.

As an additional disclosure setting, the paper analyzes 177,900 earnings-announcement 8-K press releases from 2004–2019.

Market outcomes are measured using short-window cumulative abnormal returns around the relevant disclosure dates. The authors also test whether textual measures predict future earnings surprises and future earnings-announcement returns.

## Methods
The paper compares dictionary-based sentiment with supervised machine-learning sentiment.

Dictionary-based measures use the Harvard Psychosociological Dictionary and the Loughran-McDonald financial dictionary. For each disclosure, the authors calculate positive tone, negative tone, and net tone from word counts.

The machine-learning approach uses three supervised models:
- random forest regression trees (RF),
- support vector regression (SVR), and
- supervised latent Dirichlet allocation (sLDA).

The models use counts of one- and two-word phrases from each disclosure as predictors. Text is stemmed with the Porter stemmer; stop words, words containing digits, and very infrequent phrases are removed.

The key feature of the design is that the machine-learning models are trained directly on market reactions. The models map text features to the two-day cumulative abnormal return, CAR[0,1], surrounding the 10-K filing or conference call. Rolling historical training windows are used, and predictions are generated out of sample for the subsequent period. This allows the learned language-return relation to change over time.

The authors also create a combined machine-learning factor from RF, SVR, and sLDA predictions. They compare explanatory power using regression adjusted R² and statistical tests of model fit.

Finally, they retrain the machine-learning models to predict future earnings surprises and future earnings-announcement CARs, testing whether disclosure language contains information not immediately incorporated into prices.

## Main findings
Machine-learning sentiment measures outperform dictionary-based measures in explaining market reactions to financial disclosures.

For 10-K filings, the Loughran-McDonald negative and net-tone measures replicate the original Loughran and McDonald (2011) findings during 1996–2008, but lose their association with filing-date returns when the sample is extended through 2019. The Harvard measures perform even less consistently. By contrast, the RF, sLDA, and combined machine-learning measures remain significantly related to 10-K filing returns in the full sample.

Random forest performs best among the machine-learning methods. In the 10-K setting, RF produces the highest adjusted R² among the dictionary and machine-learning measures, although the absolute explanatory power remains small because market reactions to 10-K filings are weak.

The differences are much larger for conference calls. Dictionary-based measures explain roughly 5–6% of the variation in conference-call returns, while the RF measure produces an adjusted R² of about 12.7%. The RF model’s explanatory power is approximately 99% greater than the best Loughran-McDonald specification.

Combining multiple machine-learning measures does not outperform random forest alone. The authors therefore recommend RF as a relatively simple supervised machine-learning approach for measuring disclosure sentiment.

The RF approach also predicts future earnings surprises and future earnings-announcement returns, suggesting that it captures information in disclosures that is not fully incorporated into prices immediately. Dictionary measures perform much less consistently in these future-outcome tests.

Overall, the authors conclude that supervised machine-learning methods provide more powerful and temporally robust measures of disclosure sentiment than static dictionaries.

## Relevance to my paper
This is one of the most directly relevant methodological papers for my study because it explicitly compares alternative sentiment-measurement approaches using market returns as the validation criterion.

The central idea is especially important: Frankel et al. do not define a “better” sentiment measure by agreement with human labels. Instead, following Loughran and McDonald, they ask which textual measure best captures contemporaneous market reactions. This closely aligns with the idea of evaluating competing sentiment methods according to their economic relevance.

The paper also provides evidence that a method that performs well in one period may not remain reliable over time. The Loughran-McDonald dictionary explains 10-K filing returns in the original historical period but loses explanatory power in later years, whereas machine-learning models trained with rolling windows adapt to changing language. This is useful when motivating comparisons across methods rather than assuming that a well-established financial dictionary remains the best benchmark in all settings.

Frankel et al. also provide a bridge between dictionary-based methods and modern contextual language models. Their machine-learning methods are more flexible than fixed dictionaries but still rely mainly on one- and two-word phrase features rather than contextual transformer representations. This makes the paper a useful intermediate step in the methodological progression from dictionaries to classical supervised machine learning to transformers and LLMs.

There is also an important distinction between their design and mine. Frankel et al. train their machine-learning sentiment measure directly on contemporaneous abnormal returns. The resulting RF score is therefore explicitly optimized to explain market reactions. If my models generate sentiment independently of realized returns and are then evaluated against CARs, the validation exercise is conceptually stricter because the sentiment measure is not itself trained on the outcome used to assess economic relevance.

## How I might cite it
- Machine-learning approaches can produce more powerful and reliable measures of financial-disclosure sentiment than static dictionaries.
- Loughran-McDonald dictionary sentiment explains 10-K filing returns in the original sample period but does not remain consistently associated with returns when the sample is extended through 2019.
- Rolling supervised models can adapt to changes in disclosure language over time, whereas static dictionaries cannot.
- Random forest outperforms support vector regression and supervised LDA in capturing disclosure sentiment in this study.
- Machine-learning sentiment measures explain contemporaneous market reactions to both 10-K filings and conference calls better than dictionary-based measures.
- The relative advantage of machine learning is particularly large for conference calls, where the RF model nearly doubles the explanatory power of the best Loughran-McDonald specification.
- The study provides precedent for evaluating sentiment measures by their relationship with realized market returns rather than solely by linguistic classification accuracy.
- Machine-learning text measures can also capture information that appears to be incorporated into prices only with a delay.
- Useful contrast for my paper: Frankel et al. train their machine-learning measures directly on abnormal returns, whereas my sentiment measures can be constructed independently and subsequently evaluated against market reactions.
- Useful methodological bridge: Frankel et al. sit between fixed dictionary methods and contextual transformer/LLM methods, using supervised phrase-based machine learning rather than deep contextual language models.

## Possible literature-review section
Primary: Sentiment measurement and economic/market validation

Secondary: Dictionary versus machine-learning sentiment; supervised financial text analysis; evolution from dictionaries to contextual language models

## Important quotes / page numbers
- p. 1 — Core result: the abstract states that machine-learning measures provide significantly greater explanatory power than dictionary-based sentiment measures and that random forest performs best among the tested algorithms.
- pp. 1–3 — Motivation: dictionaries are static, can fail to reflect changes across time and industries, assign fixed importance to words, and require researcher judgment.
- p. 3 — Key temporal result: the Loughran-McDonald sentiment measures explain 10-K filing returns in the original 1996–2008 period but no longer do so when the sample is extended through 2019.
- pp. 13–17 — Construction of the supervised machine-learning sentiment measures: RF, SVR, and sLDA are trained on one- and two-word phrase counts to predict CAR[0,1] using rolling historical windows.
- pp. 21–22 — 10-K result: machine-learning measures, especially RF, more consistently explain 10-K filing returns than the dictionary measures, although explanatory power is economically small in this low-information setting.
- pp. 24–25 — Conference-call result: RF produces an adjusted R² of about 12.68%, approximately 99% greater than the Loughran-McDonald net-tone model.
- pp. 29–31 — Conclusion: machine-learning methods are more robust across disclosure settings and time, with RF providing the strongest results; the authors recommend adapting methods to disclosure setting and period.
- p. 31 — Important methodological caution: machine-learning models can capture spurious relationships, but the authors argue that genuine out-of-sample testing mitigates overfitting.
- p. 7 / conclusion discussion — Generalizability caveat: the authors explicitly caution that their results may not generalize to textual constructs other than disclosure sentiment.

## Caveats / limitations
The most important limitation for comparison with independently constructed sentiment measures is that the supervised machine-learning models are trained directly on abnormal returns. The RF, SVR, and sLDA “sentiment” scores are therefore optimized to predict the same type of market reaction subsequently used to judge their effectiveness. This makes the approach economically targeted but also means it is not a purely exogenous measure of linguistic sentiment.

The authors reduce overfitting concerns by using rolling training windows and out-of-sample predictions, but supervised models can still learn phrases that correlate with returns without having an intuitive interpretation as sentiment. The paper acknowledges this tradeoff between predictive power and economic interpretability.

The strong relative results for machine learning are not uniform across disclosure settings. The improvement over dictionaries is very large for conference calls but relatively small for 10-Ks and earnings-announcement press releases. The usefulness of a method therefore appears to depend on the disclosure type and information environment.

The 10-K market-reaction tests have inherently low explanatory power because 10-K filings often contain information already released elsewhere. The authors explicitly note that even better sentiment measurement is unlikely to create economically large increases in R² in this setting.

The paper is based on English-language U.S. disclosures. Generalization to Japanese text, different tokenization regimes, other disclosure systems, or languages with substantially different morphology is not established.

The authors compare random forests, SVR, and sLDA, but not transformer-based contextual models. Consequently, the paper shows that classical supervised machine learning improves upon dictionaries, but it does not answer whether transformers or LLMs provide further incremental economic information.

Finally, the authors explicitly caution that their conclusion favoring machine learning is specific to disclosure sentiment and may not generalize to other textual constructs. For narrowly defined narrative features, dictionary methods may remain more appropriate.

Important methodological caveat: Frankel et al.’s machine-learning measures are supervised directly on abnormal returns. Their RF, SVR, and sLDA scores therefore learn textual features specifically selected for their ability to predict market reactions. Although rolling out-of-sample estimation mitigates conventional overfitting, the comparison with dictionary sentiment is asymmetric: the machine-learning methods are optimized on the economic validation target, whereas the dictionaries are not. The resulting measures are therefore better interpreted as text-based return-prediction signals than as independent measures of linguistic sentiment.