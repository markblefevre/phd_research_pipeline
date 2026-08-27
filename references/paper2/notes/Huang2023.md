# Huang, Wang, and Yang (2023)

## Local filename
Contemporary Accting Res - 2022 - Huang - FinBERT  A Large Language Model for Extracting Information from Financial Text.pdf

## Citation
@article{huang2023finbert,
  title={FinBERT: A large language model for extracting information from financial text},
  author={Huang, Allen H and Wang, Hui and Yang, Yi},
  journal={Contemporary Accounting Research},
  volume={40},
  number={2},
  pages={806--841},
  year={2023},
  publisher={Wiley Online Library}
}

## Research question
What does the paper ask?

Can a BERT-based language model adapted specifically to financial text extract financial sentiment more accurately than the Loughran-McDonald dictionary, conventional machine-learning methods, earlier deep-learning models, and generic BERT?

The paper also asks why a finance-domain model performs better, whether its advantage is especially strong when labeled training data are scarce, whether the model generalizes to other financial-text tasks such as ESG classification, and whether its sentiment scores better capture economically relevant information in earnings conference calls.

A broader methodological question is whether the added complexity of contextual transformer models produces enough improvement in finance and accounting applications to justify moving beyond simpler bag-of-words and machine-learning approaches.

## Data
The study uses several distinct datasets.

### Financial-domain pretraining corpus
FinBERT is pretrained from scratch on approximately **4.9 billion financial-text tokens**, drawn from three major sources:

- 60,490 10-K filings and 142,622 10-Q filings for Russell 3000 firms from 1994–2019, using the Business, Risk Factors, and MD&A-related sections;
- 476,633 analyst reports for S&P 500 firms from 2003–2012 from Thomson Investext; and
- 136,578 earnings conference-call transcripts for 7,740 public firms from 2004–2019 from Seeking Alpha.

The financial corpus is approximately 50% larger than the 3.3-billion-token general-language corpus originally used to pretrain Google's BERT model.

### Researcher-labeled sentiment sample
The primary sentiment-classification task uses **10,000 sentences from financial analyst reports** previously labeled by researchers.

The labels are:
- 3,577 positive,
- 4,586 neutral, and
- 1,837 negative.

The sample is split into:
- 8,100 training sentences,
- 900 validation sentences, and
- 1,000 testing sentences.

### ESG classification sample
For a separate downstream task, the authors manually label **2,000 sentences** from corporate social-responsibility reports and MD&A disclosures as:
- environmental,
- social,
- governance, or
- non-ESG.

### Earnings conference-call market-validation sample
The market-based analysis begins with 31,592 earnings conference-call transcripts for S&P 500 firms from 2003–2020.

After requiring Compustat, CRSP, I/B/E/S, and institutional-ownership data, the final sample contains **28,873 conference calls from 712 firms**.

## Methods
The authors develop **FinBERT**, a finance-domain version of Google's BERTBASE architecture.

### Financial-domain pretraining
FinBERT is pretrained from scratch on the 4.9-billion-token financial corpus using the standard BERT objectives:
- masked-language modeling, and
- next-sentence prediction.

The model therefore learns bidirectional contextual representations of financial language rather than treating words as independent tokens.

The authors also construct a finance-specific tokenizer vocabulary, allowing common financial words and subwords to be represented more naturally than under a vocabulary learned from general-domain text.

### Sentiment fine-tuning
FinBERT is fine-tuned on the researcher-labeled analyst-report sentences for three-way sentiment classification:
- positive,
- neutral,
- negative.

Its performance is compared with:
- Loughran-McDonald dictionary,
- Naive Bayes,
- support vector machine,
- random forest,
- convolutional neural network,
- long short-term memory network,
- and Google's generic BERT model.

All supervised models use the same training, validation, and test samples, allowing a relatively clean comparison of classification performance.

Performance metrics include:
- accuracy,
- precision,
- recall,
- F1 score,
- and class-specific recall.

### Small-training-sample analysis
The authors repeatedly reduce the training sample from 100% to 10% of its original size to test whether pretraining and domain adaptation allow FinBERT to retain performance when researcher-labeled data are scarce.

### Word-order randomization
To test whether FinBERT's advantage comes from contextual information rather than simply superior vocabulary, the authors randomize word order in the testing sentences and compare performance deterioration across models.

### Finance-vocabulary analysis
The authors examine whether FinBERT's advantage over generic BERT is greater when sentences contain a larger fraction of finance-specific vocabulary.

They also use an interpretability procedure to identify words that are particularly important to FinBERT's classifications.

### ESG classification
FinBERT and the comparison models are fine-tuned or trained on the separately labeled ESG dataset.

### Economic / market validation
The authors apply each sentiment model to earnings conference calls.

Each sentence is classified as positive, neutral, or negative. Conference-call tone is defined as:

`percentage positive sentences - percentage negative sentences`

The main tone measure uses managers' remarks from both the presentation and Q&A portions of the call.

The tone measures are standardized before regression.

Economic validation uses a regression of three-day cumulative abnormal returns around the conference call on the sentiment measure and controls.

The dependent variable is **CAR over a three-day window centered on the conference-call date**.

Controls include:
- current earnings,
- unexpected earnings,
- accruals,
- earnings volatility,
- size,
- market-to-book,
- leverage,
- institutional ownership,
- analyst coverage,
- firm age,
- turnover,
- prior returns,
- dividend declaration,
- and NASDAQ listing.

Regressions include fiscal year-quarter fixed effects and standard errors clustered by firm.

The authors compare explanatory power using Vuong tests and bootstrapping.

## Main findings
FinBERT substantially outperforms the competing sentiment methods on the researcher-labeled financial-text task.

On the 1,000-sentence test set:

- FinBERT: **88.2% accuracy, 87.8% F1**
- generic BERT: 85.0% accuracy, 84.2% F1
- LSTM: 76.3% accuracy, 73.3% F1
- CNN: 75.1% accuracy, 72.5% F1
- Naive Bayes: 73.6% accuracy, 71.1% F1
- SVM: 72.6% accuracy, 69.6% F1
- Random Forest: 71.9% accuracy, 66.8% F1
- Loughran-McDonald dictionary: **62.1% accuracy, 58.1% F1**

FinBERT's advantage is especially large for negative sentences. Its recall for negative sentiment is **89.7%**, while all non-BERT approaches are below 60%.

The paper provides direct evidence that **context matters**. Randomizing word order reduces FinBERT accuracy by **11.3 percentage points**, versus little or no deterioration for most bag-of-words and earlier machine-learning methods. This supports the interpretation that FinBERT gains from sequential and contextual information rather than merely from a larger feature set.

Domain adaptation also matters. FinBERT outperforms generic BERT, particularly when:
- financial vocabulary is prominent, and
- the labeled training sample is small.

With only 10% of the original training data, FinBERT retains **81.3% accuracy**, still exceeding the full-sample performance of all non-BERT models. Generic BERT falls much more sharply to 62.0%.

The ESG results are similar. FinBERT achieves **89.5% accuracy**, above generic BERT and all non-BERT approaches.

The market-validation tests also favor FinBERT.

In the 28,873-call sample, all sentiment measures are positively related to three-day CAR, but FinBERT's standardized tone coefficient is largest:

- FinBERT: approximately **0.907**
- BERT: 0.873
- LM: 0.640
- NB: 0.647
- SVM: 0.687
- RF: 0.466
- CNN: 0.692
- LSTM: 0.743

The other methods therefore understate the economic association between textual sentiment and market reaction relative to FinBERT by at least **18.1%** and as much as **48.6%**.

Vuong and bootstrap tests indicate that the FinBERT-based market-reaction model has greater explanatory power than the competing approaches.

In additional horse-race specifications containing both FinBERT tone and another model's tone, FinBERT remains positively significant while the alternative measures generally lose their positive association.

Overall, the paper concludes that domain-adapted contextual transformer models can materially improve both linguistic sentiment classification and the measurement of economically relevant information in financial text.

## Relevance to my paper
This is a very important paper for my study because it provides a **top-journal, published bridge between dictionary methods and modern contextual language models**.

Unlike Araci (2019), which primarily establishes transformer performance on sentiment benchmarks, Huang et al. explicitly connect improved linguistic classification to **capital-market outcomes** using conference-call abnormal returns.

The paper therefore provides strong support for my central question: whether more sophisticated contextual models produce economically more informative sentiment measures than traditional dictionary methods.

Its design is especially useful because the primary FinBERT sentiment model is trained on **researcher-labeled sentiment**, not on stock returns. Returns are subsequently used as an external economic-validation criterion.

That distinction is crucial when comparing Huang et al. with Frankel et al. (2022) and Siano (2025). Frankel and Siano train textual models directly on abnormal returns, whereas Huang et al. first train a linguistic sentiment classifier and then ask whether its independently measured sentiment better explains market reactions.

Huang et al. therefore provide a much cleaner precedent for my intended comparison of dictionary, transformer, and LLM sentiment measures against a common market-based outcome.

The paper also reinforces the importance of **domain adaptation**. FinBERT beats generic BERT, especially for text containing finance-specific vocabulary. This is directly relevant to Japanese financial text, where both domain adaptation and language-specific modeling may matter.

The word-order randomization result is particularly valuable for motivating contextual methods. The substantial decline in FinBERT performance when word order is destroyed provides unusually direct evidence that contextual relationships—not merely word frequencies—contribute to classification accuracy.

At the same time, the paper's market validation is based on conference calls rather than regulatory annual filings. Conference calls are timely, interactive, and closely tied to earnings announcements, so the relative benefit of contextual sentiment may differ in slower-moving annual securities reports.

## How I might cite it
- Huang, Wang, and Yang (2023) provide top-journal evidence that a finance-domain BERT model substantially outperforms dictionary, conventional machine-learning, and earlier deep-learning methods in financial sentiment classification.
- FinBERT achieves 88.2% sentiment-classification accuracy versus 62.1% for the Loughran-McDonald dictionary.
- Domain-specific pretraining improves transformer performance relative to generic BERT, particularly for finance-specific vocabulary.
- Contextual information materially contributes to FinBERT's performance: randomizing word order reduces classification accuracy by 11.3 percentage points.
- FinBERT retains strong performance even with small labeled training samples, highlighting the value of financial-domain pretraining.
- FinBERT is especially effective at identifying negative financial sentiment.
- The paper shows that improved sentiment classification can translate into stronger economic measurement: FinBERT tone explains conference-call market reactions better than competing methods.
- Alternative NLP methods understate the economic association between conference-call sentiment and abnormal returns by at least 18% relative to FinBERT.
- The paper provides an important example of **external economic validation** because FinBERT sentiment is trained on researcher labels rather than on realized returns.
- Useful contrast with Frankel and Siano: Huang et al. construct an independent sentiment classifier and then validate it against market outcomes, whereas those papers train models directly on return-related targets.
- Useful contrast for my paper: Huang et al. analyze English analyst reports and earnings conference calls, whereas my study examines Japanese regulatory disclosures.

## Possible literature-review section
Primary: Machine learning and transformer-based financial sentiment

Secondary: Economic validation: sentiment and market outcomes; domain adaptation in financial NLP; dictionary versus contextual sentiment methods

## Important quotes / page numbers
- p. 806 (PDF p. 1) — Abstract: FinBERT substantially outperforms the LM dictionary, NB, SVM, RF, CNN, and LSTM in sentiment classification and better captures conference-call textual informativeness.
- pp. 807–808 (PDF pp. 2–3) — Motivation: prior financial-text methods largely rely on bag-of-words assumptions and ignore word order and context; the paper asks whether finance-adapted contextual language models materially improve tasks relevant to finance and accounting.
- pp. 808–809 (PDF pp. 3–4) — Headline sentiment result: FinBERT reaches 88.2% accuracy compared with 85.0% for generic BERT, 76.3% for LSTM, and 62.1% for the LM dictionary.
- p. 809 (PDF p. 4) — Small-sample result: with only 10% of the original training sample, FinBERT still achieves 81.3% accuracy.
- p. 809 (PDF p. 4) — Mechanism: the authors argue that FinBERT's advantage reflects both contextual information and familiarity with financial vocabulary.
- pp. 812–813 (PDF pp. 7–8) — Pretraining corpus: 4.9 billion tokens from 10-K/10-Q filings, analyst reports, and conference calls.
- pp. 813–814 (PDF pp. 8–9) — Researcher-labeled design: 10,000 analyst-report sentences are split into common training, validation, and test samples for a direct algorithm comparison.
- pp. 814–815 (PDF pp. 9–10) — Table 1: FinBERT has the strongest accuracy/F1 and particularly strong negative-sentiment recall.
- pp. 818–819 (approximately PDF pp. 13–14) — Word-order experiment: randomizing words reduces FinBERT accuracy by 11.3 points but has relatively little effect on most non-BERT models.
- pp. 823–824 (approximately PDF pp. 18–19) — ESG application: FinBERT reaches 89.5% classification accuracy and again outperforms comparison methods.
- pp. 824–829 (approximately PDF pp. 19–24) — Market-validation design and results: 28,873 conference calls; three-day CAR; FinBERT tone has the largest economic association and greater explanatory power than competing measures.
- Conclusion, approximately p. 829 (PDF p. 24) — The authors conclude that domain-adapted contextual language models provide more accurate financial-text measurement and can reduce textual measurement error in economic research.

## Caveats / limitations
The paper refers to FinBERT as an “LLM,” consistent with terminology used at the time, but the model is a **BERTBASE-style encoder transformer**, not a modern generative LLM such as GPT-4 or LLaMA. In my literature review it is clearer to describe it primarily as a domain-adapted transformer / BERT model.

The primary classification task uses only 10,000 researcher-labeled analyst-report sentences. Although this is substantially larger than some early financial sentiment datasets, it remains relatively small and highly specific to professional analyst language.

The sentiment labels originate from prior researcher coding. FinBERT may therefore partly learn the labeling conventions of that particular annotation protocol rather than some universally correct notion of financial sentiment.

The analyst-report sentences used for supervised testing are also contained in FinBERT's unlabeled pretraining corpus. The authors argue that this should not create label leakage because sentiment labels are absent during pretraining, and they report similar FinBERT-versus-BERT performance on Financial PhraseBank, which does not overlap with the pretraining data. Nevertheless, corpus overlap should be noted.

The main economic-validation test is explicitly described by the authors as a **joint and noisy test**. A stronger relation between sentiment and CAR can reflect better sentiment measurement, but it can also depend on whether investors care about the textual information, whether information was previously known, and how much quantitative news is released simultaneously.

The market test therefore does not establish that FinBERT sentiment is the objectively “true” sentiment measure. It establishes that FinBERT-derived sentiment has a stronger association with contemporaneous market reaction in this setting.

Conference calls differ substantially from annual regulatory filings. They are timelier, interactive, earnings-focused, and contain Q&A dialogue. The size of FinBERT's advantage may therefore not transfer directly to annual securities reports.

The conference-call sample is restricted to S&P 500 firms with complete data. These are large, heavily followed companies and may not represent smaller firms or markets with weaker information environments.

The conference-call tone measure aggregates sentence-level positive and negative classifications. Although FinBERT captures context within sentences, the method does not directly model long-range context across an entire call.

BERT's 512-token input limit remains relevant. The paper avoids feeding full transcripts to the model by classifying sentences individually and then aggregating them, which sacrifices cross-sentence and document-level context.

The paper's results show that domain adaptation improves over generic BERT in English financial text, but they do not establish whether the same architecture transfers to Japanese financial language. Japanese tokenization, morphology, vocabulary, and disclosure conventions create a separate domain-adaptation problem.

Finally, the comparison predates instruction-tuned generative LLMs such as GPT-4, Claude, and modern LLaMA models. The paper establishes the superiority of contextual BERT-style representations over dictionaries and earlier ML/deep-learning methods, not the relative performance of transformers versus today's generative LLMs.
