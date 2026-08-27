# Araci (2019)

## Local filename
Araci_2019_FinBERT.pdf

## Citation
@article{araci2019finbert,
  title={Finbert: Financial sentiment analysis with pre-trained language models},
  author={Araci, Dogu},
  journal={arXiv preprint arXiv:1908.10063},
  year={2019}
}

## Research question
What does the paper ask?

Can a pre-trained transformer language model adapted to finance improve financial sentiment analysis when labeled financial data are scarce? More specifically, how does FinBERT compare with other transfer-learning approaches and prior state-of-the-art methods, does further financial-domain pre-training improve performance, and how much of BERT must be fine-tuned to obtain strong results?

## Data
The paper uses two labeled financial sentiment datasets and one unlabeled financial corpus for further pre-training.

Financial PhraseBank contains 4,845 English sentences from financial news collected from LexisNexis and annotated by 16 people with finance or business backgrounds. Labels are positive, negative, or neutral according to the expected effect of the sentence on the mentioned company's stock price. The full dataset is imbalanced, with about 59% neutral observations. The authors use a train/validation/test split for some experiments and 10-fold cross-validation for others.

FiQA Task 1 contains 1,174 financial news headlines and tweets with continuous sentiment scores ranging from -1 to 1 and a specified target financial entity. The paper evaluates this task using 10-fold cross-validation.

For domain-adaptive pre-training, the authors construct TRC2-financial from Reuters TRC2. The filtered corpus contains 46,143 financial documents, more than 29 million words, and nearly 400,000 sentences.

## Methods
FinBERT starts from pre-trained BERT and adapts it to financial sentiment analysis. The authors further pre-train BERT using either the Reuters-derived TRC2-financial corpus or the target-task training sentences, then fine-tune the model on labeled financial sentiment data. For classification, a dense output layer is added to the final [CLS] representation; for the continuous FiQA task, the architecture is adapted for regression using mean squared error.

FinBERT is compared with an LSTM using GloVe embeddings, an LSTM using ELMo contextual embeddings, ULMFit, and previously published financial sentiment models. Classification performance is evaluated with cross-entropy loss, accuracy, and macro F1; FiQA regression is evaluated with MSE and R².

The paper also conducts ablation-style experiments on domain-specific further pre-training, catastrophic forgetting, encoder-layer choice, and partial fine-tuning. To reduce catastrophic forgetting, it experiments with slanted triangular learning rates, gradual unfreezing, and discriminative fine-tuning. It also tests how performance changes when only the upper BERT layers are fine-tuned.

## Main findings
FinBERT achieves the strongest reported performance on both financial sentiment datasets. On the full Financial PhraseBank, FinBERT reaches approximately 0.86 accuracy and 0.84 macro F1, compared with 0.83 accuracy and 0.79 F1 for ULMFit. On the subset with 100% annotator agreement, FinBERT reaches about 0.97 accuracy and 0.95 F1.

FinBERT also performs well with relatively little labeled data. With only 250 training examples, it reaches about 80% accuracy, and the paper reports that it surpasses prior state-of-the-art methods with a training set as small as 500 observations. This supports the usefulness of language-model pre-training when labeled financial data are scarce.

On FiQA, FinBERT reports lower MSE and higher R² than the comparison results, although the evaluation is not directly identical because FinBERT uses 10-fold cross-validation while the prior studies used the official FiQA test set.

Further pre-training on the financial corpus produces only a small improvement over vanilla BERT. The authors therefore do not conclude that domain-specific additional pre-training is clearly necessary in this setting. Fine-tuning strategies designed to limit catastrophic forgetting improve training stability, with gradual unfreezing appearing particularly useful.

The last BERT encoder layer performs best for classification, but strong performance can be obtained without fine-tuning the entire model. Fine-tuning only the upper layers produces performance close to full fine-tuning while substantially reducing training time.

## Relevance to my paper
Araci (2019) is a foundational paper for transformer-based financial sentiment analysis and provides an important benchmark for comparing traditional sentiment methods with contextual language models. It demonstrates that a BERT-based model can substantially outperform dictionary-based or earlier machine-learning and neural approaches on labeled financial sentiment tasks, particularly when labeled data are limited.

The paper is directly relevant to comparing simple and sophisticated sentiment methods. It motivates the argument that contextualized language models may capture semantic information that bag-of-words and dictionary methods miss, while also showing that greater model complexity should be evaluated empirically rather than assumed to be beneficial.

It is also useful for separating linguistic classification performance from economic usefulness. Araci evaluates FinBERT primarily against human-annotated sentiment labels, not against realized market returns. This creates an important distinction for my study: a model may achieve higher classification accuracy yet still need separate validation to determine whether its sentiment scores are economically informative.

The finding that domain-specific further pre-training adds only modest improvement is also relevant when considering whether financial-domain adaptation necessarily produces materially better results. Likewise, the strong performance with small labeled samples supports the use of transfer learning where manually labeled financial text is expensive or scarce.

## How I might cite it
- FinBERT provides early evidence that transformer-based transfer learning can substantially improve financial sentiment classification.
- Pre-trained language models can perform well in financial NLP even when labeled financial datasets are relatively small.
- Contextual language models can outperform traditional machine-learning and earlier neural approaches on financial sentiment benchmarks.
- Financial PhraseBank sentiment labels are defined according to the expected effect of a sentence on the referenced company's stock price.
- Domain-specific further pre-training produced only modest gains over vanilla BERT in this study.
- Fine-tuning the entire transformer is not always necessary; strong performance can be achieved by fine-tuning only the upper layers.
- Classification accuracy against human sentiment labels is distinct from market-based validation of whether model outputs predict realized returns.
- Useful contrast for my paper: Araci evaluates English financial news sentences, headlines, and tweets, whereas my study examines Japanese financial disclosures and evaluates whether sentiment measures have economic relevance.

## Possible literature-review section
Primary: Transformer-based financial sentiment analysis

Secondary: Pre-trained language models and transfer learning; domain adaptation in financial NLP; financial sentiment benchmarks; traditional versus contextual sentiment models

## Important quotes / page numbers
- p. 1 (PDF p. 2) — Motivation: financial sentiment analysis is difficult because financial language is specialized and labeled data are scarce; the paper proposes pre-trained language models as a solution.
- p. 1 (PDF p. 2) — Limitation of lexicons: the author argues that word-counting methods do not capture the deeper semantic meaning of financial text.
- p. 4 (PDF p. 5) — Research questions: the paper explicitly asks how FinBERT compares with ELMo, ULMFit, and prior state-of-the-art methods; whether domain-specific pre-training helps; and how much of BERT needs to be fine-tuned.
- p. 6 (PDF p. 7) — Main Financial PhraseBank result: FinBERT reports 0.86 accuracy and 0.84 F1 on the full dataset and 0.97 accuracy and 0.95 F1 on the 100%-agreement subset.
- p. 7 (PDF p. 8) — Domain adaptation result: further pre-training on the financial corpus yields only a small improvement over vanilla BERT, and the author says more experiments are needed before concluding that domain pre-training is unimportant.
- p. 8 (PDF p. 9) — Computational result: strong classification performance can be achieved by fine-tuning only a subset of BERT's upper layers.
- p. 9 (PDF p. 10) — Conclusion: the paper states that this is, to the author's knowledge, the first application of BERT to finance and reports state-of-the-art performance on both datasets.
- p. 9 (PDF p. 10) — Future-work point especially relevant to my paper: the author notes that financial sentiment analysis is useful only insofar as it supports financial decisions and proposes testing FinBERT directly against stock-market return data.

## Caveats / limitations
The study evaluates FinBERT primarily on short English financial sentences, news headlines, and tweets rather than long corporate disclosures. Generalization to longer documents, other languages, or different disclosure regimes is therefore uncertain.

The labeled datasets are small. Financial PhraseBank contains only 4,845 sentences and FiQA only 1,174 observations. Financial PhraseBank is also heavily weighted toward the neutral class, and human annotators themselves disagree substantially on some positive-versus-neutral cases.

The reported FiQA comparison is not strictly apples-to-apples: FinBERT is evaluated using 10-fold cross-validation because the author did not have access to the official test set used by the comparison studies. The paper also explicitly notes that its LSTM, ELMo, and ULMFit baselines were not experimented with as extensively as BERT, so the model-to-model comparisons should not be treated as definitive.

Further financial-domain pre-training provides only a small improvement over vanilla BERT in these experiments, so the paper does not establish that domain-adaptive pre-training is consistently necessary or beneficial.

Most importantly for market-oriented research, the paper validates sentiment against annotated labels rather than realized stock-market outcomes. Financial PhraseBank labels reflect annotators' expectations about stock-price impact, not observed abnormal returns. The paper itself identifies direct testing against market return data as future work. Thus, strong classification performance does not by itself establish that FinBERT sentiment is economically informative.
