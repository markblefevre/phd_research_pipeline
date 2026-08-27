# Cohen et al. (2020)

## Local filename
Cohen_Malloy_Nguyen_2020_Lazy_Prices.pdf

## Citation
@article{cohen2020lazy,
  title={Lazy prices},
  author={Cohen, Lauren and Malloy, Christopher and Nguyen, Quoc},
  journal={The Journal of Finance},
  volume={75},
  number={3},
  pages={1371--1415},
  year={2020},
  publisher={Wiley Online Library}
}

## Research question
What does the paper ask?

Do year-over-year changes in the language and construction of firms' periodic SEC filings contain information about future firm fundamentals and stock returns that investors fail to incorporate when the filings are released?

More specifically, the paper asks whether changes in 10-K and 10-Q text predict future returns and operating outcomes, which sections and types of textual changes are most informative, and whether delayed price incorporation can be explained by investor inattention to changes across successive filings.

## Data
The authors download the complete set of regular U.S. corporate 10-K, 10-K405, 10-KSB, and 10-Q filings from EDGAR over 1995–2014.

The reported dataset contains approximately:
- 86,965 10-K filings, and
- 258,271 10-Q filings,

for roughly 345,000 periodic reports in total before individual similarity-measure availability restrictions.

The main filing text is retained while tables with heavy numeric content, HTML and XBRL material, exhibits, graphics, PDFs, spreadsheets, and other binary material are removed.

Market and accounting data come from CRSP and Compustat. Analyst data come from I/B/E/S. Loughran-McDonald sentiment categories are used for some mechanism tests.

The paper also uses SEC EDGAR download-log data obtained through a Freedom of Information Act request. These logs identify filing downloads, timestamps, and partially masked IP addresses, allowing the authors to construct a proxy for investor attention based on whether investors download both the current and prior-year filings.

## Methods
The central empirical variable is the similarity between a firm's current filing and the comparable filing from the prior year. A 10-K is compared with the prior year's 10-K, while each 10-Q is compared with the same fiscal quarter's 10-Q from the prior year.

The authors use four document-similarity measures:

- **Cosine similarity:** based on term-frequency vectors.
- **Jaccard similarity:** intersection divided by union of the sets of words in the two documents.
- **Minimum edit distance:** based on the number of additions, deletions, or other operations required to transform one document into the other.
- **Simple similarity:** a side-by-side `diff` / Microsoft Word Track Changes approach that counts additions, deletions, and modifications relative to document size.

Higher scores indicate more similar documents; lower scores indicate greater year-over-year textual change.

The authors form monthly quintile portfolios based on document similarity. The lowest-similarity quintile contains the largest “changers,” while the highest-similarity quintile contains firms making little or no change. Stocks enter portfolios in the month after a filing and are generally held for three months, with portfolios rebalanced monthly.

Portfolio performance is evaluated using equal-weighted and value-weighted returns and common factor models. The authors also run monthly Fama-MacBeth regressions controlling for known return predictors such as size, book-to-market, past returns, standardized unexpected earnings, accruals, investment, profitability, and other firm characteristics.

They separately examine particular filing sections, including MD&A and Risk Factors, and use the Loughran-McDonald dictionary to characterize changed text according to negative sentiment, uncertainty, and litigiousness. Changes involving executives and litigation are also identified.

To investigate investor attention, the authors use SEC download logs to identify cases in which investors download both the current and prior-year 10-K, reasoning that simultaneous multi-year downloads make comparison of textual changes more likely. They also examine filing dates with unusually many earnings announcements as periods of greater investor distraction.

Finally, they test whether document changes predict subsequent operating performance, earnings surprises, 8-K announcements, short interest, news events, and bankruptcy.

## Main findings
Substantial changes in periodic filings predict lower future stock returns, while firms whose disclosures change little subsequently earn higher returns.

Across the broad document-level measures, a portfolio that is long high-similarity “non-changers” and short low-similarity “changers” earns approximately 34–58 basis points per month in value-weighted abnormal returns, depending on the similarity measure. These return differences accumulate gradually after the filing and do not reverse.

There is essentially no contemporaneous announcement effect when the changed filing is released. Instead, the return differential appears over the following months as information foreshadowed by the filing changes is subsequently revealed through earnings, news, and other firm events.

The strongest effects occur in particular kinds of text. Changes to MD&A are informative, but changes in the Risk Factors section are especially predictive. The paper reports a five-factor alpha of more than 188 basis points per month for a non-changer-minus-changer portfolio based specifically on Risk Factor changes. Changes involving executives, litigation, and more negative language are also especially informative.

The textual changes predict real future outcomes, not merely returns. Firms with greater changes subsequently experience weaker operating income, net income, and sales, as well as evidence of more future 8-K filings, greater short interest, negative earnings surprises, and more bankruptcies.

The average relation is negative partly because most signed changes are negative. The authors report that roughly 86% of sentiment-classified changes are negative. When positive changes are isolated, they predict positive subsequent returns.

The investor-attention tests support the paper's proposed mechanism. When investors are more likely to compare current and prior filings—as proxied by downloading both years' filings—the long-run return predictability is weaker and the immediate filing-date reaction is stronger. Return predictability is also larger on high-distraction filing dates.

The authors therefore interpret the results as evidence that investors underreact to subtle but important changes in corporate reports because comparing long textual documents is costly and receives insufficient attention.

## Relevance to my paper
This is a central paper for the textual-novelty component of my study because it provides strong evidence that **change relative to prior disclosure contains economically relevant information beyond the level of textual characteristics in the current document**.

The paper extends the Brown and Tucker (2011) idea of year-over-year disclosure modification by showing that textual change predicts not only operating changes but large subsequent return differences. It also offers a different interpretation of weak filing-date reactions: a small contemporaneous response need not mean that textual changes are uninformative; investors may simply fail to process them immediately.

This distinction is particularly important for my work. If sentiment is measured on the full document, repeated or boilerplate language may dominate the signal even though the economically important information lies in what changed. Cohen et al. therefore provide a strong rationale for examining sentiment jointly with textual novelty or for calculating sentiment on changed/new text separately from unchanged text.

The paper also shows that **novelty and sentiment are distinct constructs**. The similarity measures remain predictive after controlling for sentiment, while the sentiment of the changed passages provides additional information about the direction of future outcomes. This suggests that novelty answers “how much changed?” while sentiment answers “in what direction is the changed information?”

It is also highly relevant to economic validation. The textual similarity measures are constructed independently of subsequent returns; market outcomes are then used to test whether the measures contain economically meaningful information. This is a cleaner external-validation design than training the textual signal directly on returns.

Finally, the investor-attention evidence provides a plausible mechanism for delayed market incorporation. This is useful when interpreting differences between short-window CAR tests and longer-horizon return-predictability tests: economically meaningful textual information may not be fully reflected in prices immediately around the filing date.

## How I might cite it
- Year-over-year textual changes in corporate filings contain economically meaningful information about future firm performance and stock returns.
- Firms that make larger changes to their 10-K and 10-Q disclosures subsequently underperform firms whose disclosures remain relatively unchanged.
- Textual-change signals can predict future returns even when there is little or no market reaction at the filing date.
- Weak contemporaneous market reaction does not necessarily imply that disclosure changes are uninformative; investors may incorporate the information only gradually.
- Changes in Risk Factors, MD&A, executive-related language, litigation language, and negative language are especially informative.
- Document similarity and sentiment capture distinct dimensions of financial text and can provide incremental information relative to one another.
- Investor attention appears to affect the speed with which textual changes are incorporated into prices.
- Comparing current disclosure with prior disclosure can reveal information that is difficult to identify from the current document in isolation.
- The paper provides strong motivation for treating textual novelty/change as a separate explanatory dimension from sentiment.
- Useful contrast for my paper: Cohen et al. analyze English U.S. 10-K/10-Q filings using lexical similarity measures, whereas my study examines Japanese financial disclosures and can test whether novelty alters the economic usefulness of alternative sentiment models.

## Possible literature-review section
Primary: Textual novelty

Secondary: Disclosure change and similarity; investor attention and underreaction; economic validation of textual measures; sentiment versus novelty

## Important quotes / page numbers
- NBER abstract / PDF p. 2 — Core result: changes in filing language predict future returns and firm operations, while prices show little filing-date response and adjust only when later events reveal the information.
- Working-paper pp. 1–2 / PDF pp. 3–4 — Main interpretation: the lack of an announcement effect is attributed to investors missing subtle textual changes rather than to filings becoming economically useless.
- Working-paper pp. 4–6 / PDF pp. 6–8 — Main return result: a broad long non-changers / short changers strategy earns approximately 34–58 basis points per month in value-weighted abnormal returns, with returns accruing gradually and not reversing.
- Working-paper p. 5 / PDF p. 7 — Section-level result: changes in Risk Factors are especially informative, with a reported five-factor alpha exceeding 188 basis points per month for the section-specific non-changer-minus-changer portfolio.
- Working-paper pp. 5–6 / PDF pp. 7–8 — Real-outcome result: filing changes predict future earnings, profitability, news, earnings surprises, and bankruptcy.
- Working-paper p. 6 / PDF p. 8 — Direction of changes: approximately 86% of sentiment-classified textual changes are negative, while the smaller group of positive changes predicts positive future returns.
- Working-paper pp. 8–9 / PDF pp. 10–11 — Attention mechanism: return predictability is attenuated when investors download both current and prior-year filings, while immediate announcement effects are stronger.
- Working-paper pp. 12–14 / PDF pp. 14–16 — Similarity methodology: the authors define cosine, Jaccard, minimum-edit-distance, and side-by-side `diff` measures for comparing successive filings.
- Working-paper pp. 17–20 / PDF pp. 19–22 — Portfolio and Fama-MacBeth evidence: all four similarity measures predict subsequent returns, with greater document changes associated with lower future returns.
- Working-paper pp. 25–27 / approximately PDF pp. 27–29 — Mechanism and real effects: greater investor attention weakens delayed predictability, and textual changes forecast future operating deterioration and other adverse firm events.

## Caveats / limitations
The document-similarity measures are primarily lexical rather than semantic. Cosine and Jaccard similarity depend on word overlap, minimum edit distance on textual operations, and the simple measure on additions/deletions. These approaches can identify that wording changed without necessarily understanding whether two differently worded passages convey the same meaning or whether identical language has acquired a different economic meaning.

The paper's central return signal is **unsigned document change**, yet the average empirical relation is strongly negative because most classified changes in the sample are negative. The fact that roughly 86% of signed changes are negative means the general “changers underperform” result should not be interpreted as a universal theoretical prediction that all disclosure change is bad news. The authors themselves show that positive changes predict positive returns.

The attention mechanism is supported by proxies rather than direct observation of investor cognition. Downloading both current and prior-year filings plausibly indicates comparison, but investors may download documents for other reasons, may compare filings obtained elsewhere, or may rely on intermediaries rather than EDGAR. The evidence is therefore consistent with inattention but does not uniquely establish it as the causal mechanism.

The very large 188-basis-point monthly alpha applies to a **specific Risk Factor section strategy**, not to the broad whole-document result. The general whole-document value-weighted long-short effect is much smaller, around 34–58 basis points per month. These magnitudes should not be conflated.

The paper conducts many section-level, phrase-level, outcome, portfolio, and robustness tests. The breadth of the analysis is informative, but especially large subgroup results should be interpreted with some caution because multiple specification and selection choices can increase the chance of finding extreme estimates.

Although the authors argue that transaction costs and limits to arbitrage are unlikely to explain the return pattern, the portfolio exercises are not a full implementation study. Real-world shorting constraints, turnover, trading costs, taxes, and evolving market microstructure could reduce exploitable profitability.

The sample covers U.S. filings from 1995–2014. Disclosure formats, EDGAR usage, investor technology, automated text comparison, and algorithmic processing have evolved substantially since then. The persistence and magnitude of investor inattention may differ in more recent markets.

The study examines both 10-Ks and 10-Qs and generally uses full filing text, whereas studies restricted to MD&A or another specific section may obtain different similarity distributions and economic relationships.

Finally, the design establishes strong predictive relationships but not a clean causal effect of textual change itself on future firm outcomes. Firms alter disclosures in response to underlying economic developments; the text changes are therefore signals of those developments rather than necessarily causes of the subsequent operational or return outcomes.
