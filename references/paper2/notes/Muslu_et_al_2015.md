# Muslu et al. (2015)

## Local filename
Muslu_et_al_Forward_2015.pdf

## Citation
@article{muslu2015forward,
  title={Forward-looking MD\&A disclosures and the information environment},
  author={Muslu, Volkan and Radhakrishnan, Suresh and Subramanyam, KR and Lim, Dongkuk},
  journal={Management Science},
  volume={61},
  number={5},
  pages={931--948},
  year={2015},
  publisher={INFORMS}
}

## Research question
The paper studies the **quantity of forward-looking information in MD&A**, rather than its sentiment or tone.

It asks two main questions:

1. Do firms with relatively poor information environments provide more forward-looking disclosure in MD&A?
2. Does forward-looking MD&A disclosure subsequently improve the firm’s information environment?

The authors define the information environment through the **informational efficiency of stock prices**: the extent to which current stock returns incorporate information about future earnings.

This makes the paper importantly different from Li (2010). Li focuses primarily on the **tone of forward-looking statements** and whether that tone predicts future fundamentals. Muslu et al. focus on the **amount of forward-looking disclosure** and whether it helps stock prices “bring the future forward.”

## Data
- U.S. Form 10-K filings.
- Fiscal years 1993–2009.
- MD&A sections extracted from SEC filings.
- Initial sample: **44,708 10-K filings from 5,705 firms**.
- The main return/future-earnings tests use a smaller sample (36,043 observations) because they require returns and earnings data extending from before the filing through three future fiscal years.
- On average, an annual report contains about 1,531 sentences, of which about 301 are in MD&A.
- The average MD&A contains approximately 39 forward-looking sentences.
- Mean forward-looking intensity (forward-looking sentences / total MD&A sentences) is about 12.8%.

The authors restrict the study to 10-K rather than 10-Q MD&A because they view annual-report forward-looking disclosure as more comprehensive and because annual frequency fits their empirical design.

## Methods

### 1. Identifying forward-looking sentences
The authors develop a rule-based computational method to identify forward-looking sentences in MD&A.

The unit of analysis is the **sentence**, which they regard as the smallest integral textual unit conveying an idea.

A sentence is classified as forward-looking when it contains indicators such as:

- explicit future references (for example, “next year”);
- verbs or constructions indicating plans, expectations, estimates, forecasts, intentions, targets, etc.; or
- numerical references to future years.

Examples of verbs used include variants of:
- aim
- anticipate
- assume
- commit
- estimate
- expect
- forecast
- foresee
- hope
- intend
- plan
- project
- seek
- target

Importantly, the authors deliberately exclude several modal words such as **shall, should, can, could, may, and might**, because these frequently occur in legal or boilerplate language without meaningful forward-looking content.

They validate the automated extraction by comparing the algorithm’s classifications for randomly selected MD&A disclosures with human classifications.

### 2. Forward-looking disclosure measures
They construct two primary measures:

**FLI (Forward-Looking Intensity)**  
`number of forward-looking MD&A sentences / total MD&A sentences`

and

**FLS**  
`log(1 + number of forward-looking MD&A sentences)`

The paper therefore measures the **extent or quantity** of forward-looking disclosure rather than its positive/negative sentiment.

### 3. Abnormal forward-looking disclosure
Because firms naturally differ in how much forward-looking disclosure they are expected to provide, the authors regress FLI/FLS on firm characteristics.

Controls include:
- analyst following;
- management guidance;
- firm size;
- profitability/loss status;
- earnings volatility;
- stock-return volatility;
- special items and M&A;
- book-to-market;
- firm age;
- business/geographic segments;
- financial complexity;
- MD&A emphasis;
- industry; and
- year.

The residual from this model is the key measure of **abnormal forward-looking disclosure**:

- AFLI = abnormal forward-looking intensity
- AFLS = abnormal number of forward-looking sentences

Thus, “abnormal” means forward-looking disclosure above or below what would be expected given the firm’s characteristics.

### 4. Measuring the information environment
The empirical design follows Lundholm and Myers (2002).

The basic idea is that an informative stock price should incorporate information about **future earnings**. The authors regress current stock returns on current and future earnings and interact future earnings with abnormal forward-looking disclosure.

They examine several return windows:

- an 11-month pre-filing window;
- an alternative 3-month pre-filing window;
- a one-month filing window beginning around the 10-K filing;
- combined pre-filing + filing windows.

The pre-filing regressions ask whether firms that subsequently provide more abnormal forward-looking disclosure started with poorer information environments.

The filing-window regressions ask whether the disclosure helps stock prices incorporate future earnings information.

This distinction is central to the paper.

### 5. Content and horizon analysis
The authors further classify forward-looking statements into:
- operations-related;
- finance-related; and
- accounting-related disclosures.

They also distinguish relatively **short-horizon** from **long-horizon** forward-looking disclosure.

## Main findings

### 1. Firms with poor information environments provide more forward-looking disclosure
Firms with greater abnormal forward-looking MD&A disclosure have **pre-filing stock returns that are less strongly associated with future earnings**.

The interpretation is that firms provide additional prospective disclosure partly when their existing information environment is poor.

### 2. Forward-looking MD&A disclosure improves the information environment
During the filing period, greater abnormal forward-looking disclosure is associated with stock returns that **more strongly incorporate future earnings information**.

Thus, forward-looking MD&A appears to help investors understand future firm performance.

### 3. The disclosure does not completely eliminate the information disadvantage
Across the combined pre-filing and filing period, firms with unusually high forward-looking disclosure still have returns that are less informative about future earnings.

Forward-looking disclosure therefore **mitigates**, but does not completely eliminate, the initial information disadvantage.

### 4. Results are particularly strong for loss firms
The principal findings are largely driven by firms reporting losses.

This suggests that forward-looking disclosure may be particularly useful when current accounting performance is less informative about the future.

### 5. Operations-related FLS are the important category
The results are primarily driven by **operations-related forward-looking disclosure**.

Finance-related and accounting-related forward-looking statements generally do not produce the same improvement in the information environment.

This is an important information-location result: not all forward-looking disclosure is equally informative.

### 6. Short-horizon FLS are more informative
Short-horizon forward-looking disclosures show the expected relation with the information environment, whereas long-horizon measures are largely insignificant.

The authors conclude that shorter-horizon disclosure is more effective in “bringing the future forward.”

## Relevance to my paper

This is a **supporting rather than foundational paper** for my Paper 2, but several aspects are surprisingly relevant.

### 1. It establishes that information location/type within MD&A matters
Muslu et al. do not treat MD&A as homogeneous. They isolate forward-looking sentences and further show that **operations-related forward-looking disclosure drives the results**, whereas finance- and accounting-related FLS generally do not.

This supports the broader premise behind my paper:

> The economically useful textual signal may be concentrated in particular portions of a long disclosure.

My partition is different — **novel versus persistent text** — but the underlying motivation is closely related.

### 2. It explicitly separates text selection from sentiment measurement
Muslu et al. deliberately study the **quantity of FLS rather than tone**. They even explain that identifying whether a sentence is forward-looking can be much easier than determining whether its tone is positive or negative without sufficient context.

This is directly relevant to my distinction between:

1. **Which text contains information?**
2. **Which sentiment technology measures that information best?**

My paper can argue that these are separate empirical questions. Novelty determines where potentially incremental information resides; LMMD/BERT/GPT determine how sentiment is measured within that information.

### 3. It contains a particularly important observation about persistence
The authors report significant persistence in firms’ forward-looking disclosures but explicitly caution that **persistent language is not necessarily stale information**.

Their example is a firm making essentially the same type of projection in consecutive years while changing the numerical forecast. The linguistic structure is highly similar, yet the changed number clearly conveys updated information.

This is directly relevant to my novelty design.

It warns against defining “persistent” text as automatically “uninformative.” A high-similarity sentence can contain a small but economically important change.

For my paper, this argues for:
- continuous novelty measures rather than only a hard novel/persistent split;
- sentence-level semantic comparison that is sensitive to changed values where possible;
- robustness checks around numerical changes; and
- careful terminology: **persistent/high-similarity** rather than “boilerplate/uninformative” unless that characterization is independently established.

### 4. It provides an important bridge to Brown & Tucker (2011)
Muslu et al. explicitly discuss Brown & Tucker’s finding that the market reacts more strongly when firms modify their MD&A more extensively.

That places **textual change/novelty** directly alongside the FLS literature.

This helps establish a literature progression:

**Li (2010)**  
Forward-looking tone → future fundamentals

**Brown & Tucker (2011)**  
MD&A modification/change → market response

**Muslu et al. (2015)**  
Forward-looking disclosure quantity → information environment

**Kato & Goto (2021)**  
Japanese MD&A tone / inferred future outlook → future performance

**My Paper 2**  
Textual novelty × sentiment technology → market-relevant sentiment in Japanese disclosures

### 5. Its market-based validation is closer to my paper than Kato & Goto
Kato & Goto primarily validate tone against future ROA.

Muslu et al. explicitly use **stock returns** and ask how much information about future earnings those returns incorporate.

This is not the same as my filing-window CAR event study, but conceptually it is closer to my use of the market as an external validation criterion.

### 6. It reinforces my “information dilution” argument
If operations-related FLS contain useful information while other FLS categories do not, aggregating everything together can dilute economically meaningful signals.

That parallels my hypothesis that contextual models may add value on genuinely informative/novel text but that this advantage can disappear when scores are aggregated over persistent or less informative disclosure.

### 7. It suggests a possible robustness test, not a new core dimension
Forward-looking versus non-forward-looking text could eventually be used as a secondary partition:

- novel + forward-looking;
- novel + non-forward-looking;
- persistent + forward-looking;
- persistent + non-forward-looking.

However, adding this to the core design risks making the paper unnecessarily multidimensional.

The better use of Muslu et al. is to motivate the principle that **information content varies across text**, while retaining **novelty × sentiment technology** as my central empirical design.

## How I might cite it

**For information location:**

> Prior research demonstrates that economically useful information is not distributed uniformly across MD&A. Muslu et al. (2015), for example, show that the information-environment effects of forward-looking MD&A disclosures are concentrated in operations-related statements.

**For selective text analysis:**

> Large-sample textual research has long recognized the value of isolating economically motivated subsets of disclosure. Muslu et al. (2015) identify forward-looking sentences within MD&A and show that their prevalence is associated with the extent to which stock prices incorporate future earnings information.

**For the distinction between text selection and sentiment:**

> Muslu et al. (2015) focus deliberately on the quantity of forward-looking disclosure rather than its tone, highlighting the distinction between identifying potentially informative text and measuring the sentiment expressed within that text.

**For the information environment:**

> Muslu et al. (2015) find that firms with relatively poor pre-filing information environments provide more forward-looking MD&A disclosure and that such disclosure subsequently improves, but does not fully eliminate, the deficiency in the extent to which prices reflect future earnings.

**Possible bridge into my research gap:**

> Prior studies show that both textual change and economically motivated text selection can identify informative components of MD&A (Brown and Tucker, 2011; Muslu et al., 2015). However, these studies do not ask whether the relative effectiveness of alternative sentiment-measurement technologies depends on whether the underlying disclosure is novel relative to the firm’s prior filing.

## Possible literature-review section

Primary:
**Information location / forward-looking disclosure in MD&A**

Secondary:
**Foundations of computational financial-text analysis**

Also useful as a bridge between:
- Li (2010);
- Brown & Tucker (2011);
- Kato & Goto (2021); and
- my novelty × technology research question.

I probably do **not** need a long standalone subsection on Muslu et al. The paper is best used as part of a short literature thread showing that economically meaningful information can be concentrated in particular components of MD&A.

## Important quotes / page numbers

- **pp. 1–2:** Research questions and design. The paper asks whether firms with poor information environments provide more forward-looking MD&A disclosure and whether that disclosure improves the information environment.
- **pp. 2–3:** Main results and distinction between pre-filing and filing-period information environments.
- **pp. 4–5:** Positioning relative to Li (2010), Feldman et al. (2010), and Brown & Tucker (2011). Particularly useful because the authors explicitly distinguish the *quantity* of FLS from *tone*.
- **pp. 6–8:** Construction of forward-looking disclosure measures and sample characteristics.
- **p. 8, footnote 12:** **Especially important for my paper.** The authors note that persistent forward-looking disclosure does not necessarily imply stale information; very similar statements can contain updated numerical projections.
- **pp. 10–12:** Main information-environment regressions and interpretation of the pre-filing versus filing-period results.
- **pp. 14–16:** Cross-sectional results. Loss firms drive much of the effect; operations-related and short-horizon forward-looking disclosures are most informative.
- **pp. 16–17:** Conclusion and limitations.
- **Appendix:** Detailed rule-based procedure for extracting MD&A and identifying forward-looking sentences.

## Caveats / limitations

- **U.S./English setting:** The paper studies U.S. 10-K MD&A, not Japanese 有価証券報告書.
- **FLS quantity rather than sentiment:** It does not directly compare sentiment models and therefore cannot answer my central LMMD/BERT/GPT question.
- **Rule-based FLS identification:** The forward-looking classifier relies on keywords, phrases, grammatical constructions, and dates rather than modern semantic representations.
- **Potential classification error:** The empirical conclusions are jointly dependent on the validity of the forward-looking disclosure measure and the economic hypotheses.
- **Association rather than causality:** The authors explicitly caution that their results do not establish that FLS disclosure *causes* the subsequent improvement in the information environment.
- **Other 10-K information may confound the result:** Other disclosure occurring in the filing may correlate with abnormal FLS and contribute to the market effect.
- **Other disclosure channels:** The study cannot explain why firms choose MD&A rather than press releases, conference calls, or other communication channels.
- **Persistence is conceptually tricky:** Similar language can contain updated information. This is especially important for my own novelty measure and argues against equating textual similarity mechanically with lack of information.
- **Information environment is not filing-window CAR:** Their dependent-variable design asks whether returns incorporate future earnings; my event-study design asks whether sentiment is associated with abnormal market reaction around disclosure.
- **Long sample spans regulatory regimes:** The 1993–2009 period includes Reg FD and other disclosure-environment changes. The authors address some of this with time partitions, but institutional conditions vary materially across the sample.

## Connection to Li (2010), Kato & Goto (2021), and my paper

### Li (2010)
**Question:** What is the tone/content of explicit forward-looking statements, and does that tone predict future fundamentals?

**Text selection:** Explicit FLS.

**Sentiment technology:** Supervised Naive Bayes trained on manually classified sentences.

**Economic validation:** Future earnings/liquidity and analyst behavior.

### Muslu et al. (2015)
**Question:** Why do firms provide more forward-looking MD&A disclosure, and does that disclosure improve the information environment?

**Text selection:** Rule-based identification of explicit FLS.

**Sentiment technology:** None — quantity/intensity of FLS is the key textual variable.

**Economic validation:** Ability of stock returns to incorporate future earnings.

### Kato & Goto (2021)
**Question:** Does Japanese MD&A tone contain managers’ future outlook that predicts subsequent performance?

**Text selection:** Full Japanese MD&A rather than explicit FLS.

**Sentiment technology:** Japanese translation of the Loughran-McDonald financial dictionary.

**Economic validation:** Primarily future ROA.

### My Paper 2
**Question:** Does the relative usefulness of sentiment technology depend on whether the underlying disclosure contains genuinely new information?

**Text selection:** Novel versus persistent disclosure relative to the firm’s prior filing.

**Sentiment technology:** LMMD vs Japanese Financial BERT vs GPT.

**Economic validation:** Market reaction / abnormal returns.

The important distinction is therefore:

**Li:** forward-looking text × tone  
**Muslu:** forward-looking text × information environment  
**Kato & Goto:** Japanese MD&A tone × future performance  
**My paper:** textual novelty × sentiment technology × market relevance

## Key takeaway for my project

Muslu et al. does **not** threaten my research contribution.

If anything, it provides useful conceptual support for the first half of my argument: **the information content of long financial disclosures depends on which portion of the text is being measured.**

The particularly useful insight is their warning that **persistent language is not necessarily stale language**. My paper should therefore avoid defining persistence as synonymous with boilerplate. The empirical question should instead be whether sentiment measured from *higher-novelty* text is more market relevant and whether contextual NLP gains relative to lexical methods as novelty increases.

That formulation remains distinct from Muslu et al. and makes the novelty × technology interaction the centerpiece.
