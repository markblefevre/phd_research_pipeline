Prior Work

1. Foundations of financial sentiment measurement

-   ☒ Tetlock (2007)
-   ☒ Tetlock, Saar-Tsechansky & Macskassy (2008)
-   ☒ Loughran & McDonald (2011)
-   ☒ Feldman et al. (2010)
-   ☒ Henry & Leone (2016)
-   ☒ Li (2010)
-   ☒ Glasserman & Mamaysky (2019)

This section establishes two foundational propositions: financial
language contains economically meaningful information, and the
measurement of that information is itself a nontrivial empirical
problem. Tetlock (2007) and Tetlock, Saar-Tsechansky & Macskassy (2008)
provide early evidence that textual tone in financial news is associated
with market returns, trading activity, firm fundamentals, and subsequent
price behavior. Feldman et al. (2010) extend this evidence to managerial
disclosure, showing that changes in MD&A tone contain information beyond
conventional quantitative signals.

The methodological literature then shifts from whether tone matters to
how it should be measured. Loughran & McDonald (2011) demonstrate that
general-purpose sentiment dictionaries can perform poorly in financial
text because words that appear negative in ordinary language often have
neutral or specialized meanings in financial contexts. Henry & Leone
(2016) reinforce the importance of domain-specific measurement and show
that the choice of textual measure can materially affect statistical
power and economic inference. Their evidence also cautions against
assuming that methodological complexity necessarily produces a superior
economic signal.

Li (2010) provides an important early bridge from lexical methods to
supervised machine learning and from whole-document measurement to
economically motivated text selection. Using a Naive Bayes classifier
trained on manually coded forward-looking statements (FLS) from MD&A, Li
classifies both tone and content and shows that forward-looking tone
contains incremental information about subsequent firm performance and
liquidity. The study therefore establishes an important distinction
between identifying the portion of a disclosure likely to contain
relevant information and measuring the tone expressed within that
portion.

Glasserman & Mamaysky (2019) broaden the measurement problem further by
showing that sentiment can interact with textual unusualness. Their
results suggest that the economic relevance of tone may depend not only
on its direction but also on whether the underlying language is unusual
or informative.

The central lesson from this literature is therefore not merely that
financial text matters. Rather, economically useful textual measurement
depends on domain-appropriate language representation, the location of
information within the document, and validation against economically
meaningful outcomes.

------------------------------------------------------------------------

2. Machine learning, contextual models, and domain adaptation

-   ☒ Li (2010)
-   ☒ Araci (2019)
-   ☒ Huang, Wang & Yang (2023)
-   ☒ Frankel, Jennings & Lee (2022)
-   ☒ Suzuki et al. (2023)
-   ☒ Siano (2025)

Machine-learning approaches address limitations of fixed dictionaries by
allowing textual meaning to be inferred from patterns in data rather
than assigned solely through predetermined word lists. Li (2010) is an
early financial application of this approach: a supervised Naive Bayes
classifier learns to distinguish the tone and content of forward-looking
MD&A statements from manually labeled examples. This provides a useful
historical precursor to later contextual language models.

Araci (2019) introduces FinBERT and demonstrates the value of adapting
transformer representations to financial language. Huang, Wang & Yang
(2023) further develop finance-specific transformer approaches through
large-scale domain pre-training, providing evidence that contextual
representations can improve the extraction of financially relevant
information.

A related literature moves from linguistic classification toward
market-based prediction. Frankel, Jennings & Lee (2022) use supervised
machine-learning methods trained on disclosure text to explain market
reactions and generally find that learned textual measures outperform
dictionary measures. Siano (2025) pushes this approach further by
fine-tuning a RoBERTa-based contextual model directly on short-window
abnormal stock returns and showing that contextual models can extract
substantial market-relevant information from earnings-announcement
disclosures.

Suzuki et al. (2023) introduce an additional issue that is particularly
important for Japanese text: domain adaptation involves not only
financial terminology but also language-specific tokenization and
representation. Their results show that Japanese financial-domain
pre-training and tokenizer adaptation can improve downstream NLP
performance, providing a direct reason not to assume that
English-language financial models transfer seamlessly to Japanese
disclosures.

An important methodological distinction emerges from this literature.
Li, Araci, and Suzuki primarily construct models using linguistic or
manually assigned labels, whereas Frankel and Siano use realized market
outcomes in model training. A model trained directly on returns may
achieve strong market fit, but this is conceptually different from
constructing a sentiment measure independently and subsequently
evaluating its economic informativeness. The distinction is central to
comparisons of alternative sentiment technologies under a common
external-validation framework.

------------------------------------------------------------------------

3. Generative LLMs and financial reasoning

-   ☒ Fatouros et al. (2023)
-   ☒ Li et al. (2023)
-   ☒ Chiu & Hung (2025)
-   ☒ Lopez-Lira & Tang (2026)

This section considers whether generative large language models provide
an additional improvement over dictionaries, conventional machine
learning, and domain-specific transformers. Unlike many earlier
supervised approaches, generative LLMs can often be applied through
zero-shot or few-shot prompting without training directly on the target
economic outcome.

Fatouros et al. (2023) provide an early comparison between ChatGPT and
FinBERT and show that zero-shot GPT-based sentiment can outperform a
finance-specific transformer in a financial-news setting. Their results
also demonstrate that prompt design and the provision of financial
context materially affect performance. Li et al. (2023)—distinct from
Feng Li (2010)—provide a broader taxonomy of financial LLM applications,
distinguishing zero-shot and few-shot prompting, fine-tuning, tool
augmentation, and finance-specific pre-training.

Chiu & Hung (2025) extend this literature to long financial disclosures
by combining summarization with a finance-adapted LLaMA model and
comparing its sentiment signals with FinBERT, dictionary approaches, and
traditional machine-learning methods. Lopez-Lira & Tang (2026) provide a
broad direct model comparison, examining GPT-4 alongside earlier GPT
models, BERT-family models, FinBERT, Llama variants, RavenPack, and
dictionary methods. Their results suggest that greater model
sophistication can translate into stronger economic performance,
particularly when models are evaluated against immediate market
reactions and subsequent return drift.

The progression in this literature raises a more precise empirical
question than whether LLMs can perform financial sentiment analysis. The
relevant issue is whether their greater contextual and semantic capacity
produces incrementally more economically meaningful signals when models
are evaluated on the same text, using the same economic outcome, and
under a comparable empirical design.

------------------------------------------------------------------------

4. Japanese financial text and closest empirical precedents

-   ☒ Kato & Goto (2021)
-   ☐ Manabe et al.
-   ☐ Nakatsuka
-   ☒ Suzuki et al. (2023)
-   ☒ Okada et al. (2025)

The Japanese literature is important not simply because it applies NLP
to another language, but because Japanese financial disclosure presents
distinct institutional, linguistic, and tokenization challenges. It also
provides increasingly close empirical precedents for the present study.

Kato & Goto (2021) provide a particularly important antecedent. They
analyze MD&A-related text from Japanese annual securities reports
obtained through EDINET and construct tone using a Japanese translation
of the Loughran–McDonald financial sentiment dictionary. Building
explicitly on Li (2010), they do not restrict the analysis to explicitly
forward-looking sentences. Instead, they estimate the component of
full-MD&A tone unexplained by contemporaneously observable firm
characteristics and interpret the residual as a proxy for managers’
qualitative future outlook. This adjusted tone is positively associated
with subsequent operating performance. Their evidence demonstrates that
a transparent, domain-specific lexical measure applied to Japanese
regulatory disclosure can contain economically meaningful
forward-looking information.

Suzuki et al. (2023) provide the methodological foundation for
contextual modeling in Japanese finance by showing that financial-domain
pre-training and tokenizer construction matter for downstream NLP
performance. Their results demonstrate that Japanese financial NLP poses
language-specific problems that cannot necessarily be addressed by
importing English-language models or tokenization choices. Their
evaluation, however, is primarily based on supervised NLP tasks rather
than realized stock-market outcomes.

Okada et al. (2025) provide one of the closest direct comparisons to the
current study. They analyze Japanese annual securities reports and
compare a Japanese financial polarity dictionary, a domain-adapted
DeBERTaV2 model, and general-purpose generative LLMs including GPT,
Claude, and Gemini. Their market-based results demonstrate that
alternative sentiment technologies can be applied economically to
Japanese regulatory disclosures and that model rankings may differ when
evaluated against economic rather than conventional sentiment-label
criteria.

These studies substantially narrow the research gap. The unresolved
question is no longer whether Japanese annual-report text contains
economically meaningful sentiment, nor whether modern contextual and
generative methods can be applied to it. Rather, the remaining
opportunity is to determine when greater contextual sophistication adds
economic value relative to a specialized lexical benchmark and whether
that relative value depends on the informational novelty of the
underlying disclosure.

------------------------------------------------------------------------

5. Information location and forward-looking disclosure

-   ☒ Li (2010)
-   ☒ Muslu, Radhakrishnan, Subramanyam & Lim (2015)
-   ☒ Kato & Goto (2021)

A separate but closely related literature demonstrates that economically
meaningful information is not distributed uniformly throughout long
financial disclosures. This literature is important because it
distinguishes two questions that are often conflated: which portions of
a document contain potentially relevant information, and how should the
information in those portions be measured?

Li (2010) isolates explicit forward-looking statements from MD&A and
shows that their tone contains incremental information about subsequent
profitability and liquidity. The study therefore provides early evidence
that economically motivated text selection can be useful before
sentiment is measured.

Muslu et al. (2015) focus on a different property of forward-looking
disclosure: its quantity rather than its tone. Using a rule-based
procedure to identify forward-looking sentences in U.S. 10-K MD&A, they
show that firms with weaker pre-filing information environments provide
more abnormal forward-looking disclosure and that filing-period returns
subsequently incorporate future earnings information more strongly. The
disclosure appears to improve, but not fully eliminate, the initial
information deficiency. Importantly, the results are concentrated in
operations-related and relatively short-horizon forward-looking
disclosures, indicating that even within forward-looking text, different
categories can have materially different economic relevance.

Kato & Goto (2021) provide a complementary approach in the Japanese
setting. Rather than explicitly extracting FLS, they infer managers’
future outlook from the portion of full-MD&A tone that is not explained
by contemporaneous quantitative information. Thus, Li, Muslu et al., and
Kato & Goto employ different mechanisms for isolating economically
meaningful disclosure: explicit FLS classification, FLS intensity, and
residualized full-document tone, respectively.

Taken together, this literature establishes an important premise for the
present study: identifying the economically relevant component of a long
disclosure is itself a measurement problem. It also motivates a
distinction between text selection and sentiment technology. The present
study applies that distinction to a different information-location
criterion—year-over-year textual novelty—and asks whether the technology
used to measure sentiment becomes more or less valuable as the
underlying text becomes more novel.

------------------------------------------------------------------------

6. Textual change, novelty, and disclosure informativeness

-   ☒ Brown & Tucker (2011)
-   ☒ Dyer, Lang & Stice-Lawrence (2017)
-   ☒ Cohen, Malloy & Nguyen (2020)
-   ☐ Nakatsuka & Suimon (2026)

The disclosure-change literature provides a natural alternative
criterion for locating incremental information within long regulatory
filings. Sentiment measures describe the direction or tone of language;
textual novelty describes the extent to which the disclosure differs
from information previously provided by the same firm. These are
conceptually distinct and potentially complementary dimensions.

Brown & Tucker (2011) show that year-over-year changes in MD&A contain
economically meaningful information and that greater textual
modification is associated with stronger market reactions. Dyer, Lang &
Stice-Lawrence (2017) document increasing disclosure length, redundancy,
boilerplate, and stickiness, providing an important explanation for why
full-document textual measures may contain substantial repeated
material. Cohen, Malloy & Nguyen (2020) show that changes between
successive filings predict future firm outcomes and returns, consistent
with investors underreacting to information embedded in textual changes.

An important qualification follows from Muslu et al. (2015): persistent
language is not necessarily stale or uninformative language. A highly
similar sentence can still communicate economically important new
information through a changed forecast, numerical value, or expectation.
Persistence should therefore not be treated mechanically as equivalent
to boilerplate. This consideration favors continuous measures of textual
novelty, careful treatment of numerical changes, and robustness analyses
rather than relying exclusively on a binary novel/persistent partition.

The broader implication is that sentiment and novelty capture different
dimensions of disclosure. Sentiment describes the direction of
information; novelty describes its degree of change relative to prior
disclosure. The economically relevant signal may be disproportionately
concentrated in changed portions of a filing, but the degree to which
this is true remains an empirical question. More importantly for the
present study, textual novelty may affect the relative usefulness of
alternative sentiment technologies.

------------------------------------------------------------------------

7. Research gap

The existing literature establishes several components of the proposed
mechanism separately.

First, financial language contains information associated with market
outcomes, but the quality of the resulting signal depends on how
financial language is represented. Domain-specific dictionaries improve
on general-purpose lexical measures, while supervised machine learning,
contextual transformers, and generative LLMs provide increasingly
flexible mechanisms for representing meaning.

Second, NLP benchmark performance and economic informativeness are
distinct concepts. A model may reproduce human sentiment labels
accurately without exhibiting a stronger association with realized
market outcomes. Conversely, a model trained directly on abnormal
returns may achieve strong market fit partly because the economic
outcome is embedded in the construction of the textual measure. A common
external-validation framework is therefore necessary for meaningful
comparisons of independently constructed sentiment measures.

Third, Japanese evidence now establishes that both lexical and modern
NLP approaches can extract economically meaningful signals from Japanese
financial disclosures. Kato & Goto (2021) show that dictionary-based
MD&A tone contains information about subsequent firm performance; Suzuki
et al. (2023) establish the importance of Japanese financial-domain
adaptation; and Okada et al. (2025) demonstrate that dictionaries,
contextual models, and generative LLMs can be compared economically on
Japanese annual securities reports. Consequently, the contribution
cannot rest simply on applying modern sentiment methods to Japanese
regulatory text.

Fourth, research on forward-looking disclosure and textual change
demonstrates that information is unevenly distributed within long
filings. Li (2010) and Muslu et al. (2015) show that economically
relevant information can be concentrated in particular forward-looking
components of MD&A, while Brown & Tucker (2011), Dyer et al. (2017), and
Cohen et al. (2020) establish that year-over-year modification,
redundancy, and textual change have independent economic significance.

What remains unresolved is whether these dimensions interact. In
particular, prior research does not establish whether the relative
effectiveness of lexical, contextual, and generative sentiment
technologies is conditional on the informational novelty of the text to
which they are applied. A specialized financial dictionary may capture
the economically relevant signal efficiently when language is
persistent, standardized, or lexically explicit. Greater contextual
capacity may become more valuable when firms introduce genuinely new,
substantially modified, or linguistically complex information.
Alternatively, domain-specific lexical methods may remain superior even
within highly novel text, implying that contextual sophistication adds
economically irrelevant variation rather than useful signal.

The resulting research gap is therefore not simply whether increasingly
sophisticated NLP methods outperform dictionaries on Japanese financial
text. It is when contextual sophistication adds value. The central
empirical question is whether the relative ranking of a specialized
financial dictionary, a Japanese financial transformer, and a generative
LLM changes systematically with the novelty of the underlying disclosure
when all methods are applied to the same Japanese regulatory filings and
evaluated under a common market-based framework.

A complementary question concerns sentiment innovation. Textual novelty
asks whether the firm says something new; year-over-year change in
sentiment asks whether the direction or tone of the firm’s message
changes. These dimensions need not coincide. Examining both permits the
study to distinguish linguistic innovation from changes in managerial
tone and to test whether changes in sentiment contain incremental
information beyond the contemporaneous sentiment level, particularly
when they occur in conjunction with substantial textual change.

------------------------------------------------------------------------

8. Overall story arc

-   Section 1 — Establish the measurement problem. Financial text
    contains economically meaningful information, but the choice of
    measurement technology matters. Tetlock establishes the economic
    relevance of tone; Loughran & McDonald and Henry & Leone demonstrate
    the importance of domain-specific measurement; Li shows that
    supervised learning and targeted text selection can extract
    forward-looking information; and Glasserman & Mamaysky indicate that
    sentiment may interact with textual unusualness.

-   Section 2 — Introduce contextual modeling as a response to lexical
    limitations. The literature progresses from early supervised machine
    learning to FinBERT and other domain-adapted transformers.
    Contextual models address the inability of dictionaries to condition
    meaning on surrounding language, while Japanese evidence from Suzuki
    shows that language-specific domain adaptation and tokenization
    create additional methodological considerations.

-   Section 3 — Ask whether generative LLMs represent a further
    improvement. Generative models offer broader contextual reasoning
    and can often be applied without training directly on the target
    outcome. The empirical question becomes whether this additional
    sophistication produces a stronger economic signal rather than
    merely better linguistic performance.

-   Section 4 — Bring the methodological debate into the Japanese
    setting. Kato & Goto establish that transparent dictionary-based
    tone measures contain economically meaningful information in
    Japanese annual securities reports. Suzuki demonstrates the value of
    Japanese financial-domain adaptation, while Okada shows that
    dictionaries, contextual models, and generative LLMs can all be
    applied to Japanese regulatory disclosures. These studies make Japan
    a substantive empirical setting rather than, by itself, the research
    gap.

-   Section 5 — Establish that information location matters. Li, Muslu
    et al., and Kato & Goto use different approaches to isolate
    forward-looking or economically meaningful components of MD&A. Their
    evidence demonstrates that a long disclosure should not necessarily
    be treated as a homogeneous textual object and separates the problem
    of locating information from the problem of measuring its sentiment.

-   Section 6 — Introduce textual novelty as the information-location
    mechanism. Brown & Tucker, Dyer et al., and Cohen et al. show that
    year-over-year changes, redundancy, and disclosure stickiness have
    economic significance. Novelty therefore provides a natural way to
    identify potentially incremental information, while Muslu et
    al. caution that persistence itself should not be equated
    mechanically with irrelevance.

-   Section 7 — Arrive at the research question. Prior research
    separately establishes that sentiment technology matters and that
    information location matters. The unresolved question is whether
    they interact: does the relative economic value of contextual NLP
    increase as disclosure becomes more novel? The paper therefore
    shifts the comparison from “Which sentiment model is best?” to the
    more theoretically informative question, “When does contextual NLP
    add value?”
