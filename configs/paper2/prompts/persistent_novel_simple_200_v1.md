# Paper 2 — Stage 6E simplified 200-sentence pilot (v1)
# EXPERIMENTAL ONLY. NOT THE PRODUCTION STAGE 6E PROMPT.

You classify ONE CURRENT-YEAR Japanese MD&A sentence against up to three
retrieved PRIOR-YEAR sentences from the same firm.

Your job is to identify WHAT CHANGED, not whether the change is economically
important or whether markets already knew the information.

Return exactly one label:

- unchanged: The current sentence communicates essentially the same substantive
  factual claim as a prior sentence. Ignore purely stylistic differences.
  A repeated business description may be unchanged.
- revised: The same underlying subject or claim appears in the prior disclosure,
  but its substantive factual content differs. ANY changed reported numerical
  value, rate, amount, direction, forecast, driver, event status, scope, or
  substantive claim counts as revised, regardless of its economic materiality.
  Do NOT classify routine annual numerical updates as unchanged.
- new_candidate: The current sentence contains a substantive claim for which
  no corresponding claim appears in the supplied prior candidates. This is a
  CANDIDATE for new disclosure, not proof it is absent from the entire prior
  report. Do not label a same-topic numerical update new_candidate.
- non_substantive: A heading, isolated reporting date, bare section label,
  cross-reference, or fragment with no standalone economic claim. Do not put
  substantive changes in accounting definitions or formulas here.
- uncertain: The sentence is too ambiguous, or the retrieved prior candidates
  are too incomplete, to distinguish unchanged/revised/new_candidate reliably.

Decision order:
1. Is the current sentence non_substantive?
2. Is a comparable substantive claim present in any supplied prior candidate?
   If yes, label unchanged if its substantive content is the same, otherwise revised.
3. If no comparable claim is supplied, label new_candidate when the sentence
   is a clear standalone claim; otherwise uncertain.
4. Do not use cosine scores, original Stage 6E labels, stock prices, external
   knowledge, or judgments of economic importance.
5. For claims that refer to the reporting period, a different reported figure
   counts as revised even if the sentence template is identical.
6. Prior candidates may contain duplicates. Do not interpret duplicates as
   independent corroboration.
7. These are retrieved candidates, NOT the complete previous report. A
   new_candidate classification requires full-report verification later.

Return ONLY valid JSON with these keys:
{
  "label": "unchanged | revised | new_candidate | non_substantive | uncertain",
  "confidence": "high | mid | low",
  "prior_index_used": null,
  "reason": "One short English sentence explaining the comparison."
}

Set prior_index_used to the numeric P-index of the best prior comparison, if
one exists (for example 88, not "P88"), otherwise null. No markdown or extra keys.
