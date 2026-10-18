# V2 clarification — substantive disclosure versus boilerplate

Apply these rules before comparing current and prior disclosures:

- Classify generic introductory statements, document-navigation language,
  section headings, cross-references, filing disclaimers, and generic
  accounting caveats without company-specific economic information as
  `non_substantive`, even when repeated verbatim in the prior report.
- Reserve `unchanged` for substantive business, financial, accounting-policy,
  risk, operational, or strategic information that has not factually changed.
- Do NOT exclude company-specific accounting assumptions, measurement
  definitions, risk exposures, forecasts, or financial disclosures merely
  because they use standard wording.
- Preserve the numerical-change rule: ANY changed reported numerical value,
  rate, amount, direction, forecast, driver, or substantive factual claim
  about an existing topic is `revised`, including routine annual updates
  and an increase becoming a decrease.
- Both `revised` and `new_candidate` belong to the downstream `changed`
  sentiment group. This clarification does not alter the five-label schema.

---

# Paper 2 — Stage 6E, five-way disclosure-change classifier (v1)
# NEW EXPERIMENT. Not the frozen persistent/novel production prompt.

You classify EVERY indexed CURRENT-YEAR Japanese MD&A sentence relative to
its PREVIOUS-YEAR MD&A. Classify factual disclosure change, NOT economic
materiality, sentiment, market surprise, or information available to investors.

OUTPUT LABELS (exactly one per current sentence)

unchanged:
  A substantive factual claim is substantially the same as a counterpart
  in the previous report. Paraphrasing, changed sentence boundaries, and
  harmless formatting do not change the label. A repeated substantive
  description of a segment, business model, or risk can be unchanged.

revised:
  A comparable subject or disclosure exists in the previous report, but
  the current sentence changes one or more substantive facts: reported
  numerical values, amounts, rates, quantities, directions, status, drivers,
  forecasts, policies, scope, or specific strategic initiatives. ANY changed
  reported number is revised, even when the template and trend are identical.
  A different initiative within a previously discussed strategic response
  is revised, not automatically new_candidate. Do NOT decide materiality.

new_candidate:
  A substantive factual claim in the current report has no defensible
  corresponding disclosure in the COMPLETE previous-year MD&A. It is new
  relative to the prior ANNUAL REPORT, not necessarily new to investors.
  Do not infer this label from poor retrieval matches alone; inspect the
  rest of the previous document before deciding.

non_substantive:
  A heading, section label, navigational cross-reference, isolated date,
  or fragment with no standalone substantive economic or accounting claim.
  Do not discard meaningful boilerplate, business descriptions, accounting
  policy changes, or other substantive disclosures just because they repeat.

uncertain:
  The text is ambiguous, corrupted, or genuinely insufficient to make a
  defensible distinction after considering the supplied full documents.
  Use sparingly, and explain what prevents a decision.

INPUT
  1. COMPLETE indexed prior-year MD&A sentences P0, P1, ...
  2. COMPLETE indexed current-year MD&A sentences C0, C1, ...
  3. Three suggested prior-year retrieval indices for each current C-index.

DECISION PROCEDURE
  - Classify non-substantive text first.
  - Start with the retrieved P-indices, then search the rest of the previous
    MD&A when needed. Retrieval is a hint, NOT an exhaustive novelty test.
  - If the substantive claim has an equivalent previous counterpart and no
    factual change, label unchanged.
  - If a comparable previous disclosure exists but facts differ, label revised.
  - If no defensible previous counterpart exists anywhere in the previous
    report, label new_candidate.
  - If a sentence contains multiple claims, use revised when at least one
    substantive claim changes against an existing comparable disclosure; use
    new_candidate when its central substantive claim is absent from the prior
    report. Explain mixed cases briefly.
  - A reported value changed from 5% to 12% -> revised, NOT unchanged.
  - Repeated description of the same single business segment -> unchanged.
  - A repeated cash-flow section title -> non_substantive.
  - A new set of actions addressing an already-discussed recruitment downturn
    -> revised when the prior document discusses that strategic response.
  - Do not use retrieval scores, prior classifier labels, model predictions,
    financial returns, or judgments about economic importance.
  - Make an independent decision for EVERY current C-index; do not copy labels
    from adjacent sentences.

OUTPUT
Return ONLY valid JSON, with exactly one classification for EVERY C-index,
without missing, duplicate, or extra indices:
{
  "classifications": [
    {
      "current_index": 0,
      "label": "unchanged",
      "confidence": "high",
      "prior_index_used": 12,
      "reason": "Same single-segment disclosure as P12."
    }
  ]
}
Allowed labels: unchanged, revised, new_candidate, non_substantive, uncertain.
Allowed confidence: high, mid, low.
prior_index_used: integer P-index of the best comparable prior sentence, or null.
Use short English reasons (one sentence). Return no additional fields.
