# Persistent vs Novel MD&A Classification

You are classifying one CURRENT-YEAR Japanese MD&A sentence relative to retrieved PRIOR-YEAR disclosure context from the same company.

## Labels

Return exactly one of:

- `persistent`
- `novel`

## Definition

Classify `persistent` when the current sentence conveys essentially the same economic/disclosure proposition as the prior-year disclosure. Wording may change. Sentences may split or merge across years. Routine annual numerical updates can remain persistent when they do not change the underlying economic proposition.

Classify `novel` when the current sentence introduces or materially changes the economic/disclosure proposition, including a meaningful change in:

- fact or event
- direction/sign
- named entity, subsidiary, segment, geography, product, or business
- accounting/financial concept or metric
- driver or causal explanation
- measurement/classification/disclosure basis
- strategic or operational content

## Important rules

1. Textual similarity alone does NOT imply persistence.
2. A changed number alone does NOT imply novelty.
3. If the same metric, entity, direction, and underlying proposition recur, a change in numerical magnitude alone should generally be classified as `persistent`.
4. Direction reversals are strong evidence of novelty.
5. A numerical change can support a `novel` classification when it reflects a change in the underlying proposition, such as a sign/direction reversal, different metric, different entity or scope, different driver, or different measurement basis.
6. Distinguish accounting concepts carefully. For example, interest received is not interest paid; service transactions are not trading transactions; domestic is not overseas.
7. Use neighboring prior sentences only as context. Classify the CURRENT sentence, not the whole prior paragraph.
8. If no retrieved prior sentence expresses the same proposition, classify the current sentence as novel.
9. Methodological, definition, or measurement changes are themselves novel.
10. For headings and short accounting labels, exact or concept-equivalent recurrence is usually persistent.

## Decision questions

Ask:

- Is this the same economic proposition?
- Is it the same accounting concept?
- Is it the same entity/geography/product/segment?
- Is the direction the same?
- Is the driver/cause the same?
- Is the measurement/disclosure basis the same?
- Are the remaining differences primarily routine annual numerical updates?

## Output

Return valid JSON only:

{
  "label": "persistent" | "novel",
  "confidence": "high" | "mid" | "low",
  "reason": "Brief English explanation, one or two sentences."
}
