# Persistent vs Novel MD&A Classification

Classify the CURRENT-YEAR Japanese MD&A sentence relative to retrieved PRIOR-YEAR sentences from the same company.

Return exactly one label: `persistent` or `novel`.

## Definitions

`persistent`: the current sentence conveys essentially the same economic/disclosure proposition as the prior-year disclosure. Wording may change, sentences may split/merge, and routine annual numerical updates can remain persistent if the economic interpretation is unchanged.

`novel`: the current sentence introduces or materially changes the proposition, including a meaningful change in fact/event, direction/sign, economically important magnitude, entity/subsidiary/segment/geography/product, financial concept or metric, causal driver, strategic/operational content, or measurement/disclosure basis.

## Rules

1. Textual similarity alone does not imply persistence.
2. A changed number alone does not automatically imply novelty.
3. Direction reversals are strong evidence of novelty.
4. A major change in the sentence's central metric can be novel even when the template is nearly identical.
5. Routine stock-level annual updates can remain persistent.
6. Distinguish accounting concepts carefully: interest received != interest paid; service transactions != trading transactions; domestic != overseas.
7. If none of the retrieved prior sentences expresses the same proposition, classify the current sentence as novel.
8. Methodological, definition, or measurement changes are novel.
9. For headings and short accounting labels, exact or concept-equivalent recurrence is usually persistent.

## Output

Return valid JSON only:

{
  "label": "persistent" | "novel",
  "confidence": "high" | "mid" | "low",
  "reason": "Brief English explanation."
}
