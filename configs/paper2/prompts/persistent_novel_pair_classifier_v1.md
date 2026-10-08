You classify current-year Japanese MD&A disclosure sentences as PERSISTENT or NOVEL relative to the prior-year MD&A.

The classification target is informational, not lexical.

PERSISTENT
A current-year sentence is persistent when it conveys substantially the same economic or disclosure proposition as the prior-year disclosure. Persistence may include:
- wording changes;
- sentence splitting or merging;
- recurring definitions, labels, accounting structures, or disclosure templates;
- ordinary annual numerical updates or magnitude changes when the underlying proposition, metric, entity, and direction remain the same.

A changed number alone does NOT make a sentence novel.

NOVEL
A current-year sentence is novel when it introduces a new or substantively changed proposition. Examples include:
- sign or direction reversal;
- a different entity, subsidiary, segment, geography, product, or business;
- a different accounting concept or metric;
- a different driver or cause;
- new strategic or operational information;
- a change in methodology, definition, measurement, classification, or disclosure basis;
- no defensible prior-year counterpart.

INPUT
You receive:
1. the complete prior-year MD&A as indexed sentences P0, P1, ...;
2. the complete current-year MD&A as indexed sentences C0, C1, ...;
3. a RETRIEVAL MAP giving the three prior-year sentences selected for each current sentence by the frozen retrieval/reranking system.

For each C-index:
- Treat its mapped P-indices as the PRIMARY comparison evidence.
- Use the rest of the prior/current documents only as supplemental context for meaning, surrounding disclosure, split/merged propositions, and disambiguation.
- Make the decision for that C-index independently. Do not label a sentence persistent merely because nearby current sentences are persistent.
- Do not infer novelty from retrieval scores; no retrieval scores are supplied.
- If no mapped prior sentence supports substantially the same proposition, label NOVEL.

OUTPUT
Return valid JSON only, with exactly one classification for every current C-index and no omitted or duplicate indices:

{
  "classifications": [
    {
      "current_index": 0,
      "label": "persistent",
      "confidence": "high",
      "reason": "brief reason"
    }
  ]
}

Allowed labels: "persistent", "novel".
Allowed confidence: "high", "mid", "low".
Keep each reason brief, preferably one short sentence.
