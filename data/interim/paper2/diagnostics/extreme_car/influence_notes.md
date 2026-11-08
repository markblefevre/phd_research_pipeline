# Paper 2: Stage 9 influence diagnostic

This analysis is computed solely from the frozen Stage 9 regression dataset.
All model refits use identical two-way firm and fiscal-year fixed effects,
seven controls, firm-clustered CR1 standard errors, and Student-t p-values
with number of firm clusters minus one degrees of freedom.

## Replication gates
- Baseline N=22,266, firms=4,103.
- New-sentiment coefficient=0.24527446, SE=0.11328345, p=0.03043449.
- New-minus-unchanged contrast=0.26757171, p=0.15696269.
- Extreme-return group: 96 observations with |CAR|>20 pp.
- Exact leave-one-out refits: 199 observations, all 96 extreme-return filings
  and the 150 largest absolute first-order coefficient-deletion scores.
- Exact leave-one-firm-out refits: 40 firms screened using firm-grouped first-order scores.

## Findings
- Among largest 20 exactly evaluated absolute coefficient-influences, 18 have |CAR|>20 pp.
- Among largest 50 exactly evaluated absolute coefficient-influences, 31 have |CAR|>20 pp.
- Among largest 100 exactly evaluated absolute coefficient-influences, 43 have |CAR|>20 pp.
- The filing with the largest absolute CAR changes beta by +0.03650143 when retained versus removed.
- Of the 96 extreme-CAR filings, 55 individually increase beta
  and 41 individually decrease beta on net (case deletion).

## Scenario table

| Diagnostic | N | beta new | p new | contrast p |
|---|---:|---:|---:|---:|
| Original baseline | 22,266 | +0.2453 | 0.0304 | 0.1570 |
| Exclude all |CAR|>20pp | 22,170 | +0.1411 | 0.1261 | 0.3324 |
| Winsorize CAR 1/99 | 22,266 | +0.1742 | 0.0518 | 0.3809 |
| Delete largest absolute CAR | 22,265 | +0.2088 | 0.0538 | 0.2145 |
| Delete top 1 absolute coefficient-influence | 22,265 | +0.2088 | 0.0538 | 0.2145 |
| Delete top 1 positive coefficient-influence | 22,265 | +0.2088 | 0.0538 | 0.2145 |
| Delete top 5 absolute coefficient-influence | 22,261 | +0.2056 | 0.0514 | 0.2227 |
| Delete top 5 positive coefficient-influence | 22,261 | +0.1646 | 0.1165 | 0.2667 |
| Delete top 10 absolute coefficient-influence | 22,256 | +0.1955 | 0.0552 | 0.2371 |
| Delete top 10 positive coefficient-influence | 22,256 | +0.1261 | 0.2192 | 0.3612 |
| Delete top 20 absolute coefficient-influence | 22,246 | +0.1862 | 0.0563 | 0.2216 |
| Delete top 20 positive coefficient-influence | 22,246 | +0.0700 | 0.4856 | 0.5064 |
| Delete top 1 absolute firm-influence | 22,259 | +0.2138 | 0.0481 | 0.1988 |
| Delete top 5 absolute firm-influence | 22,234 | +0.1881 | 0.0692 | 0.2218 |
| Delete top 10 absolute firm-influence | 22,204 | +0.1781 | 0.0751 | 0.2520 |

## Interpretation and limitations
- The first-order scores are a screening approximation; exact results are based on
  re-estimation with the selected observation or firm removed, including re-absorption
  of fixed effects and recalculation of the clustered covariance matrix.
- The selection of highly influential observations is based on realized data and is
  **not a prespecified exclusion criterion**. Do not use these ex-post exclusions to
  claim or refute statistical significance in a preferred paper specification.
- Removing multiple observations need not equal the sum of their individual effects;
  all listed joint-deletion scenarios are separately re-estimated.
- These results assess coefficient stability, not the economic reason for price changes
  and not the validity of stock-price or adjustment-factor data.
- No LLM classification, sentiment scoring or original Stage 9 outputs were changed.

Elapsed processing time: 28.7s.
