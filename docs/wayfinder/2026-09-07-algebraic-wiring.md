# Wayfinder map — Algebraic state wiring (2026-09-07)

**Status:** In progress — one PR (Tickets 1-3 land together).
**Driver:** ADR-0022 map carry-item — "Algebraic partial-state wiring (Welford-merge columns per the AC) + stddev/variance n<2 NULL-parity end-to-end + Median posture". Ticket 4 shipped the `AlgebraicStateNotWired` refusal; this unblocks it.

---

## Destination

`Avg`, `StddevSample`, `StddevPopulation`, `VarianceSample`, `VariancePopulation` queries route through rollups when the rollup stores the needed partial states, with engine-parity NULL guards (stddev_samp n<2 → NULL; n=1 single-observation group edge in the regression set).

## Design

### State columns (per input field F)

| Function | State columns |
|---|---|
| Avg | `count__F` (BigInt, nn), `sum__F` (Double, nullable) |
| StddevSample | `count__F` (BigInt, nn), `sum__F` (Double, nullable), `sumsq__F` (Double, nullable) |
| StddevPopulation | same as StddevSample |
| VarianceSample / Population | same as StddevSample |

### Re-aggregation expressions (at the rewriter)

For Avg: `Sum(sum__F) / Sum(count__F)` — safe (denominator is a non-null Long, never zero for a real group).

For StddevSample: `sqrt((Sum(sumsq__F) - Sum(sum__F)² / Sum(count__F)) / (Sum(count__F) - 1))` guarded by `CASE WHEN Sum(count__F) < 2 THEN NULL END`.

For StddevPopulation: same numerator, divide by `Sum(count__F)` (not `Sum(count__F) - 1`), guarded by `CASE WHEN Sum(count__F) <= 0 THEN NULL END`.

For Variance: same as Stddev without the `sqrt`.

### Materializer side

The connector materializer extends `stateColumns` to emit these state columns for Algebraic measures (`avg_amount` → `count__amount + sum__amount`; `stddev_amount` → `count__amount + sum__amount + sumsq__amount`).

### RollupSpec side

`RollupSpec.measures` already declares Algebraic measures by name — no schema change needed. The rewriter now checks the state-column presence and refuses only if the rollup does NOT have the state (which can't happen if the materializer emitted it).

## Tickets

### Ticket #1 — materializer state columns
Extend the connector materializer to write the Algebraic state columns. Welford-merge preference: use `(n, mean, M2)` per the wayfinder AC to avoid catastrophic cancellation for large-mean data — but for v1 simplicity, use `(n, sum, sumSq)` with the merge formula `mean = sum / n; M2 = sumsq - sum²/n`; note the cancellation risk in the runbook.

### Ticket #2 — rewriter re-aggregation + NULL guards
Flip the Algebraic refusal to recognize the state columns and emit the re-aggregation expression with NULL guards. Pin the guard builders at the Expr level (they already exist from Ticket 4).

### Ticket #3 — end-to-end parity regression
Extend the materializer spec to test Avg/Stddev/Variance end-to-end (rollup-path == base-path on integral data + the n=1 / n<2 NULL-parity edge cases). Include the Welford-merge preference note.
