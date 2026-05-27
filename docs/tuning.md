# Detector Tuning Workflow

Real detection tuning must happen on **production-shaped data** — heavy tails, gaps, diurnal cycles, and week-on-week drift. Detectors that look perfect on synthetic fixtures often false-positive on real Sundays.

`promanomaly backtest` answers the only question that matters:  
**“How does this detector actually behave on my history?”**

## Two Modes

| Mode                  | What it answers                                      | Best used for |
|-----------------------|------------------------------------------------------|---------------|
| `backtest` (default)  | How often does it fire on unmodified production data? | Checking real-world false-positive rate |
| `backtest --inject`   | What are the precision, recall, and F1 on synthetic anomalies? | Pre-merge validation of detector parameters |

## Quickstart

```bash
# Real-data backtest for a specific group
promanomaly backtest --config config.yaml --group cpu_utilisation

# Full injection test (recommended before changing params)
promanomaly backtest --config config.yaml --group cpu_utilisation --inject --output markdown
```

Or via make (dev setup):

```bash
make backtest GROUP=cpu_utilisation
make backtest-inject GROUP=cpu_utilisation
```

## Output

- **JSON** (default) — full metrics including TP/FP/FN, precision/recall/F1, and per-anomaly-type breakdown. Ideal for CI gates.
- **Markdown** — clean tables ready to paste into PRs or runbooks.

## Pre-Deploy Checklist

1. Branch your tuning changes.
2. Run the inject backtest on a representative group.
3. Confirm F1 score is at least as good as on `main`.
4. Run plain `backtest` to verify real-data fire rate stays acceptable.
5. Paste the Markdown output into your PR description.

## Limitations

- Discovery-templated queries are currently skipped.
- Synthetic injection gives a **floor**, not a ceiling, of detector quality. Real-world observation is still required.
- Each window runs 4 detector passes (1 clean + 3 injected patterns). Use `--max-windows` if you want to limit runtime.

This workflow gives you objective, reproducible numbers before you ship any detector change.