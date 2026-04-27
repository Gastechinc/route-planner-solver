# GTI Route Planner — solver service

OR-Tools VRPTW backend for [route-planner-web](https://route-planner-web.vercel.app).
Receives jobs + engineers + stock from the Vercel frontend, geocodes via
postcodes.io, builds a travel matrix via Mapbox (with live traffic + per-arc
forecast refinement), runs the OR-Tools solver, returns per-engineer routes.

## Architecture

```
 ┌────────────────┐   POST /optimise + X-Auth-Token   ┌────────────────┐
 │ Vercel (Next)  │  ───────────────────────────────► │ Render (Docker)│
 │ route-planner- │                                   │ this repo      │
 │ web            │  ◄───────────────────────────────  │ FastAPI/uvicorn│
 └────────────────┘   { ok, routes, unassigned, … }   └───┬────────────┘
                                                          │ Mapbox + postcodes.io
                                                          ▼
```

## Endpoints

- `GET /` — service banner + Mapbox config status
- `GET /healthz` — Render health check
- `POST /optimise` — main pipeline (requires `X-Auth-Token` header)

See `src/solver/models.py` for the full `OptimiseRequest` / `OptimiseResponse`
shapes.

## Local development

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .
export SOLVER_TOKEN=local-dev
uvicorn solver.main:app --app-dir src --reload
```

Smoke tests:

```bash
pytest tests/
```

## Deployment

This repo's `render.yaml` is a Render Blueprint. After connecting to GitHub:

1. Render dashboard → New → Blueprint → pick this repo
2. Set `SOLVER_TOKEN` (shared secret with Vercel)
3. Set `MAPBOX_TOKEN` (optional — falls back to mock matrix if unset)
4. Auto-deploys on every push to `main`

Render free tier sleeps after 15 min idle (~30s wake). Bump to "starter"
($7/mo) for always-on if cold starts hurt UX.

## Solver tuning

The OR-Tools VRPTW model lives in `src/solver/solver.py`. Key constants:

| Constant | Value | What it does |
|---|---|---|
| `JOB_BALANCE_SPAN_COST` | 200 | Penalty per job of imbalance — drives even workload across engineers |
| `MISSING_PART_PENALTY_MIN` | 5,000 | Soft fallback when no engineer has the full parts set |
| `DROP_PENALTY_MIN` | 100,000 | Cost of dropping a job entirely (last-resort fallback) |
| `OVERTIME_ALLOWANCE_MIN` | 120 | Engineer may finish up to 2h past contracted hours |
| `SEARCH_TIME_SECONDS` | 10 | Solver time budget per pass |

Parts is a HARD constraint when ≥1 engineer's van has the full set —
locks the job to those engineers. Geography then picks between them.
Workload balance pushes against pile-ons. Drive time minimised last.
