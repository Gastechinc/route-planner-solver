"""FastAPI entry point for the GTI Route Planner solver service.

Endpoints:
  GET  /                — health/version
  POST /optimise        — run the VRPTW pipeline; protected by SOLVER_TOKEN
  GET  /healthz         — Render keepalive

Auth:
  Both endpoints require an `X-Auth-Token` header matching the
  SOLVER_TOKEN env var. Vercel's server action sets this header before
  posting; the value is set in Vercel + Render env vars (different copies
  of the same shared secret).

Mapbox:
  Token is read from MAPBOX_TOKEN env var. If unset, solver falls back to
  the haversine-based mock matrix and a warning is added to the response.
"""
from __future__ import annotations

import os
import time as _time
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from solver.models import (
    OptimiseRequest,
    OptimiseResponse,
    JobOut,
    route_to_out,
)
from solver.optimiser import OptimiseError, optimise
from solver.stock import StockSnapshot

SOLVER_TOKEN = os.environ.get("SOLVER_TOKEN", "").strip()
MAPBOX_TOKEN = os.environ.get("MAPBOX_TOKEN", "").strip() or None
SERVICE_VERSION = "0.1.0"
# Render auto-injects RENDER_GIT_COMMIT on every deploy (full 40-char SHA).
# Surface the short form so the web app's footer can show what's running
# without the office having to dig through the dashboard.
COMMIT_SHA = os.environ.get("RENDER_GIT_COMMIT", "").strip()
COMMIT_SHORT = COMMIT_SHA[:7] if COMMIT_SHA else "dev"

app = FastAPI(
    title="GTI Route Planner — solver",
    version=SERVICE_VERSION,
    description="OR-Tools VRPTW backend for route-planner-web.vercel.app",
)


def _check_auth(request: Request) -> None:
    if not SOLVER_TOKEN:
        # Fail-closed: refuse to serve if the deployment forgot the secret
        raise HTTPException(
            status_code=503,
            detail="Solver service is mis-configured (SOLVER_TOKEN env var unset).",
        )
    presented = request.headers.get("x-auth-token", "").strip()
    if not presented or presented != SOLVER_TOKEN:
        raise HTTPException(status_code=401, detail="Bad or missing X-Auth-Token")


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "route-planner-solver",
        "version": SERVICE_VERSION,
        "commit": COMMIT_SHORT,
        "mapbox": "configured" if MAPBOX_TOKEN else "missing — using mock matrix",
    }


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Keepalive endpoint Render uses to mark the service healthy."""
    return {"status": "ok"}


@app.post("/optimise", response_model=OptimiseResponse)
def optimise_endpoint(payload: OptimiseRequest, request: Request) -> OptimiseResponse:
    _check_auth(request)

    started = _time.monotonic()
    engineers = [e.to_internal() for e in payload.engineers]
    jobs = [j.to_internal() for j in payload.jobs]
    stock = StockSnapshot.from_request(payload.stock.by_location) if payload.stock else None

    try:
        result = optimise(
            engineers=engineers,
            jobs=jobs,
            mapbox_token=MAPBOX_TOKEN,
            stock=stock,
            apply_parking=payload.apply_parking,
            time_dependent=payload.time_dependent,
            target_date=payload.parsed_target_date(),
            billing_only_codes=payload.billing_only_codes,
        )
    except OptimiseError as exc:
        # Client-fault: bad input, missing postcodes, no engineers, etc.
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        # Server-fault: anything unexpected in the pipeline.
        raise HTTPException(
            status_code=500, detail=f"Solver crashed: {exc}"
        ) from exc

    elapsed_ms = int((_time.monotonic() - started) * 1000)
    return OptimiseResponse(
        ok=True,
        routes=[route_to_out(r) for r in result.routes],
        unassigned=[
            JobOut(
                call_number=j.call_number,
                site_name=j.site_name,
                postcode=j.postcode,
                reason=j.unassigned_reason,
                reason_tag=j.unassigned_reason_tag,
            )
            for j in result.unassigned
        ],
        warnings=result.warnings + [f"Solver elapsed: {elapsed_ms}ms"],
        geocoded=result.geocoded,
    )


@app.exception_handler(HTTPException)
async def _http_exc_handler(request: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"ok": False, "error": exc.detail},
    )
