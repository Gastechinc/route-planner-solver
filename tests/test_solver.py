"""End-to-end smoke test against the mock travel matrix (no Mapbox needed).

Confirms the FastAPI request → optimiser → response pipeline produces a
sensible plan for a small fixture.
"""
from __future__ import annotations

import os

# Set required env BEFORE importing main so the auth check doesn't reject us
os.environ.setdefault("SOLVER_TOKEN", "test-token")
# leave MAPBOX_TOKEN unset so we go through the mock-matrix path

from fastapi.testclient import TestClient

from solver.main import app

client = TestClient(app)
HEADERS = {"X-Auth-Token": "test-token"}

# Two engineers + four central-London jobs. Postcodes chosen to be easy
# wins for postcodes.io (well-known sites) so the test isn't brittle
# against postcode-database lag for new releases.
SAMPLE_PAYLOAD = {
    "target_date": "2026-04-28",
    "engineers": [
        {
            "name": "Carl Wellington",
            "home_postcode": "EN5 1AA",  # Barnet (north)
            "work_start": "08:00",
            "work_end": "16:00",
            "vehicle_reg": "RE68UOT",
            "availability": "AVAILABLE",
        },
        {
            "name": "Gavin Daley Bovell",
            "home_postcode": "TW3 1QQ",  # Hounslow (west)
            "work_start": "08:00",
            "work_end": "16:00",
            "vehicle_reg": "RK71OUM",
            "availability": "AVAILABLE",
        },
    ],
    "jobs": [
        {
            "call_number": "30910",
            "site_name": "Harrow District Masonic Centre",
            "postcode": "HA3 0EL",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [],
        },
        {
            "call_number": "30911",
            "site_name": "Camberley Woods Care Home",
            "postcode": "GU17 9HS",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [],
        },
        {
            "call_number": "30912",
            "site_name": "Premier Inn — Bracknell",
            "postcode": "RG42 1NA",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [],
        },
        {
            "call_number": "30915",
            "site_name": "Flight Club Darts, Bloomsbury",
            "postcode": "WC1A 1BS",
            "earliest_access": "09:00",
            "duration_minutes": 60,
            "required_parts": [
                {"code": "87.01.770S", "quantity": 1},
            ],
        },
    ],
    "stock": {
        "by_location": {
            "RE68UOT": {"87.01.770S": 1},  # Carl has the Bloomsbury part
            "RK71OUM": {},  # Gavin doesn't
        }
    },
    "billing_only_codes": ["Parking", "Congestion Charge"],
    "apply_parking": True,
    "time_dependent": False,  # mock matrix doesn't need refinement
}


def test_root_health() -> None:
    r = client.get("/")
    assert r.status_code == 200
    assert r.json()["service"] == "route-planner-solver"


def test_healthz() -> None:
    r = client.get("/healthz")
    assert r.status_code == 200


def test_optimise_requires_auth() -> None:
    r = client.post("/optimise", json=SAMPLE_PAYLOAD)
    assert r.status_code == 401


def test_optimise_returns_routes() -> None:
    r = client.post("/optimise", json=SAMPLE_PAYLOAD, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["ok"] is True
    assert len(data["routes"]) == 2  # one per engineer

    total_stops = sum(len(rt["stops"]) for rt in data["routes"])
    assert total_stops + len(data["unassigned"]) == 4

    # The Bloomsbury job has a parts hard-restriction — only Carl has the
    # part in stock, so it MUST be on his route (or unassigned). Never Gavin.
    for rt in data["routes"]:
        for stop in rt["stops"]:
            if stop["call_number"] == "30915":
                assert rt["vehicle_reg"] == "RE68UOT", (
                    f"Bloomsbury job must be assigned to Carl (RE68UOT), "
                    f"got {rt['vehicle_reg']}"
                )

    # Solver should always produce at least one route with stops, even if
    # one engineer ends up empty (workload balance pushes against this).
    assert any(rt["stops"] for rt in data["routes"])


def test_optimise_rejects_no_engineers() -> None:
    bad = dict(SAMPLE_PAYLOAD)
    bad["engineers"] = [
        dict(SAMPLE_PAYLOAD["engineers"][0], availability="ANNUAL_LEAVE"),
    ]
    r = client.post("/optimise", json=bad, headers=HEADERS)
    assert r.status_code == 422
    assert "available" in r.json()["error"].lower()
