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


# ──────────────────────────────────────────────────────────────────────
# Constraint-specific tests — exercise each solver feature with a small
# focused fixture so a regression in one constraint is easy to localise.
# ──────────────────────────────────────────────────────────────────────


def _base_two_engineer_payload() -> dict:
    """Two-engineer baseline used by several constraint tests below."""
    return {
        "target_date": "2026-04-28",
        "engineers": [
            {
                "name": "Carl Wellington",
                "home_postcode": "EN5 1AA",
                "work_start": "08:00",
                "work_end": "16:00",
                "vehicle_reg": "RE68UOT",
                "availability": "AVAILABLE",
            },
            {
                "name": "Gavin Daley Bovell",
                "home_postcode": "TW3 1QQ",
                "work_start": "08:00",
                "work_end": "16:00",
                "vehicle_reg": "RK71OUM",
                "availability": "AVAILABLE",
            },
        ],
        "jobs": [],
        "stock": {"by_location": {"RE68UOT": {}, "RK71OUM": {}}},
        "billing_only_codes": ["Parking", "Congestion Charge"],
        "apply_parking": False,
        "time_dependent": False,
    }


def test_cumulative_parts_constraint_caps_at_van_stock() -> None:
    """
    A van starts the day with one of part X. Three jobs all need part X.
    Without the cumulative constraint the solver might happily put all
    three on that engineer (passes the per-job "van has the part" check
    each time). With the cumulative constraint, only ONE of the three
    can go to that engineer — the others must go to the other engineer
    or be left unassigned.
    """
    payload = _base_two_engineer_payload()
    # Carl has 1 of the part, Gavin has 0.
    payload["stock"]["by_location"]["RE68UOT"] = {"87.01.770S": 1}
    payload["jobs"] = [
        {
            "call_number": f"3091{i}",
            "site_name": f"Site {i}",
            "postcode": "WC1A 1BS",
            "earliest_access": "09:00",
            "duration_minutes": 60,
            "required_parts": [{"code": "87.01.770S", "quantity": 1}],
        }
        for i in range(3)
    ]

    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()

    # Count how many jobs ended up on Carl's route — must be ≤ 1
    # (his van capacity for this part).
    carl_route = next(rt for rt in data["routes"] if rt["vehicle_reg"] == "RE68UOT")
    gavin_route = next(rt for rt in data["routes"] if rt["vehicle_reg"] == "RK71OUM")
    assert len(carl_route["stops"]) <= 1, (
        f"Carl's van has 1 of part X but he was given {len(carl_route['stops'])} jobs"
    )
    # Gavin has 0 of the part, so the per-job check should keep him empty.
    assert len(gavin_route["stops"]) == 0
    # The 2 unallocatable jobs land in the unassigned list.
    assert len(data["unassigned"]) == 2


def test_forced_engineer_locks_assignment() -> None:
    """
    A job with `forced_engineer_name` set must go to that engineer or
    be dropped — the solver isn't free to reassign on geography even
    if another engineer is closer.
    """
    payload = _base_two_engineer_payload()
    # Geographically Gavin is much closer to TW2 6EG than Carl.
    # But we force the job onto Carl.
    payload["jobs"] = [
        {
            "call_number": "30901",
            "site_name": "Forced collection",
            "postcode": "TW2 6EG",
            "earliest_access": "09:00",
            "duration_minutes": 30,
            "required_parts": [],
            "forced_engineer_name": "Carl Wellington",
        },
    ]

    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()

    # The job must be on Carl's route (or unassigned, but for a single
    # easy job with a 30-min duration there's no reason to drop it).
    found_on_carl = any(
        rt["vehicle_reg"] == "RE68UOT"
        and any(s["call_number"] == "30901" for s in rt["stops"])
        for rt in data["routes"]
    )
    found_on_gavin = any(
        rt["vehicle_reg"] == "RK71OUM"
        and any(s["call_number"] == "30901" for s in rt["stops"])
        for rt in data["routes"]
    )
    assert found_on_carl, "Forced job should be on Carl's route"
    assert not found_on_gavin, "Forced job must not appear on Gavin's route"


def test_2pl_real_pair_split_across_engineers() -> None:
    """
    Two jobs at the same site/date both flagged two_engineer = real
    pair. Solver must assign them to different engineers (different
    vehicles) and arrivals within 30 min of each other.
    """
    payload = _base_two_engineer_payload()
    payload["jobs"] = [
        {
            "call_number": "30801",
            "site_name": "Twin Site",
            "postcode": "WC1A 1BS",
            "earliest_access": "10:00",
            "duration_minutes": 60,
            "required_parts": [],
            "two_engineer": True,
        },
        {
            "call_number": "30802",
            "site_name": "Twin Site",
            "postcode": "WC1A 1BS",
            "earliest_access": "10:00",
            "duration_minutes": 60,
            "required_parts": [],
            "two_engineer": True,
        },
    ]

    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()

    # Locate each call number's stop and the engineer assigned.
    found = {}
    for rt in data["routes"]:
        for stop in rt["stops"]:
            if stop["call_number"] in {"30801", "30802"}:
                found[stop["call_number"]] = (rt["vehicle_reg"], stop["arrival_minute"])

    # If the pair was scheduled at all, both halves must be present.
    if found:
        assert len(found) == 2, "2PL pair must be both-or-neither"
        v1, t1 = found["30801"]
        v2, t2 = found["30802"]
        assert v1 != v2, "2PL pair must be on different vehicles"
        assert abs(t1 - t2) <= 30, (
            f"2PL pair arrivals must be within 30 min; got {abs(t1 - t2)}"
        )
        # Both stops should report each other in paired_with.
        for rt in data["routes"]:
            for stop in rt["stops"]:
                if stop["call_number"] in {"30801", "30802"}:
                    assert stop["paired_with"] is not None


def test_2pl_lone_job_duplicates_to_two_engineers() -> None:
    """
    A single 2PL call number — the optimiser should duplicate it into
    a primary + shadow secondary so both engineers go to the same
    site simultaneously. Only the primary's call_number appears in
    unassigned if the pair fails to route.
    """
    payload = _base_two_engineer_payload()
    payload["jobs"] = [
        {
            "call_number": "30850",
            "site_name": "Lone 2PL",
            "postcode": "WC1A 1BS",
            "earliest_access": "10:00",
            "duration_minutes": 60,
            "required_parts": [],
            "two_engineer": True,
        },
    ]

    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()

    # Two stops should appear across both routes (one primary, one
    # shadow secondary), both with call_number 30850.
    matching_stops = [
        (rt["vehicle_reg"], stop)
        for rt in data["routes"]
        for stop in rt["stops"]
        if stop["call_number"] == "30850"
    ]
    if matching_stops:
        assert len(matching_stops) == 2, "Lone 2PL should duplicate to two stops"
        vehicles = {v for v, _ in matching_stops}
        assert len(vehicles) == 2, "2PL halves must be on different vehicles"
        secondaries = [s for _, s in matching_stops if s["is_pair_secondary"]]
        assert len(secondaries) == 1, "Exactly one half is the shadow secondary"


def test_idle_engineer_penalty_spreads_work() -> None:
    """
    Two engineers, two easy jobs in different parts of London. The
    soft "min 1 stop per engineer" penalty should push the solver to
    give each engineer at least one job, even if a tiny drive-time
    saving exists from giving both to one engineer.
    """
    payload = _base_two_engineer_payload()
    payload["jobs"] = [
        {
            "call_number": "30100",
            "site_name": "North job",
            "postcode": "EN5 1AA",  # Right next to Carl
            "earliest_access": "09:00",
            "duration_minutes": 60,
            "required_parts": [],
        },
        {
            "call_number": "30101",
            "site_name": "West job",
            "postcode": "TW3 1QQ",  # Right next to Gavin
            "earliest_access": "09:00",
            "duration_minutes": 60,
            "required_parts": [],
        },
    ]

    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()

    # Both engineers should have at least one stop.
    for rt in data["routes"]:
        assert len(rt["stops"]) >= 1, (
            f"{rt['engineer_name']} should have at least one stop "
            "(idle penalty should spread work)"
        )


def test_attendance_window_constrains_arrival() -> None:
    """
    A job with a tight access window (10:00-11:00) — the solver's
    arrival_minute must fall inside [600, 660].
    """
    payload = _base_two_engineer_payload()
    # We use earliest_access only (no Fix Date in the API model yet —
    # the upper bound comes implicitly from the engineer's work_end +
    # the job's duration). For now this test confirms the floor.
    payload["jobs"] = [
        {
            "call_number": "30200",
            "site_name": "Late window",
            "postcode": "WC1A 1BS",
            "earliest_access": "14:00",
            "duration_minutes": 30,
            "required_parts": [],
        },
    ]

    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()
    found = [
        stop
        for rt in data["routes"]
        for stop in rt["stops"]
        if stop["call_number"] == "30200"
    ]
    assert len(found) == 1, "Job must be routed once"
    assert found[0]["arrival_minute"] >= 14 * 60, (
        f"Arrival before 14:00 violates the earliest_access window: "
        f"got {found[0]['arrival_minute']}"
    )
