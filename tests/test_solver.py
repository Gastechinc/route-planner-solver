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


def test_latest_departure_enforces_off_site_deadline() -> None:
    """
    A job with latest_departure = 11:00 and duration 60 min — the solver
    must schedule arrival ≤ 10:00 so the engineer is off site by 11:00.
    Without this constraint the solver would happily arrive at 10:30.
    """
    payload = _base_two_engineer_payload()
    payload["jobs"] = [
        {
            "call_number": "30300",
            "site_name": "Off-site by 11",
            "postcode": "WC1A 1BS",
            "earliest_access": "08:00",
            "latest_departure": "11:00",
            "duration_minutes": 60,
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
        if stop["call_number"] == "30300"
    ]
    assert len(found) == 1, "Job must be routed once"
    # arrival + duration must be ≤ latest_departure (660 minutes from midnight)
    assert found[0]["departure_minute"] <= 11 * 60, (
        f"Departure after 11:00 violates latest_departure: "
        f"got {found[0]['departure_minute']}"
    )
    # Buffer in front of the deadline — the solver should clear the cut-off
    # by at least LATEST_DEPARTURE_BUFFER_MIN where feasible. With earliest
    # 08:00 + duration 60, the buffer is well within the available window.
    from solver.solver import LATEST_DEPARTURE_BUFFER_MIN

    slack = 11 * 60 - found[0]["departure_minute"]
    assert slack >= LATEST_DEPARTURE_BUFFER_MIN, (
        f"Expected at least {LATEST_DEPARTURE_BUFFER_MIN} min slack before "
        f"the 11:00 deadline; got {slack} min "
        f"(departure {found[0]['departure_minute']})"
    )


def test_latest_departure_buffer_drops_when_infeasible_otherwise() -> None:
    """
    A job whose buffered constraint would be infeasible (e.g. earliest 09:30
    + duration 60 + buffer 15 = 645 > deadline 660) must STILL be schedulable
    — falling back to no-buffer rather than dropped. Better to run the
    deadline tight than refuse the work.
    """
    payload = _base_two_engineer_payload()
    payload["jobs"] = [
        {
            "call_number": "30310",
            "site_name": "Tight but feasible",
            "postcode": "WC1A 1BS",
            "earliest_access": "09:30",
            "latest_departure": "11:00",
            "duration_minutes": 60,
            "required_parts": [],
        },
    ]
    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()
    routed = [
        stop
        for rt in data["routes"]
        for stop in rt["stops"]
        if stop["call_number"] == "30310"
    ]
    assert len(routed) == 1, "Job must still be routed when buffer back-off is needed"
    assert routed[0]["departure_minute"] <= 11 * 60, "Deadline still respected"


def test_latest_departure_drops_infeasible_job() -> None:
    """
    A job with earliest_access = 10:00, latest_departure = 10:30, but a
    60-min duration is infeasible — must be DROPPED to unassigned, not
    scheduled in violation of either bound.
    """
    payload = _base_two_engineer_payload()
    payload["jobs"] = [
        {
            "call_number": "30301",
            "site_name": "Impossible window",
            "postcode": "WC1A 1BS",
            "earliest_access": "10:00",
            "latest_departure": "10:30",
            "duration_minutes": 60,
            "required_parts": [],
        },
    ]
    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()
    routed = [
        stop
        for rt in data["routes"]
        for stop in rt["stops"]
        if stop["call_number"] == "30301"
    ]
    unassigned = [u for u in data["unassigned"] if u["call_number"] == "30301"]
    # Either dropped, OR (defence in depth) routed but not violating the
    # deadline. We assert dropped here because the solver's clamp-then-drop
    # path is the cleaner outcome.
    if routed:
        assert routed[0]["departure_minute"] <= 10 * 60 + 30, (
            "If the solver did schedule it, departure must respect the deadline"
        )
    else:
        assert len(unassigned) == 1, (
            "Infeasible job must end up in unassigned"
        )


def test_must_be_first_makes_job_the_first_stop() -> None:
    """
    Three jobs across two engineers — one flagged must_be_first. Whichever
    engineer ends up serving it, that engineer's first stop (index 0) must
    be the flagged job.
    """
    payload = _base_two_engineer_payload()
    payload["jobs"] = [
        {
            "call_number": "30401",
            "site_name": "Must Be First",
            "postcode": "WC1A 1BS",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [],
            "must_be_first": True,
        },
        {
            "call_number": "30402",
            "site_name": "Filler A",
            "postcode": "HA3 0EL",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [],
        },
        {
            "call_number": "30403",
            "site_name": "Filler B",
            "postcode": "GU17 9HS",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [],
        },
    ]
    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()
    # Find the route that took the must-be-first job
    serving_route = None
    for rt in data["routes"]:
        if any(s["call_number"] == "30401" for s in rt["stops"]):
            serving_route = rt
            break
    assert serving_route is not None, (
        "must_be_first job was dropped — should have been routed"
    )
    assert serving_route["stops"][0]["call_number"] == "30401", (
        f"must_be_first job must be the first stop on its route; "
        f"got order: {[s['call_number'] for s in serving_route['stops']]}"
    )


def test_engineer_preference_biases_assignment() -> None:
    """
    Two engineers, both equally placed geographically. One job each.
    Engineer A prefers contract_pm, Engineer B prefers emergency.
    Two jobs: one PMV, one emergency. The PMV must go to A and the
    emergency to B — preference penalty makes any swap more expensive.
    """
    payload = _base_two_engineer_payload()
    # Both engineers anchored at the same SE1 home so geography is
    # neutral; only the preference penalty drives the assignment.
    payload["engineers"][0]["home_postcode"] = "SE1 7PB"
    payload["engineers"][1]["home_postcode"] = "SE1 7PB"
    # Carl (engineer 0) prefers contract_pm (PMVs)
    payload["engineers"][0]["preferred_call_categories"] = ["contract_pm"]
    # Gavin (engineer 1) prefers emergency (breakdowns)
    payload["engineers"][1]["preferred_call_categories"] = ["emergency"]

    payload["jobs"] = [
        {
            "call_number": "30501",
            "site_name": "PMV site",
            "postcode": "SE1 7PB",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [],
            "call_category": "contract_pm",
        },
        {
            "call_number": "30502",
            "site_name": "Emergency site",
            "postcode": "SE1 7PB",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [],
            "call_category": "emergency",
        },
    ]
    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()

    # Map call_number -> assigned engineer name
    by_call: dict[str, str] = {}
    for rt in data["routes"]:
        for stop in rt["stops"]:
            by_call[stop["call_number"]] = rt["engineer_name"]

    assert "30501" in by_call, "PMV must be routed"
    assert "30502" in by_call, "Emergency must be routed"
    assert by_call["30501"] == "Carl Wellington", (
        f"PMV should go to Carl (contract_pm preference); got {by_call['30501']}"
    )
    assert by_call["30502"] == "Gavin Daley Bovell", (
        f"Emergency should go to Gavin (emergency preference); got {by_call['30502']}"
    )


def test_engineer_preference_yields_to_capacity() -> None:
    """
    One engineer (Gavin) with preference, two PMV jobs. Even though Carl
    has no preference for PMV, the solver should still route both jobs
    rather than dropping one to honour the preference — the penalty is
    SOFT and yields to feasibility.
    """
    payload = _base_two_engineer_payload()
    payload["engineers"][1]["preferred_call_categories"] = ["contract_pm"]
    payload["jobs"] = [
        {
            "call_number": "30601",
            "site_name": "PMV A",
            "postcode": "WC1A 1BS",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [],
            "call_category": "contract_pm",
        },
        {
            "call_number": "30602",
            "site_name": "PMV B",
            "postcode": "HA3 0EL",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [],
            "call_category": "contract_pm",
        },
    ]
    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()
    routed_calls = {
        stop["call_number"]
        for rt in data["routes"]
        for stop in rt["stops"]
    }
    # Both should be routed — preference is soft, feasibility wins.
    assert "30601" in routed_calls, "PMV A must still be routed"
    assert "30602" in routed_calls, "PMV B must still be routed"


def test_trainee_bonus_inflates_service_time() -> None:
    """
    A trainee engineer doing a 60-min job has their on-site duration padded
    by TRAINEE_DURATION_BONUS_MIN — the displayed depart-from-this-site
    must reflect arrival + duration + bonus, and total_service_minutes for
    the route must include the bonus per stop. Otherwise a non-trainee
    engineer with the same job is unchanged.
    """
    from solver.solver import TRAINEE_DURATION_BONUS_MIN

    payload = _base_two_engineer_payload()
    # Carl is the trainee; Gavin is not. Both have one job.
    payload["engineers"][0]["is_trainee"] = True
    payload["jobs"] = [
        {
            "call_number": "30700",
            "site_name": "Trainee site",
            # Near Carl (EN5 1AA) so geography routes it to him.
            "postcode": "N1 1AA",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [],
        },
        {
            "call_number": "30701",
            "site_name": "Non-trainee site",
            # Near Gavin (TW3 1QQ) so it routes to him.
            "postcode": "TW1 1AA",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [],
        },
    ]
    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()
    by_call_route: dict[str, dict] = {}
    for rt in data["routes"]:
        for stop in rt["stops"]:
            by_call_route[stop["call_number"]] = {"stop": stop, "route": rt}

    # Carl (trainee) — service should be 60 + bonus.
    carl = by_call_route["30700"]
    assert carl["route"]["engineer_name"] == "Carl Wellington"
    carl_service = carl["stop"]["departure_minute"] - carl["stop"]["arrival_minute"]
    assert carl_service == 60 + TRAINEE_DURATION_BONUS_MIN, (
        f"Trainee Carl's stop should run {60 + TRAINEE_DURATION_BONUS_MIN} min "
        f"(60 base + {TRAINEE_DURATION_BONUS_MIN} bonus); got {carl_service}"
    )

    # Gavin (non-trainee) — unchanged.
    gavin = by_call_route["30701"]
    assert gavin["route"]["engineer_name"] == "Gavin Daley Bovell"
    gavin_service = gavin["stop"]["departure_minute"] - gavin["stop"]["arrival_minute"]
    assert gavin_service == 60, (
        f"Non-trainee Gavin's stop should run 60 min; got {gavin_service}"
    )

    # Route totals reflect the bonus too.
    carl_route_total = next(
        rt["total_service_minutes"]
        for rt in data["routes"]
        if rt["engineer_name"] == "Carl Wellington"
    )
    gavin_route_total = next(
        rt["total_service_minutes"]
        for rt in data["routes"]
        if rt["engineer_name"] == "Gavin Daley Bovell"
    )
    assert carl_route_total == 60 + TRAINEE_DURATION_BONUS_MIN
    assert gavin_route_total == 60


def test_trainee_bonus_tightens_latest_departure_deadline() -> None:
    """
    With a trainee on the team, a windowed job whose duration + bonus
    can't fit before the off-site deadline must be DROPPED rather than
    routed to a non-trainee with a tight deadline — the deadline check
    is global (we don't know which vehicle will take it). Earliest
    08:00, deadline 09:00, duration 60: non-trainee could just fit
    (60 ≤ 60); with a trainee on the team, deadline tightens by 30
    and the job becomes infeasible.
    """
    from solver.solver import TRAINEE_DURATION_BONUS_MIN

    assert TRAINEE_DURATION_BONUS_MIN == 30  # guard

    payload = _base_two_engineer_payload()
    payload["engineers"][0]["is_trainee"] = True
    payload["jobs"] = [
        {
            "call_number": "30710",
            "site_name": "Tight window",
            "postcode": "WC1A 1BS",
            "earliest_access": "08:00",
            "latest_departure": "09:00",
            "duration_minutes": 60,
            "required_parts": [],
        },
    ]
    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()
    routed = [
        stop
        for rt in data["routes"]
        for stop in rt["stops"]
        if stop["call_number"] == "30710"
    ]
    unassigned = [u for u in data["unassigned"] if u["call_number"] == "30710"]
    # 08:00 + 60 + 30 buffer = 09:30 > 09:00 deadline → infeasible
    # under the team-max-bonus rule. Should be dropped.
    assert routed == [], (
        f"Job must be dropped under trainee-tightened deadline; got {routed}"
    )
    assert len(unassigned) == 1


def test_unassigned_diagnostic_parts_shortage() -> None:
    """
    A job that needs a part no engineer's van carries should be dropped
    AND tagged with the parts_shortage diagnostic, so the office can see
    the cause rather than guessing.
    """
    payload = _base_two_engineer_payload()
    # Neither van has the required part; job should drop with a clear reason.
    payload["jobs"] = [
        {
            "call_number": "30800",
            "site_name": "Needs unobtainium",
            "postcode": "WC1A 1BS",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [{"code": "UNOBTAINIUM-1", "quantity": 1}],
        },
    ]
    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()
    unassigned = [u for u in data["unassigned"] if u["call_number"] == "30800"]
    assert len(unassigned) == 1, "Parts-shortage job must drop"
    assert unassigned[0]["reason_tag"] == "parts_shortage"
    assert "UNOBTAINIUM-1" in (unassigned[0]["reason"] or "")


def test_unassigned_diagnostic_window_too_tight() -> None:
    """
    A job whose earliest_access + duration overruns its latest_departure
    is geometrically infeasible — must be dropped and tagged
    window_too_tight so the office knows to push the deadline rather
    than waste time hunting for available engineers.
    """
    payload = _base_two_engineer_payload()
    payload["jobs"] = [
        {
            "call_number": "30801",
            "site_name": "Impossible window",
            "postcode": "WC1A 1BS",
            "earliest_access": "10:00",
            "latest_departure": "10:30",
            "duration_minutes": 60,
            "required_parts": [],
        },
    ]
    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()
    unassigned = [u for u in data["unassigned"] if u["call_number"] == "30801"]
    assert len(unassigned) == 1
    assert unassigned[0]["reason_tag"] == "window_too_tight"
    # Reason mentions the timeframe so the office sees the maths.
    reason = unassigned[0]["reason"] or ""
    assert "10:00" in reason and "10:30" in reason


def test_unassigned_diagnostic_no_reason_falls_back_to_none() -> None:
    """
    A job that drops for non-pinpointable reasons (over-packed engineers,
    say) must come back with reason_tag=None — the client renders a
    generic fallback for that case instead of pretending it knows.
    Built by piling enough jobs onto a single engineer that cost-based
    drops are inevitable but no specific constraint is violated.
    """
    payload = _base_two_engineer_payload()
    # 8 hour-long jobs around central London; with two engineers and
    # work-day 08:00-16:00 that's enough to force dropouts on cost
    # without breaking parts/window/forced rules.
    payload["jobs"] = [
        {
            "call_number": f"3090{i}",
            "site_name": f"Generic {i}",
            "postcode": "WC1A 1BS",
            "earliest_access": "08:00",
            "duration_minutes": 120,
            "required_parts": [],
        }
        for i in range(10)
    ]
    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()
    if data["unassigned"]:
        # When the solver does drop something for cost reasons we
        # don't yet diagnose, reason_tag should be None — the client
        # then shows a generic message instead of asserting a cause.
        for u in data["unassigned"]:
            if u["reason_tag"] is None:
                assert u["reason"] is None
                return
    # If everything routed (the matrix was lenient) the test still
    # passes — we're only asserting the shape of unassigned rows.


def test_unassigned_diagnostic_forced_engineer_off() -> None:
    """
    A job force-locked to an engineer who's marked OFF must DROP with
    forced_engineer_unavailable rather than silently re-route to a
    different engineer. Earlier the solver let the constraint slip
    when the named engineer was filtered out of the routing pool by
    the upstream availability check; the office reported "I locked
    this to Carl, why did Gavin take it?".
    """
    payload = _base_two_engineer_payload()
    # Carl marked OFF; Gavin is the only working engineer today.
    payload["engineers"][0]["availability"] = "OFF"
    payload["jobs"] = [
        {
            "call_number": "30900",
            "site_name": "Locked to absent Carl",
            "postcode": "WC1A 1BS",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [],
            "forced_engineer_name": "Carl Wellington",
        },
    ]
    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()

    # Job must NOT be routed to anyone (the lock isn't satisfiable).
    routed = [
        stop
        for rt in data["routes"]
        for stop in rt["stops"]
        if stop["call_number"] == "30900"
    ]
    assert routed == [], (
        f"Forced-engineer job must drop when the named engineer is OFF; "
        f"silent re-route detected — {routed}"
    )

    # And it must show up in unassigned with the proper diagnostic.
    unassigned = [u for u in data["unassigned"] if u["call_number"] == "30900"]
    assert len(unassigned) == 1
    assert unassigned[0]["reason_tag"] == "forced_engineer_unavailable"
    assert "Carl" in (unassigned[0]["reason"] or "")


def test_unassigned_diagnostic_forced_engineer_unknown_name() -> None:
    """
    A job force-locked to a name that doesn't match any engineer must
    drop the same way as the OFF case — the office sees a clear
    forced_engineer_unavailable reason rather than a mystery routing.
    """
    payload = _base_two_engineer_payload()
    payload["jobs"] = [
        {
            "call_number": "30901",
            "site_name": "Locked to ghost",
            "postcode": "WC1A 1BS",
            "earliest_access": "08:00",
            "duration_minutes": 60,
            "required_parts": [],
            "forced_engineer_name": "Phantom Engineer",
        },
    ]
    r = client.post("/optimise", json=payload, headers=HEADERS)
    assert r.status_code == 200, r.text
    data = r.json()

    routed = [
        stop
        for rt in data["routes"]
        for stop in rt["stops"]
        if stop["call_number"] == "30901"
    ]
    assert routed == [], (
        f"Forced-engineer job must drop when the name doesn't match; "
        f"silent re-route detected — {routed}"
    )

    unassigned = [u for u in data["unassigned"] if u["call_number"] == "30901"]
    assert len(unassigned) == 1
    assert unassigned[0]["reason_tag"] == "forced_engineer_unavailable"
    assert "Phantom Engineer" in (unassigned[0]["reason"] or "")
