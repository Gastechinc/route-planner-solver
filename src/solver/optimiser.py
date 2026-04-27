"""High-level optimisation orchestration: jobs+engineers in, schedule out.

Ported from the desktop app — pipeline preserved verbatim, only import paths
adjusted and HTML in warning messages stripped (web frontend renders them
plainly).
"""
from __future__ import annotations

from datetime import datetime

from solver.geocoding import GeocodeError, geocode_postcodes
from solver.models import Availability, Engineer, Job, SolveResult
from solver.solver import solve_vrptw, time_to_minutes
from solver.stock import StockSnapshot
from solver.travel import (
    MapboxError,
    apply_parking_buffer,
    mapbox_travel_matrix,
    mock_travel_matrix,
    refine_matrix_with_depart_at,
)


class OptimiseError(Exception):
    """Wraps any failure in the geocode → matrix → solve pipeline."""


def _rebuild_used_arcs(
    result: SolveResult,
    postcodes: list[str],
    n_engineers: int,
) -> list[tuple[int, int, int]]:
    """Traverse each engineer's route and produce (from_node, to_node, depart_min)."""
    arcs: list[tuple[int, int, int]] = []

    pc_to_idx: dict[str, int] = {}
    for idx, pc in enumerate(postcodes):
        pc_to_idx.setdefault(pc, idx)

    for v, route in enumerate(result.routes):
        if not route.stops:
            continue
        eng_node = v
        prev_node = eng_node
        prev_depart = time_to_minutes(route.engineer.work_start)
        for stop in route.stops:
            dest_node = pc_to_idx.get(stop.job.postcode)
            if dest_node is None or dest_node < n_engineers:
                continue
            arcs.append((prev_node, dest_node, prev_depart))
            prev_node = dest_node
            prev_depart = stop.departure_minute
        arcs.append((prev_node, eng_node, prev_depart))

    return arcs


def optimise(
    engineers: list[Engineer],
    jobs: list[Job],
    mapbox_token: str | None = None,
    stock: StockSnapshot | None = None,
    apply_parking: bool = True,
    time_dependent: bool = True,
    target_date: datetime | None = None,
    billing_only_codes: list[str] | None = None,
) -> SolveResult:
    """Run the full pipeline and return per-engineer routes.

    With a Mapbox token and `time_dependent=True`, performs a two-pass solve:
    initial live-now matrix → solve → re-query each used arc with its
    predicted departure time → re-solve. Schedule reflects traffic profile
    for each leg's actual time of day on the target date.
    """
    if target_date is None:
        target_date = datetime.now()

    unavailable_names = [
        e.name for e in engineers if e.availability != Availability.AVAILABLE
    ]
    active_engineers = [
        e for e in engineers if e.availability == Availability.AVAILABLE
    ]

    if not active_engineers:
        raise OptimiseError(
            "No engineers are available today. Mark at least one Available."
        )
    if not jobs:
        raise OptimiseError("Load some jobs first.")

    warnings: list[str] = []
    if unavailable_names:
        warnings.append(
            f"Skipped {len(unavailable_names)} engineer(s) marked off/annual leave: "
            f"{', '.join(unavailable_names)}."
        )

    postcodes = [e.home_postcode for e in active_engineers] + [j.postcode for j in jobs]
    n_engineers = len(active_engineers)

    try:
        geo = geocode_postcodes(postcodes)
    except GeocodeError as exc:
        raise OptimiseError(f"Couldn't reach the geocoding service: {exc}") from exc

    missing = [g.postcode for g in geo if not g.found]
    if missing:
        raise OptimiseError(
            "Couldn't find these postcodes (check spelling):\n  • "
            + "\n  • ".join(missing)
        )

    approx = [g.postcode for g in geo if g.approximate]
    if approx:
        warnings.append(
            f"Used approximate location (outward postcode only) for "
            f"{len(approx)} postcode(s): {', '.join(approx)}. Travel times "
            "for those stops may be off by a few minutes."
        )

    coords = [(g.lat, g.lng) for g in geo]
    using_mapbox = False

    if mapbox_token:
        try:
            matrix = mapbox_travel_matrix(coords, mapbox_token, profile="driving-traffic")
            using_mapbox = True
        except MapboxError as exc:
            warnings.append(
                f"Mapbox unavailable ({exc}); fell back to estimated travel times."
            )
            matrix = mock_travel_matrix(coords)
    else:
        matrix = mock_travel_matrix(coords)
        warnings.append(
            "Travel times are estimated (40 km/h average). "
            "Configure Mapbox for live-traffic data."
        )

    parking_affected_set: set[int] = set()
    if apply_parking:
        matrix, parking_affected = apply_parking_buffer(matrix, postcodes, n_engineers)
        parking_affected_set = set(parking_affected)
        if parking_affected:
            job_names_affected = [
                jobs[i - n_engineers].site_name
                for i in parking_affected if 0 <= (i - n_engineers) < len(jobs)
            ]
            warnings.append(
                f"Added 15-min parking buffer for {len(parking_affected)} central-London "
                f"job(s): {', '.join(job_names_affected[:5])}"
                f"{'…' if len(job_names_affected) > 5 else ''}"
            )

    if stock is None:
        needs_parts = [j for j in jobs if j.required_parts]
        if needs_parts:
            warnings.append(
                f"{len(needs_parts)} job(s) have required parts but no stock data was "
                "provided — parts constraint not applied."
            )

    # ---- Pass 1: initial solve with live-now (+ parking) matrix ----
    result = solve_vrptw(
        active_engineers, jobs, matrix,
        stock=stock,
        billing_only_codes=billing_only_codes,
    )

    # ---- Pass 2: refine with per-arc depart_at, then re-solve ----
    if (
        using_mapbox
        and time_dependent
        and result.routes
        and any(r.stops for r in result.routes)
    ):
        try:
            used_arcs = _rebuild_used_arcs(result, postcodes, n_engineers)
            if used_arcs:
                refined_matrix = refine_matrix_with_depart_at(
                    matrix,
                    coords,
                    used_arcs,
                    mapbox_token,
                    day_date=target_date,
                    parking_affected_nodes=parking_affected_set,
                )
                final_result = solve_vrptw(
                    active_engineers, jobs, refined_matrix,
                    stock=stock,
                    billing_only_codes=billing_only_codes,
                )
                result = final_result
                warnings.append(
                    f"Travel times use traffic forecast for "
                    f"{target_date.strftime('%A %d %B')} at each leg's actual "
                    f"departure time ({len(used_arcs)} arcs refined via Mapbox)."
                )
            else:
                warnings.append("Travel times reflect live Mapbox traffic data (current snapshot).")
        except Exception as exc:  # noqa: BLE001
            warnings.append(
                f"Traffic-forecast refinement failed ({exc}); using live-now snapshot."
            )
    elif using_mapbox:
        warnings.append("Travel times reflect live Mapbox traffic data (current snapshot).")

    result.warnings.extend(warnings)
    result.geocoded = {pc: (g.lat, g.lng) for pc, g in zip(postcodes, geo)}
    return result
