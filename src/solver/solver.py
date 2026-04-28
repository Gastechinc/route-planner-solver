"""OR-Tools VRPTW solver. Ported verbatim from the desktop app — the
algorithm and tuning are battle-tested. Only import paths and a couple
of imports were touched.

Models the daily routing problem:
  - One vehicle per (available) engineer
  - Each engineer starts and ends at their own home depot
  - Each job has a service duration and an earliest-access time window
  - Engineer's working hours bound the start of the route and the latest return
  - Minimise total drive time across all engineers
  - Allow dropping a job (with high penalty) if no feasible assignment exists
  - Parts constraint (hierarchical):
      * If ≥1 engineer's van stocks ALL of a job's required parts, the job is
        restricted to those engineers — geography picks between them.
      * Only if NO engineer's van has the full set do we fall back to a soft
        per-missing-part penalty so the job still gets scheduled to the most
        complete van (and is flagged for exchange).
  - Workload balancing: the business pays engineers whether or not there's
    work, so we actively distribute jobs across the team rather than
    consolidating. A JobCount dimension with a global span coefficient
    penalises imbalance — the solver prefers (3, 3, 3, 3) over (12, 0, 0, 0).
  - Overtime policy: overtime kicks in WEEKLY (past 40h/wk), not daily.
    Days are allowed to run short (engineers bank hours) or up to ~2h over
    contracted hours without penalty.
"""
from __future__ import annotations

from datetime import time

from ortools.constraint_solver import pywrapcp, routing_enums_pb2

from solver.models import Engineer, EngineerRoute, Job, SolveResult, Stop
from solver.stock import StockSnapshot
from solver.travel import TravelMatrix

# Minutes in a day — used as the planning horizon.
HORIZON_MIN = 24 * 60
# Cost of dropping a job (in "minutes"). Must dominate any feasible total drive
# time (max ~ engineers × work-day × 1 = a few hundred minutes).
DROP_PENALTY_MIN = 100_000
# Cost of assigning a job to an engineer whose van is missing ONE required part.
# Only applies in the fallback case (no engineer has the full set) — when any
# engineer has everything the job is hard-restricted to the ones who do.
MISSING_PART_PENALTY_MIN = 5000
# Balancing coefficient: how strongly the solver minimises the spread of
# jobs-per-engineer across the team. 200 means "handing engineer A one extra
# job costs as much as 200 extra drive minutes" — the solver will accept
# meaningful drive-time increases to keep workload roughly even.
JOB_BALANCE_SPAN_COST = 200
# Engineers may finish up to this many minutes past their contracted work_end
# without penalty — normal "bank-some-hours" behaviour.
OVERTIME_ALLOWANCE_MIN = 120
# Max search time. The problem is tiny (≤4 vehicles × ≤16 jobs).
SEARCH_TIME_SECONDS = 10
# 2PL — both engineers must arrive within this many minutes of each other.
# Generous enough to absorb traffic noise, tight enough that they're genuinely
# on-site together (not back-to-back drop-bys 25 min apart).
PAIR_ARRIVAL_TOLERANCE_MIN = 30


def time_to_minutes(t: time) -> int:
    return t.hour * 60 + t.minute


def minutes_to_time(m: int) -> time:
    m = max(0, min(m, 24 * 60 - 1))
    return time(m // 60, m % 60)


def is_billing_only(code: str, billing_only_codes: list[str] | None) -> bool:
    """Is this code a billing line item (parking, congestion, etc.)?"""
    if not billing_only_codes:
        return False
    code_norm = code.strip().lower()
    return any(code_norm == bc.strip().lower() for bc in billing_only_codes)


def _missing_parts(
    engineer: Engineer,
    job: Job,
    stock: StockSnapshot | None,
    billing_only_codes: list[str] | None = None,
) -> list[str]:
    """Stock codes the engineer's van either doesn't carry, or carries
    in a smaller quantity than the job needs. Billing-only codes skipped."""
    if not job.required_parts or stock is None:
        return []
    if not engineer.vehicle_reg:
        return [
            p.code for p in job.required_parts
            if not is_billing_only(p.code, billing_only_codes)
        ]
    van = engineer.vehicle_reg.strip().upper()
    return [
        p.code for p in job.required_parts
        if not is_billing_only(p.code, billing_only_codes)
        and stock.quantity(van, p.code) < p.quantity
    ]


def solve_vrptw(
    engineers: list[Engineer],
    jobs: list[Job],
    travel: TravelMatrix,
    stock: StockSnapshot | None = None,
    billing_only_codes: list[str] | None = None,
    pair_map: dict[int, int] | None = None,
) -> SolveResult:
    """Solve the VRPTW for the given engineers + jobs.

    `pair_map` (optional): for 2PL jobs the optimiser duplicates the job into
    primary + secondary "shadow" entries in `jobs`. The map is
    `{secondary_job_idx: primary_job_idx}` — both indices into `jobs`. The
    solver then enforces:
      • secondary and primary on different vehicles
      • |arrival(secondary) − arrival(primary)| ≤ PAIR_ARRIVAL_TOLERANCE_MIN
      • either both routed or both dropped (so no engineer turns up alone
        for a 2-engineer job)
    """
    if not engineers:
        return SolveResult(routes=[], unassigned=list(jobs))
    if not jobs:
        return SolveResult(
            routes=[EngineerRoute(engineer=e) for e in engineers],
            unassigned=[],
        )

    n_eng = len(engineers)
    n_jobs = len(jobs)
    n_nodes = n_eng + n_jobs
    if travel.n != n_nodes:
        raise ValueError(
            f"Travel matrix size {travel.n} does not match node count {n_nodes} "
            f"({n_eng} engineers + {n_jobs} jobs)"
        )

    starts = list(range(n_eng))
    ends = list(range(n_eng))

    manager = pywrapcp.RoutingIndexManager(n_nodes, n_eng, starts, ends)
    routing = pywrapcp.RoutingModel(manager)

    # Per-vehicle arc cost: base travel minutes + missing-parts penalty.
    def make_cost_cb(vehicle_idx: int):
        eng = engineers[vehicle_idx]

        def cb(from_idx: int, to_idx: int) -> int:
            f = manager.IndexToNode(from_idx)
            t = manager.IndexToNode(to_idx)
            base = travel.seconds[f][t] // 60
            if t >= n_eng:
                job = jobs[t - n_eng]
                missing = _missing_parts(eng, job, stock, billing_only_codes)
                base += len(missing) * MISSING_PART_PENALTY_MIN
            return base

        return cb

    for v in range(n_eng):
        cb_idx = routing.RegisterTransitCallback(make_cost_cb(v))
        routing.SetArcCostEvaluatorOfVehicle(cb_idx, v)

    # Time dimension uses pure travel time (no parts penalty).
    def time_transit_cb(from_idx: int, to_idx: int) -> int:
        f = manager.IndexToNode(from_idx)
        t = manager.IndexToNode(to_idx)
        service = jobs[f - n_eng].duration_minutes if f >= n_eng else 0
        return service + (travel.seconds[f][t] // 60)

    time_cb_idx = routing.RegisterTransitCallback(time_transit_cb)

    routing.AddDimension(time_cb_idx, HORIZON_MIN, HORIZON_MIN, False, "Time")
    time_dim = routing.GetDimensionOrDie("Time")

    for v, eng in enumerate(engineers):
        start_idx = routing.Start(v)
        end_idx = routing.End(v)
        ws = time_to_minutes(eng.work_start)
        we = time_to_minutes(eng.work_end)
        we_with_ot = min(we + OVERTIME_ALLOWANCE_MIN, HORIZON_MIN - 1)
        time_dim.CumulVar(start_idx).SetRange(ws, we)
        time_dim.CumulVar(end_idx).SetRange(ws, we_with_ot)

    latest_work_end = max(time_to_minutes(e.work_end) for e in engineers)
    latest_with_ot = min(latest_work_end + OVERTIME_ALLOWANCE_MIN, HORIZON_MIN - 1)
    for j_idx, job in enumerate(jobs):
        node = n_eng + j_idx
        index = manager.NodeToIndex(node)
        earliest = time_to_minutes(job.earliest_access)
        latest_arrival = max(earliest, latest_with_ot - job.duration_minutes)
        time_dim.CumulVar(index).SetRange(earliest, latest_arrival)

    # Job-count dimension — drives even workload distribution.
    jobs_cb_idx = routing.RegisterUnaryTransitCallback(
        lambda idx: 1 if manager.IndexToNode(idx) >= n_eng else 0
    )
    routing.AddDimension(jobs_cb_idx, 0, n_jobs, True, "JobCount")
    jobs_dim = routing.GetDimensionOrDie("JobCount")
    jobs_dim.SetGlobalSpanCostCoefficient(JOB_BALANCE_SPAN_COST)

    for j_idx in range(n_jobs):
        node = n_eng + j_idx
        routing.AddDisjunction([manager.NodeToIndex(node)], DROP_PENALTY_MIN)

    # Parts-aware HARD constraint (only when stock is loaded).
    if stock is not None:
        for j_idx, job in enumerate(jobs):
            if not job.required_parts:
                continue
            eligible = [
                v for v, eng in enumerate(engineers)
                if not _missing_parts(eng, job, stock, billing_only_codes)
            ]
            if eligible and len(eligible) < n_eng:
                index = manager.NodeToIndex(n_eng + j_idx)
                routing.VehicleVar(index).SetValues(list(eligible) + [-1])

    # 2PL pairing — different vehicles, arrivals synced, both-or-neither.
    # Constraints are gated on ActiveVar so dropping the pair (when no
    # feasible 2-engineer assignment exists) is still allowed; otherwise
    # the solver would refuse the whole plan instead of just shedding
    # the unrouteable 2PL.
    if pair_map:
        cp_solver = routing.solver()
        big_m = HORIZON_MIN + PAIR_ARRIVAL_TOLERANCE_MIN
        for sec_j_idx, prim_j_idx in pair_map.items():
            sec_index = manager.NodeToIndex(n_eng + sec_j_idx)
            prim_index = manager.NodeToIndex(n_eng + prim_j_idx)
            sec_veh = routing.VehicleVar(sec_index)
            prim_veh = routing.VehicleVar(prim_index)
            sec_time = time_dim.CumulVar(sec_index)
            prim_time = time_dim.CumulVar(prim_index)
            sec_active = routing.ActiveVar(sec_index)
            prim_active = routing.ActiveVar(prim_index)

            # Both-or-neither — never schedule one half of a 2PL.
            cp_solver.Add(sec_active == prim_active)
            # Different vehicle, but only enforced when both are routed.
            # vehicle_diff is a 0/1 — must be 1 whenever both active.
            # NB: the method is `IsDifferentVar` (two IntVars) — `IsDifferent`
            # without the suffix doesn't exist on the OR-Tools constraint
            # solver and would crash at solve time.
            vehicle_diff = cp_solver.IsDifferentVar(sec_veh, prim_veh)
            cp_solver.Add(vehicle_diff >= sec_active)
            # Arrival within tolerance — slacked off when the pair is dropped.
            cp_solver.Add(
                sec_time - prim_time
                <= PAIR_ARRIVAL_TOLERANCE_MIN + big_m * (1 - sec_active)
            )
            cp_solver.Add(
                prim_time - sec_time
                <= PAIR_ARRIVAL_TOLERANCE_MIN + big_m * (1 - sec_active)
            )

    params = pywrapcp.DefaultRoutingSearchParameters()
    params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    params.time_limit.seconds = SEARCH_TIME_SECONDS

    solution = routing.SolveWithParameters(params)
    if solution is None:
        return SolveResult(routes=[], unassigned=list(jobs))

    # First pass: walk each route, recording vehicle assignments so we can
    # resolve "paired_with" engineer names in a second pass below.
    routes: list[EngineerRoute] = []
    assigned_job_indices: set[int] = set()
    job_idx_to_engineer: dict[int, str] = {}
    # Per-route parallel list of job indices, lined up with route.stops, so
    # the second pass can look up the original job_idx without falling back
    # to fragile object-identity comparisons against `jobs`.
    route_stop_job_idx: list[list[int]] = []

    # Build the bidirectional partner map (primary↔secondary) for paired-name lookup.
    partner_of: dict[int, int] = {}
    if pair_map:
        for sec_j_idx, prim_j_idx in pair_map.items():
            partner_of[sec_j_idx] = prim_j_idx
            partner_of[prim_j_idx] = sec_j_idx

    for v in range(n_eng):
        eng = engineers[v]
        idx = routing.Start(v)
        prev_node = manager.IndexToNode(idx)

        route = EngineerRoute(engineer=eng)
        per_stop_job_idx: list[int] = []
        idx = solution.Value(routing.NextVar(idx))
        while not routing.IsEnd(idx):
            node = manager.IndexToNode(idx)
            arrival = solution.Value(time_dim.CumulVar(idx))
            travel_sec = travel.seconds[prev_node][node]
            job_idx = node - n_eng
            job = jobs[job_idx]
            missing = tuple(_missing_parts(eng, job, stock, billing_only_codes))
            route.stops.append(
                Stop(
                    job=job,
                    arrival_minute=arrival,
                    departure_minute=arrival + job.duration_minutes,
                    travel_seconds_from_previous=travel_sec,
                    missing_parts=missing,
                    is_pair_secondary=job.is_pair_secondary,
                )
            )
            assigned_job_indices.add(job_idx)
            job_idx_to_engineer[job_idx] = eng.name
            per_stop_job_idx.append(job_idx)
            route.total_drive_seconds += travel_sec
            route.total_service_minutes += job.duration_minutes
            prev_node = node
            idx = solution.Value(routing.NextVar(idx))

        end_node = manager.IndexToNode(idx)
        route.total_drive_seconds += travel.seconds[prev_node][end_node]
        route.return_minute = solution.Value(time_dim.CumulVar(idx))
        routes.append(route)
        route_stop_job_idx.append(per_stop_job_idx)

    # Second pass — now that every job is mapped to its engineer, fill in
    # `paired_with` names on each side of every 2PL pair.
    if partner_of:
        for route, per_stop in zip(routes, route_stop_job_idx):
            for i, (stop, job_idx) in enumerate(zip(route.stops, per_stop)):
                if job_idx not in partner_of:
                    continue
                partner_name = job_idx_to_engineer.get(partner_of[job_idx])
                if partner_name is None:
                    continue
                # Stop is frozen — replace with a new one carrying paired_with.
                route.stops[i] = Stop(
                    job=stop.job,
                    arrival_minute=stop.arrival_minute,
                    departure_minute=stop.departure_minute,
                    travel_seconds_from_previous=stop.travel_seconds_from_previous,
                    missing_parts=stop.missing_parts,
                    is_pair_secondary=stop.is_pair_secondary,
                    paired_with=partner_name,
                )

    # Drop pair secondaries from `unassigned` — the primary already
    # represents the job; surfacing the secondary too would double-count.
    unassigned = [
        jobs[i] for i in range(n_jobs)
        if i not in assigned_job_indices and not jobs[i].is_pair_secondary
    ]
    return SolveResult(routes=routes, unassigned=unassigned)
