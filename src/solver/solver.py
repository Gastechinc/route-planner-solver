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

from solver.models import (
    Availability,
    Engineer,
    EngineerRoute,
    Job,
    SolveResult,
    Stop,
)
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
# Penalty for leaving an active engineer with zero stops. The business pays
# every available engineer regardless of work; getting them on the road for
# at least one job both covers their cost and primes the team for late
# add-ons. 480 = 8h of drive — strong enough to dominate any reasonable
# concentration saving (a typical London cross-town leg is 30-60 min) but
# soft, so when geography genuinely makes a stop too costly the solver
# pays the penalty rather than refusing the plan.
IDLE_ENGINEER_PENALTY_MIN = 480
# Engineers may finish up to this many minutes past their contracted work_end
# without penalty — normal "bank-some-hours" behaviour.
OVERTIME_ALLOWANCE_MIN = 120
# Max search time. The problem is tiny (≤4 vehicles × ≤16 jobs).
SEARCH_TIME_SECONDS = 10
# 2PL — both engineers must arrive within this many minutes of each other.
# Generous enough to absorb traffic noise, tight enough that they're genuinely
# on-site together (not back-to-back drop-bys 25 min apart).
PAIR_ARRIVAL_TOLERANCE_MIN = 30
# Per-engineer call-type preference penalty (per stop). Charged in the
# per-vehicle arc cost when an engineer has a non-empty preference list
# AND the job's category isn't in it. Tuned so the solver biases toward
# preferred matches without overruling geography on a busy day:
#   90 min ≈ one cross-London leg = the solver will accept a noticeable
#   detour to send a PMV to the right engineer, but if every preferred
#   engineer is already full the assignment still happens rather than
#   the job being dropped.
PREFERENCE_MISMATCH_PENALTY_MIN = 90
# Safety buffer in front of a job's `latest_departure` deadline. Without it
# the solver is technically compliant when arrival + duration == deadline
# (e.g. 09:48 + 60 = 10:48 against a 11:00 deadline = 12 min slack). That
# leaves the engineer no room for traffic noise, an over-running job, or
# the customer locking up early. We ask the solver to clear the deadline
# by at least this many minutes — i.e. arrival + duration + buffer ≤
# latest_departure — so a "off-site by 11" job is targeted to be done by
# ~10:45 at the latest, not 10:59. Hard constraint; if no slot can clear
# the buffer the job is dropped via the disjunction (same as before).
LATEST_DEPARTURE_BUFFER_MIN = 15
# Trainee service-time bonus. When an engineer is flagged is_trainee=True,
# the solver pads every job assigned to them by this many minutes — so a
# 60-min job becomes a 90-min slot on a trainee's route. Applied to
# service time only (travel is unchanged), via per-vehicle time transit
# callbacks. The deadline check (latest_departure) is tightened globally
# by the team-max bonus so a trainee can't be assigned a windowed job
# they couldn't finish in time.
TRAINEE_DURATION_BONUS_MIN = 30


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
        return SolveResult(
            routes=[],
            unassigned=[
                diagnose_unassigned(j, engineers, stock, billing_only_codes)
                for j in jobs
            ],
        )
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

    # Per-vehicle arc cost: base travel minutes + missing-parts penalty
    # + call-category preference penalty.
    def make_cost_cb(vehicle_idx: int):
        eng = engineers[vehicle_idx]
        # Pre-compute the engineer's preference set once per vehicle
        # rather than per-arc — this callback fires thousands of times
        # during the OR-Tools search.
        prefs = set(eng.preferred_call_categories)

        def cb(from_idx: int, to_idx: int) -> int:
            f = manager.IndexToNode(from_idx)
            t = manager.IndexToNode(to_idx)
            base = travel.seconds[f][t] // 60
            if t >= n_eng:
                job = jobs[t - n_eng]
                missing = _missing_parts(eng, job, stock, billing_only_codes)
                base += len(missing) * MISSING_PART_PENALTY_MIN
                # Soft preference: if the engineer has expressed any
                # preference AND this job's category isn't in it,
                # charge the mismatch penalty. Generalists (empty prefs)
                # pay no penalty either way. Jobs with no resolved
                # category (call_category=None) also skip — we don't
                # penalise on missing taxonomy data.
                if prefs and job.call_category and job.call_category not in prefs:
                    base += PREFERENCE_MISMATCH_PENALTY_MIN
            return base

        return cb

    for v in range(n_eng):
        cb_idx = routing.RegisterTransitCallback(make_cost_cb(v))
        routing.SetArcCostEvaluatorOfVehicle(cb_idx, v)

    # Time dimension uses pure travel time (no parts penalty), with a
    # per-vehicle service bonus for trainees. Each engineer gets their
    # own transit callback so a job's service time depends on which
    # engineer is doing it — Carl/trainee = duration + 30, otherwise
    # just duration. Required for AddDimensionWithVehicleTransits below.
    def make_time_transit_cb(vehicle_idx: int):
        eng = engineers[vehicle_idx]
        bonus = TRAINEE_DURATION_BONUS_MIN if eng.is_trainee else 0

        def cb(from_idx: int, to_idx: int) -> int:
            f = manager.IndexToNode(from_idx)
            t = manager.IndexToNode(to_idx)
            service = jobs[f - n_eng].duration_minutes if f >= n_eng else 0
            if f >= n_eng and bonus:
                service += bonus
            return service + (travel.seconds[f][t] // 60)

        return cb

    time_cb_indices = [
        routing.RegisterTransitCallback(make_time_transit_cb(v))
        for v in range(n_eng)
    ]

    routing.AddDimensionWithVehicleTransits(
        time_cb_indices, HORIZON_MIN, HORIZON_MIN, False, "Time"
    )
    time_dim = routing.GetDimensionOrDie("Time")
    # Team-max trainee bonus — used to tighten the off-site deadline
    # check below so a trainee can't be assigned a windowed job they
    # can't finish in time. Non-trainees get extra slack on those
    # jobs as a side effect, which is harmless.
    team_max_trainee_bonus = max(
        (TRAINEE_DURATION_BONUS_MIN if e.is_trainee else 0 for e in engineers),
        default=0,
    )

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
    cp_solver_for_windows = routing.solver()
    for j_idx, job in enumerate(jobs):
        node = n_eng + j_idx
        index = manager.NodeToIndex(node)
        earliest = time_to_minutes(job.earliest_access)
        # Standard arrival window: must fit within the engineer's work
        # day (with OT). Use a wide-open upper bound so an infeasible
        # latest_departure deadline (added below as a separate hard
        # constraint) forces the solver to DROP the job via the
        # disjunction, rather than us silently clamping to a value
        # that violates the customer's off-site deadline.
        latest_arrival = max(earliest, latest_with_ot - job.duration_minutes)
        time_dim.CumulVar(index).SetRange(earliest, latest_arrival)
        # Customer "must be off site by" deadline — derived from source
        # xls Fix Date. arrival + duration + buffer ≤ latest_departure.
        # Adding this as a separate constraint (not by clamping the
        # range) is the only way to make the case where earliest +
        # duration > latest_departure properly infeasible — which
        # combined with the disjunction means the solver drops the job
        # rather than scheduling it in violation of the deadline.
        # The buffer is there so the solver doesn't pick a slot that
        # JUST clears the deadline (e.g. 12 min spare against an 11:00
        # cut-off) — engineers need realistic room for traffic, an
        # over-running job, etc. Falls back to no-buffer if buffer
        # makes the job infeasible.
        if job.latest_departure is not None:
            deadline = time_to_minutes(job.latest_departure)
            # Effective service for the deadline check. We don't know which
            # vehicle will take the job at constraint time, so we pessimise
            # — assume any trainee on the team could be assigned. Without
            # this, a trainee could legally be picked for a windowed job
            # whose duration+30 doesn't fit before the deadline, blowing
            # the constraint mid-route. Side effect: non-trainees get
            # extra slack on these jobs (harmless).
            effective_service = job.duration_minutes + team_max_trainee_bonus
            buffer_min = LATEST_DEPARTURE_BUFFER_MIN
            # If the buffered constraint would make this job infeasible
            # (earliest + effective + buffer > deadline), back off the
            # buffer so the job can still be scheduled rather than
            # dropped — better to run the deadline tight than refuse
            # the work. The trainee bonus is NOT backed off here — that
            # would mean a trainee got the job but couldn't finish.
            if earliest + effective_service + buffer_min > deadline:
                buffer_min = max(0, deadline - earliest - effective_service)
            cp_solver_for_windows.Add(
                time_dim.CumulVar(index) + effective_service + buffer_min
                <= deadline
            )

    # Job-count dimension — drives even workload distribution.
    jobs_cb_idx = routing.RegisterUnaryTransitCallback(
        lambda idx: 1 if manager.IndexToNode(idx) >= n_eng else 0
    )
    routing.AddDimension(jobs_cb_idx, 0, n_jobs, True, "JobCount")
    jobs_dim = routing.GetDimensionOrDie("JobCount")
    jobs_dim.SetGlobalSpanCostCoefficient(JOB_BALANCE_SPAN_COST)

    # Soft "minimum 1 stop per engineer" — if there are at least as many
    # jobs as engineers, the solver should give every engineer something.
    # SetCumulVarSoftLowerBound on the route's end node penalises (1 - n_stops)
    # × IDLE_ENGINEER_PENALTY_MIN whenever a route is left empty, so the
    # solver actively reaches for work to fill an idle van. When jobs < eng
    # the penalty is unavoidable — paid, not enforced — keeping the model
    # feasible. Skipped when jobs < engineers so we don't prefer
    # impossible-to-staff plans.
    if n_jobs >= n_eng:
        for v in range(n_eng):
            end_idx = routing.End(v)
            jobs_dim.SetCumulVarSoftLowerBound(end_idx, 1, IDLE_ENGINEER_PENALTY_MIN)

    for j_idx in range(n_jobs):
        node = n_eng + j_idx
        routing.AddDisjunction([manager.NodeToIndex(node)], DROP_PENALTY_MIN)

    # Parts-aware HARD constraint (only when stock is loaded).
    #
    # Two-layer constraint:
    #   1. Per-job vehicle restriction (existing) — a job that needs a
    #      part can only go to engineers whose vans had at least 1 of it
    #      at start of day. This is a fast prune that limits the search.
    #   2. Cumulative per-part dimension (new) — across each engineer's
    #      whole route, total demand for each part code can't exceed
    #      that van's starting stock. This catches the "engineer fits
    #      part X at stop A then is asked to fit X again at stop B but
    #      van only had 1" case that the per-job check misses.
    #
    # Cumulative is implemented as one dimension per distinct part code
    # via AddDimensionWithVehicleCapacity. With the typical 10-50
    # distinct codes per day this adds ~10ms to solve time.
    if stock is not None:
        # Layer 1 — per-job vehicle restriction.
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

        # Layer 2 — cumulative per-part dimension.
        #
        # Build the set of distinct (non-billing-only) part codes
        # demanded across all jobs today. For each, register a unary
        # transit callback that returns the per-job demand for that
        # code (0 for engineer nodes / shadows / jobs that don't need
        # this code; quantity for jobs that do).
        all_codes: set[str] = set()
        for job in jobs:
            for p in job.required_parts:
                if is_billing_only(p.code, billing_only_codes):
                    continue
                all_codes.add(p.code.strip().upper())

        for code in all_codes:
            # Per-vehicle capacity = van's starting stock of this code.
            # Real engineers only — there's no "unassigned vehicle" in
            # the active list. If the van has 0, capacity is 0 and the
            # solver will refuse to send any job needing this part to
            # that engineer (subsumes the per-job restriction's effect
            # for the zero-stock case).
            capacities = []
            for eng in engineers:
                van = eng.vehicle_reg.strip().upper() if eng.vehicle_reg else ""
                qty = stock.quantity(van, code) if van else 0
                # OR-Tools wants ints; quantity is float-typed elsewhere
                # but always whole units in practice. Floor to be safe.
                capacities.append(max(0, int(qty)))

            # Per-stop demand callback. Closure captures `code` via
            # default arg to dodge Python's late-binding gotcha when
            # building callbacks in a loop.
            def _demand_cb(idx, code=code, n_eng=n_eng, jobs=jobs, manager=manager):
                node = manager.IndexToNode(idx)
                if node < n_eng:
                    return 0
                j_idx = node - n_eng
                if j_idx >= len(jobs):
                    return 0
                # Shadow secondaries don't actually fit — don't double
                # count their demand against the engineer's stock.
                if jobs[j_idx].is_shadow_duplicate:
                    return 0
                for p in jobs[j_idx].required_parts:
                    if p.code.strip().upper() == code:
                        return int(p.quantity)
                return 0

            cb_idx = routing.RegisterUnaryTransitCallback(_demand_cb)
            # Dimension name needs to be unique + a valid identifier.
            dim_name = "Part_" + "".join(
                c if c.isalnum() else "_" for c in code
            )[:60]
            # AddDimensionWithVehicleCapacity:
            #   evaluator_index, slack, vehicle_capacities,
            #   fix_start_cumul_to_zero, name
            routing.AddDimensionWithVehicleCapacity(
                cb_idx, 0, capacities, True, dim_name
            )

    # Forced-engineer assignment (COL collection runs). When the office
    # has hand-picked who must collect parts for a job, restrict the
    # job's VehicleVar to ONLY that engineer's index. -1 stays in the
    # allowed set so the solver can still drop the job (with the
    # disjunction penalty) if that engineer is full or unavailable —
    # better than refusing the whole plan.
    #
    # NB: when the named engineer isn't in the active routing list
    # (OFF / annual leave / name typo / deleted account), we MUST NOT
    # leave the constraint unset — that lets the solver silently route
    # the job to whoever's cheapest, which the office reads as the
    # planner ignoring the lock. Lock the VehicleVar to [-1] only
    # (i.e. "may only be unassigned") so the disjunction drops it
    # and diagnose_unassigned can surface the
    # `forced_engineer_unavailable` reason instead of a mystery drop.
    for j_idx, job in enumerate(jobs):
        if not job.forced_engineer_name:
            continue
        forced_idx = next(
            (v for v, eng in enumerate(engineers)
             if eng.name == job.forced_engineer_name),
            None,
        )
        index = manager.NodeToIndex(n_eng + j_idx)
        if forced_idx is None:
            # Engineer not in the routing pool — lock to "drop only"
            # so the office gets a clear unassigned reason instead of
            # the job silently rerouting.
            routing.VehicleVar(index).SetValues([-1])
            continue
        routing.VehicleVar(index).SetValues([forced_idx, -1])

    # Must-be-first — for jobs the office has promised the customer as
    # the day's opening call. On whichever vehicle ends up serving the
    # job, NextVar(Start(v)) must be the job's node. Implementation:
    #
    #   For each must_be_first job j and each vehicle v:
    #     active(j) ∧ vehicle(j)==v  ⇒  next_after_start(v) == node(j)
    #
    # Linearised as:
    #     next_eq_job(v) + (1 - is_this_v) + (1 - active) >= 1
    #
    # If two must_be_first jobs land on the same vehicle the constraints
    # conflict and the solver drops one (paying the disjunction penalty)
    # rather than refusing the whole plan — same graceful-degrade pattern
    # as forced_engineer.
    cp_solver = routing.solver()
    for j_idx, job in enumerate(jobs):
        if not job.must_be_first:
            continue
        # Shadow secondaries don't carry the flag — primary holds it.
        if job.is_shadow_duplicate:
            continue
        job_node_index = manager.NodeToIndex(n_eng + j_idx)
        job_veh = routing.VehicleVar(job_node_index)
        job_active = routing.ActiveVar(job_node_index)
        for v in range(n_eng):
            next_after_start = routing.NextVar(routing.Start(v))
            is_this_v = cp_solver.IsEqualCstVar(job_veh, v)
            next_eq_job = cp_solver.IsEqualCstVar(
                next_after_start, job_node_index
            )
            cp_solver.Add(next_eq_job + (1 - is_this_v) + (1 - job_active) >= 1)

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
        return SolveResult(
            routes=[],
            unassigned=[
                diagnose_unassigned(j, engineers, stock, billing_only_codes)
                for j in jobs
            ],
        )

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
        # Trainee bonus is applied per-job at output time so the
        # displayed depart-from-this-site reflects the actual time
        # the engineer leaves (arrival + duration + bonus). Without
        # this, the route panel would show a tighter slot than the
        # engineer's actual time progression — confusing the office.
        trainee_bonus = TRAINEE_DURATION_BONUS_MIN if eng.is_trainee else 0
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
            effective_service = job.duration_minutes + trainee_bonus
            route.stops.append(
                Stop(
                    job=job,
                    arrival_minute=arrival,
                    departure_minute=arrival + effective_service,
                    travel_seconds_from_previous=travel_sec,
                    missing_parts=missing,
                    is_pair_secondary=job.is_pair_secondary,
                )
            )
            assigned_job_indices.add(job_idx)
            job_idx_to_engineer[job_idx] = eng.name
            per_stop_job_idx.append(job_idx)
            route.total_drive_seconds += travel_sec
            route.total_service_minutes += effective_service
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

    # Drop SHADOW secondaries from `unassigned` — those are synthetic
    # duplicates the optimiser added to route a single 2PL call across
    # two engineers; surfacing them too would double-count the same job.
    # Real-pair secondaries (two distinct call_numbers raised at one
    # site) are kept: if the pair fails to route, the office should see
    # both call_numbers in the unassigned list.
    unassigned = [
        jobs[i] for i in range(n_jobs)
        if i not in assigned_job_indices and not jobs[i].is_shadow_duplicate
    ]
    # Attach a friendly reason for each — the office shouldn't have to
    # guess between parts shortage, infeasible window, forced-engineer
    # mismatch, etc. Returns the same Job dataclass with `unassigned_reason`
    # / `unassigned_reason_tag` populated where we can identify a clear
    # cause; otherwise None (meaning "the solver dropped this on cost
    # grounds — typically over-packed engineers / time-window squeeze").
    diagnosed = [
        diagnose_unassigned(j, engineers, stock, billing_only_codes)
        for j in unassigned
    ]
    return SolveResult(routes=routes, unassigned=diagnosed)


def diagnose_unassigned(
    job: Job,
    engineers: list[Engineer],
    stock: StockSnapshot | None,
    billing_only_codes: list[str] | None,
) -> Job:
    """
    Return the job dataclass with `unassigned_reason` / `unassigned_reason_tag`
    populated when we can pin down why the solver dropped it.

    Reasons checked, in priority order:
      1. parts_shortage — no available engineer's van has the full set of
         required parts. This is the parts-aware HARD constraint kicking in;
         the office needs to either re-allocate stock or add the parts to
         a van.
      2. forced_engineer_unavailable — the COL assignment locked the job
         to an engineer who's OFF / on annual leave today.
      3. window_too_tight — earliest_access + duration > customer's
         latest_departure deadline (with the trainee bonus tightening
         applied if any trainees are on the team).
      4. window_after_workday — earliest_access is so late that no
         engineer can finish before their work_end (+ overtime allowance).
      5. two_engineer_pair_dropped — 2PL job whose partner also failed
         to route (the pairing constraint demands both-or-neither).

    None reason means we couldn't isolate one of these — usually the
    solver dropped the job because every engineer was over-packed once
    higher-priority work landed. The office can clear that by removing
    something else or extending availability.
    """
    available = [e for e in engineers if e.availability == Availability.AVAILABLE]

    # 1. Parts shortage — every engineer (including unavailable ones for
    # diagnostic purposes) is missing at least one part.
    if job.required_parts and stock is not None:
        missing_per_eng = [
            _missing_parts(e, job, stock, billing_only_codes)
            for e in engineers
        ]
        if all(m for m in missing_per_eng):
            short_codes = sorted(
                {code for missing in missing_per_eng for code in missing}
            )
            preview = ", ".join(short_codes[:3])
            if len(short_codes) > 3:
                preview += f" +{len(short_codes) - 3} more"
            return _with_reason(
                job,
                tag="parts_shortage",
                reason=(
                    f"No van has all required parts (missing: {preview}). "
                    "Re-stock or re-allocate before optimising."
                ),
            )

    # 2. Forced engineer (COL) — locked to someone who isn't in today's
    # routing pool. By the time diagnose_unassigned runs the engineers
    # list has already been filtered to AVAILABLE-only by optimiser.py,
    # so a missing target covers both "name typo" and "marked OFF/AL"
    # — they're indistinguishable from this side, but the message
    # doesn't need to distinguish either: the office just needs to
    # know the lock can't be honoured.
    if job.forced_engineer_name:
        target = next(
            (e for e in engineers if e.name == job.forced_engineer_name),
            None,
        )
        if target is None:
            return _with_reason(
                job,
                tag="forced_engineer_unavailable",
                reason=(
                    f"Locked to '{job.forced_engineer_name}' but they're "
                    "not in today's routing pool (off, on annual leave, "
                    "or the name doesn't match an engineer record). "
                    "Re-assign the COL or update availability and re-optimise."
                ),
            )
        if target.availability != Availability.AVAILABLE:
            # Defensive — shouldn't happen given the upstream filter,
            # but kept in case a future code-path passes the full list.
            return _with_reason(
                job,
                tag="forced_engineer_unavailable",
                reason=(
                    f"Locked to {target.name}, who's "
                    f"{target.availability.value.lower().replace('_', ' ')} today. "
                    "Re-assign or change their availability."
                ),
            )

    # 3. Window vs duration — including the trainee bonus if any.
    if job.latest_departure is not None:
        team_max_bonus = max(
            (
                TRAINEE_DURATION_BONUS_MIN if e.is_trainee else 0
                for e in engineers
            ),
            default=0,
        )
        earliest = time_to_minutes(job.earliest_access)
        deadline = time_to_minutes(job.latest_departure)
        if earliest + job.duration_minutes + team_max_bonus > deadline:
            slack = deadline - earliest - job.duration_minutes
            return _with_reason(
                job,
                tag="window_too_tight",
                reason=(
                    f"Window too tight: {earliest // 60:02d}:{earliest % 60:02d}"
                    f"–{deadline // 60:02d}:{deadline % 60:02d} can't fit "
                    f"{job.duration_minutes} min duration"
                    + (f" + {team_max_bonus} min trainee bonus" if team_max_bonus else "")
                    + f" (only {max(0, slack)} min usable). "
                    "Push the deadline or shorten the job."
                ),
            )

    # 4. Earliest access too late vs work_end (+ OT) — no engineer can fit.
    if available:
        latest_end_with_ot = max(
            time_to_minutes(e.work_end) + OVERTIME_ALLOWANCE_MIN
            for e in available
        )
        earliest = time_to_minutes(job.earliest_access)
        if earliest + job.duration_minutes > latest_end_with_ot:
            return _with_reason(
                job,
                tag="window_after_workday",
                reason=(
                    f"Earliest access {earliest // 60:02d}:{earliest % 60:02d} "
                    f"+ {job.duration_minutes} min runs past every engineer's "
                    "work-end (incl. overtime). Lift access window or extend hours."
                ),
            )

    # 5. 2PL pair — needs at least 2 available engineers; if not, the
    # pairing constraint forces it to drop.
    if job.two_engineer and len(available) < 2:
        return _with_reason(
            job,
            tag="two_engineer_pair_dropped",
            reason=(
                f"2-engineer job needs two available engineers; only "
                f"{len(available)} working today."
            ),
        )

    # Nothing clearly diagnosable — leave reason blank. Most common
    # cause when this fires is the day being over-packed: every
    # engineer's route hit a hard time/parts constraint that pushed
    # this stop out via the disjunction penalty.
    return _with_reason(job, tag=None, reason=None)


def _with_reason(job: Job, *, tag: str | None, reason: str | None) -> Job:
    """Return a copy of `job` with diagnostic fields populated."""
    return Job(
        call_number=job.call_number,
        site_name=job.site_name,
        postcode=job.postcode,
        earliest_access=job.earliest_access,
        latest_departure=job.latest_departure,
        duration_minutes=job.duration_minutes,
        required_parts=job.required_parts,
        two_engineer=job.two_engineer,
        is_pair_secondary=job.is_pair_secondary,
        is_shadow_duplicate=job.is_shadow_duplicate,
        forced_engineer_name=job.forced_engineer_name,
        must_be_first=job.must_be_first,
        call_category=job.call_category,
        unassigned_reason=reason,
        unassigned_reason_tag=tag,
    )
