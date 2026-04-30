"""Domain models — both the internal solver dataclasses and the API
request/response shapes (pydantic).

Mirrors the desktop app's models but trimmed for service use:
  - drops Path-based settings (no filesystem on Render)
  - drops Pydantic for the in-solver dataclasses (faster, no validation cost
    inside the OR-Tools loop)
  - adds explicit pydantic API models with sample-validated time/postcode
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
from enum import Enum
from typing import Annotated, Optional

from pydantic import BaseModel, Field, field_validator


# ──────────────────────────────────────────────────────────────────────
#   Enums
# ──────────────────────────────────────────────────────────────────────


class Availability(str, Enum):
    AVAILABLE = "AVAILABLE"
    OFF = "OFF"
    ANNUAL_LEAVE = "ANNUAL_LEAVE"


# ──────────────────────────────────────────────────────────────────────
#   Internal dataclasses — used inside solver.py / optimiser.py
#   These are passed by reference into hot loops; pydantic models would add
#   per-instance overhead we don't need.
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RequiredPart:
    code: str
    quantity: int = 1


@dataclass(frozen=True)
class Job:
    call_number: str
    site_name: str
    postcode: str
    earliest_access: time = time(8, 0)
    # Hard "must be off site by" deadline — derived from the source xls
    # "Fix Date" field. The solver enforces departure_minute ≤
    # latest_departure (i.e. arrival_minute + duration ≤ latest_departure)
    # so the engineer is GONE by this time, not just arriving by it. None
    # means no deadline beyond the engineer's normal work_end + overtime.
    latest_departure: time | None = None
    duration_minutes: int = 60
    required_parts: tuple[RequiredPart, ...] = ()
    # 2PL — needs two engineers on scene simultaneously.
    #   Real pair: two distinct call_numbers raised at the same site/day,
    #     both flagged two_engineer. The optimiser pairs them directly
    #     (one is_pair_secondary=True, neither is_shadow_duplicate).
    #   Lone 2PL: only one call_number raised. The optimiser duplicates
    #     it into a shadow secondary so both engineers get the visit;
    #     the shadow has is_shadow_duplicate=True and is filtered out
    #     of the unassigned list.
    two_engineer: bool = False
    is_pair_secondary: bool = False
    is_shadow_duplicate: bool = False
    # Forced engineer assignment — set when the office has hand-picked
    # who must do this job (typically COL collection runs where the
    # office knows which engineer's van the parts belong on). The
    # solver restricts VehicleVar to this engineer's index, so the
    # optimiser can't override the assignment based on geography.
    forced_engineer_name: str | None = None
    # Must-be-first — when set, this job must be the FIRST non-depot
    # stop on whichever vehicle ends up serving it. Used for jobs the
    # office has promised the customer as the day's opening call.
    must_be_first: bool = False
    # Call-type category — passed through from the route-planner-web
    # taxonomy (contract_pm / emergency / recall / standard / etc.).
    # Used by the per-engineer preference soft constraint: when an
    # engineer has a non-empty preferred_call_categories list AND this
    # category isn't in it, the assignment is penalised so the solver
    # biases toward better-matched engineers. None ⇒ no preference
    # check (treated as a generalist match).
    call_category: str | None = None


@dataclass(frozen=True)
class Engineer:
    name: str
    home_postcode: str
    work_start: time = time(8, 0)
    work_end: time = time(16, 0)
    vehicle_reg: str = ""
    availability: Availability = Availability.AVAILABLE
    # Engineer's preferred call-type categories. Empty = generalist
    # (no preference). Non-empty = solver applies a soft penalty when
    # a job whose category isn't in this set is assigned to them.
    # Tuple (not list) so the dataclass stays frozen + hashable.
    preferred_call_categories: tuple[str, ...] = ()


@dataclass(frozen=True)
class Stop:
    job: Job
    arrival_minute: int
    departure_minute: int
    travel_seconds_from_previous: int
    missing_parts: tuple[str, ...] = ()
    # 2PL — name of the OTHER engineer attending this same job.
    # None for single-engineer jobs.
    paired_with: str | None = None
    # True if this stop is the "shadow" routed onto the second engineer
    # so their day plan reflects the visit. The frontend treats the
    # primary stop (False) as the canonical one for things like the
    # customer-confirmation email button.
    is_pair_secondary: bool = False


@dataclass
class EngineerRoute:
    engineer: Engineer
    stops: list[Stop] = field(default_factory=list)
    return_minute: int = 0
    total_drive_seconds: int = 0
    total_service_minutes: int = 0


@dataclass
class SolveResult:
    routes: list[EngineerRoute]
    unassigned: list[Job]
    warnings: list[str] = field(default_factory=list)
    geocoded: dict[str, tuple[float, float]] = field(default_factory=dict)


# ──────────────────────────────────────────────────────────────────────
#   Pydantic API models — for FastAPI request/response
# ──────────────────────────────────────────────────────────────────────

# HH:MM string — kept as text in the API to keep clients language-agnostic
TimeStr = Annotated[str, Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")]


def _parse_time(s: str) -> time:
    h, m = s.split(":")
    return time(int(h), int(m))


def _format_time(t: time) -> str:
    return f"{t.hour:02d}:{t.minute:02d}"


class RequiredPartIn(BaseModel):
    code: str
    quantity: int = Field(default=1, ge=1)

    def to_internal(self) -> RequiredPart:
        return RequiredPart(code=self.code.strip(), quantity=self.quantity)


class JobIn(BaseModel):
    call_number: str
    site_name: str
    postcode: str
    earliest_access: TimeStr = "08:00"
    latest_departure: Optional[TimeStr] = Field(
        default=None,
        description="Hard 'must be off site by' deadline (HH:MM). Derived "
        "from the source xls 'Fix Date' field. Solver enforces "
        "departure_minute ≤ this — so the engineer is GONE by this time, "
        "not just arriving by it. None ⇒ no deadline beyond work_end+OT.",
    )
    duration_minutes: int = Field(default=60, ge=1, le=600)
    required_parts: list[RequiredPartIn] = Field(default_factory=list)
    two_engineer: bool = Field(
        default=False,
        description="2PL — needs two engineers on scene simultaneously. "
        "The optimiser duplicates the node and constrains both stops to "
        "different vehicles + arrivals within 30 min of each other.",
    )
    forced_engineer_name: str | None = Field(
        default=None,
        description="Name of the engineer who MUST do this job — used "
        "for COL collection runs the office hand-assigns. The solver "
        "restricts VehicleVar to that engineer's index so the optimiser "
        "can't reassign based on geography.",
    )
    must_be_first: bool = Field(
        default=False,
        description="If true, this job must be the FIRST non-depot stop "
        "on whichever vehicle ends up serving it (NextVar(Start(v)) is "
        "constrained to this job's node when assigned). Multiple "
        "must_be_first jobs on the same vehicle would conflict — the "
        "solver will drop one (paying the disjunction penalty) rather "
        "than refuse the plan.",
    )
    call_category: Optional[str] = Field(
        default=None,
        description="Resolved call-type category from the route-planner "
        "taxonomy (e.g. contract_pm / emergency / recall / standard). "
        "Used to apply per-engineer preference penalties. None ⇒ no "
        "preference check.",
    )

    @field_validator("postcode")
    @classmethod
    def _postcode_strip(cls, v: str) -> str:
        return v.strip().upper()

    def to_internal(self) -> Job:
        return Job(
            call_number=self.call_number,
            site_name=self.site_name,
            postcode=self.postcode,
            earliest_access=_parse_time(self.earliest_access),
            latest_departure=(
                _parse_time(self.latest_departure)
                if self.latest_departure is not None
                else None
            ),
            duration_minutes=self.duration_minutes,
            required_parts=tuple(p.to_internal() for p in self.required_parts),
            two_engineer=self.two_engineer,
            forced_engineer_name=self.forced_engineer_name,
            must_be_first=self.must_be_first,
            call_category=self.call_category,
        )


class EngineerIn(BaseModel):
    name: str
    home_postcode: str
    work_start: TimeStr = "08:00"
    work_end: TimeStr = "16:00"
    vehicle_reg: str = ""
    availability: Availability = Availability.AVAILABLE
    preferred_call_categories: list[str] = Field(
        default_factory=list,
        description="Call-type categories this engineer prefers. Empty "
        "list = generalist (no penalty either way). Non-empty list = "
        "soft penalty applied when a job whose category isn't in this "
        "list is assigned to them.",
    )

    @field_validator("home_postcode", "vehicle_reg")
    @classmethod
    def _strip_upper(cls, v: str) -> str:
        return v.strip().upper()

    def to_internal(self) -> Engineer:
        return Engineer(
            name=self.name,
            home_postcode=self.home_postcode,
            work_start=_parse_time(self.work_start),
            work_end=_parse_time(self.work_end),
            vehicle_reg=self.vehicle_reg,
            availability=self.availability,
            preferred_call_categories=tuple(self.preferred_call_categories),
        )


class StockIn(BaseModel):
    """Stock snapshot from public.stock — keyed by van_reg → {stock_code: qty}.

    This is the JSON-friendly form the web app sends. The solver internally
    converts to a StockSnapshot dataclass with a `quantity(van, code)` helper.
    """

    by_location: dict[str, dict[str, float]] = Field(default_factory=dict)


class OptimiseRequest(BaseModel):
    target_date: str = Field(
        ...,
        description="ISO 8601 date or datetime — used for traffic-time forecast",
        examples=["2026-04-28", "2026-04-28T00:00:00Z"],
    )
    engineers: list[EngineerIn]
    jobs: list[JobIn]
    stock: StockIn = Field(default_factory=StockIn)
    billing_only_codes: list[str] = Field(
        default_factory=lambda: ["Parking", "Congestion Charge"],
        description="Codes the solver treats as always-available (invoicing line items, not real parts)",
    )
    apply_parking: bool = True
    time_dependent: bool = True

    def parsed_target_date(self) -> datetime:
        s = self.target_date
        # Tolerate both date-only and full ISO datetime
        if len(s) == 10:
            return datetime.fromisoformat(s + "T00:00:00")
        return datetime.fromisoformat(s.replace("Z", "+00:00"))


# ── Response models ──


class StopOut(BaseModel):
    call_number: str
    site_name: str
    postcode: str
    arrival_minute: int
    departure_minute: int
    arrival_time: TimeStr
    departure_time: TimeStr
    travel_seconds_from_previous: int
    missing_parts: list[str] = Field(default_factory=list)
    paired_with: str | None = None
    is_pair_secondary: bool = False


class EngineerRouteOut(BaseModel):
    engineer_name: str
    vehicle_reg: str
    home_postcode: str
    stops: list[StopOut]
    return_minute: int
    return_time: TimeStr
    total_drive_seconds: int
    total_service_minutes: int


class JobOut(BaseModel):
    call_number: str
    site_name: str
    postcode: str


class OptimiseResponse(BaseModel):
    ok: bool = True
    routes: list[EngineerRouteOut]
    unassigned: list[JobOut]
    warnings: list[str] = Field(default_factory=list)
    geocoded: dict[str, tuple[float, float]] = Field(default_factory=dict)


def stop_to_out(stop: Stop) -> StopOut:
    return StopOut(
        call_number=stop.job.call_number,
        site_name=stop.job.site_name,
        postcode=stop.job.postcode,
        arrival_minute=stop.arrival_minute,
        departure_minute=stop.departure_minute,
        arrival_time=_format_time(_minutes_to_time(stop.arrival_minute)),
        departure_time=_format_time(_minutes_to_time(stop.departure_minute)),
        travel_seconds_from_previous=stop.travel_seconds_from_previous,
        missing_parts=list(stop.missing_parts),
        paired_with=stop.paired_with,
        is_pair_secondary=stop.is_pair_secondary,
    )


def route_to_out(route: EngineerRoute) -> EngineerRouteOut:
    return EngineerRouteOut(
        engineer_name=route.engineer.name,
        vehicle_reg=route.engineer.vehicle_reg,
        home_postcode=route.engineer.home_postcode,
        stops=[stop_to_out(s) for s in route.stops],
        return_minute=route.return_minute,
        return_time=_format_time(_minutes_to_time(route.return_minute)),
        total_drive_seconds=route.total_drive_seconds,
        total_service_minutes=route.total_service_minutes,
    )


def _minutes_to_time(m: int) -> time:
    m = max(0, min(m, 24 * 60 - 1))
    return time(m // 60, m % 60)
