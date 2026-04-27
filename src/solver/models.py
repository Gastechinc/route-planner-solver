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
from typing import Annotated

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
    duration_minutes: int = 60
    required_parts: tuple[RequiredPart, ...] = ()


@dataclass(frozen=True)
class Engineer:
    name: str
    home_postcode: str
    work_start: time = time(8, 0)
    work_end: time = time(16, 0)
    vehicle_reg: str = ""
    availability: Availability = Availability.AVAILABLE


@dataclass(frozen=True)
class Stop:
    job: Job
    arrival_minute: int
    departure_minute: int
    travel_seconds_from_previous: int
    missing_parts: tuple[str, ...] = ()


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
    duration_minutes: int = Field(default=60, ge=1, le=600)
    required_parts: list[RequiredPartIn] = Field(default_factory=list)

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
            duration_minutes=self.duration_minutes,
            required_parts=tuple(p.to_internal() for p in self.required_parts),
        )


class EngineerIn(BaseModel):
    name: str
    home_postcode: str
    work_start: TimeStr = "08:00"
    work_end: TimeStr = "16:00"
    vehicle_reg: str = ""
    availability: Availability = Availability.AVAILABLE

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
