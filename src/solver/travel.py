"""Travel-time matrices.

Two implementations behind the same TravelMatrix interface:
  - mock_travel_matrix   : haversine × sinuosity / fixed UK speed (offline, free)
  - mapbox_travel_matrix : Mapbox Matrix API with live-traffic profile (token required)

Plus `mapbox_directions_duration` for single-arc lookups with a specific
`depart_at` — used by the two-pass optimiser to refine used arcs with the
actual predicted departure time for each leg (true time-dependent routing).

The optimiser picks Mapbox when a token is configured, mock otherwise.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, time, timezone

import httpx

# Default parking buffer added to arcs ending at a central-London job.
# 15 minutes is a realistic average for finding a meter / car park / dropping
# off near the site before the engineer can start work.
CENTRAL_LONDON_PARKING_BUFFER_SECONDS = 15 * 60

# UK average urban+rural mix. Tuned to be conservative (favours over-estimating
# drive time so the schedule has slack).
UK_AVG_KMH = 40.0
ROAD_SINUOSITY = 1.3  # straight-line × this ≈ road distance


@dataclass(frozen=True)
class TravelMatrix:
    """Square matrix of travel times (seconds) and distances (metres)."""

    seconds: list[list[int]]
    distance_metres: list[list[int]]

    @property
    def n(self) -> int:
        return len(self.seconds)


def haversine_metres(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle distance in metres."""
    R = 6_371_000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    )
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


MAPBOX_MATRIX_URL = "https://api.mapbox.com/directions-matrix/v1/mapbox/{profile}/{coords}"
# driving-traffic profile is capped at 10 coords per request on all tiers.
# driving / walking / cycling allow 25.
_PROFILE_COORD_LIMITS = {
    "driving-traffic": 10,
    "driving": 25,
    "walking": 25,
    "cycling": 25,
}


class MapboxError(Exception):
    """Raised when the Mapbox Matrix API can't return a usable matrix."""


def _format_coord(lat: float, lng: float) -> str:
    return f"{lng:.6f},{lat:.6f}"  # Mapbox expects lng,lat


def _mapbox_call(
    coords: list[tuple[float, float]],
    token: str,
    profile: str,
    sources: list[int] | None = None,
    destinations: list[int] | None = None,
) -> dict:
    coord_str = ";".join(_format_coord(lat, lng) for lat, lng in coords)
    url = MAPBOX_MATRIX_URL.format(profile=profile, coords=coord_str)
    params: dict[str, str] = {
        "access_token": token,
        "annotations": "duration,distance",
    }
    if sources is not None:
        params["sources"] = ";".join(str(s) for s in sources)
    if destinations is not None:
        params["destinations"] = ";".join(str(d) for d in destinations)
    try:
        resp = httpx.get(url, params=params, timeout=30)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise MapboxError(f"Mapbox request failed: {exc}") from exc
    data = resp.json()
    if data.get("code") != "Ok":
        raise MapboxError(f"Mapbox returned {data.get('code')}: {data.get('message', '')}")
    return data


def mapbox_travel_matrix(
    coords: list[tuple[float, float]],
    token: str,
    profile: str = "driving-traffic",
) -> TravelMatrix:
    """Build a TravelMatrix from Mapbox Matrix API.

    Live-traffic profile is capped at 10 coords per request, so larger problems
    are split into one row-chunk per origin (each ≤ 9 destinations per call).
    """
    n = len(coords)
    if n == 0:
        return TravelMatrix(seconds=[], distance_metres=[])

    seconds: list[list[int]] = [[0] * n for _ in range(n)]
    distances: list[list[int]] = [[0] * n for _ in range(n)]
    max_per_call = _PROFILE_COORD_LIMITS.get(profile, 10)

    if n <= max_per_call:
        data = _mapbox_call(coords, token, profile)
        durations = data["durations"]
        dists = data["distances"]
        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                if durations[i][j] is None or dists[i][j] is None:
                    raise MapboxError(
                        f"No route found between coord {i} and coord {j}"
                    )
                seconds[i][j] = int(durations[i][j])
                distances[i][j] = int(dists[i][j])
        return TravelMatrix(seconds=seconds, distance_metres=distances)

    # Chunked path: for each origin, query its row in chunks of (max-1) destinations.
    chunk_size = max_per_call - 1
    for i in range(n):
        others = [j for j in range(n) if j != i]
        for c0 in range(0, len(others), chunk_size):
            chunk = others[c0 : c0 + chunk_size]
            request_coords = [coords[i]] + [coords[j] for j in chunk]
            data = _mapbox_call(
                request_coords,
                token,
                profile,
                sources=[0],
                destinations=list(range(1, len(request_coords))),
            )
            row_dur = data["durations"][0]
            row_dist = data["distances"][0]
            for k, j in enumerate(chunk):
                if row_dur[k] is None or row_dist[k] is None:
                    raise MapboxError(
                        f"No route found between coord {i} and coord {j}"
                    )
                seconds[i][j] = int(row_dur[k])
                distances[i][j] = int(row_dist[k])

    return TravelMatrix(seconds=seconds, distance_metres=distances)


# ----------------------------------------------------------------------------
# Single-arc Directions API lookup (for time-dependent routing).
# ----------------------------------------------------------------------------

MAPBOX_DIRECTIONS_URL = "https://api.mapbox.com/directions/v5/mapbox/{profile}/{coords}"


def _minutes_since_midnight_to_iso(day_date: datetime, minute_of_day: int) -> str:
    """Build an ISO8601 timestamp for Mapbox `depart_at`.

    `day_date` is an aware or naive datetime whose date we use. `minute_of_day`
    is minutes since 00:00 on that date. Mapbox accepts `YYYY-MM-DDTHH:MM` in
    the local timezone of the request or with an explicit offset.
    """
    dt = datetime.combine(day_date.date(), time(0, 0))
    dt = dt.replace(minute=0, hour=0) + _td_minutes(minute_of_day)
    # Mapbox wants no seconds, local-time ISO format
    return dt.strftime("%Y-%m-%dT%H:%M")


def _td_minutes(m: int):
    from datetime import timedelta
    return timedelta(minutes=m)


def mapbox_directions_duration(
    origin: tuple[float, float],
    destination: tuple[float, float],
    token: str,
    depart_at: datetime | None = None,
    profile: str = "driving-traffic",
) -> tuple[int, int]:
    """Query Mapbox Directions API for a single arc and return
    (duration_seconds, distance_metres).

    If `depart_at` is provided, Mapbox uses its traffic prediction model for
    that future time. Otherwise, live-now traffic is used (same as Matrix
    API). Falling back here is fine — returns the same numbers as the matrix.

    Raises `MapboxError` on failure.
    """
    coord_str = f"{_format_coord(*origin)};{_format_coord(*destination)}"
    url = MAPBOX_DIRECTIONS_URL.format(profile=profile, coords=coord_str)
    params: dict[str, str] = {
        "access_token": token,
        "alternatives": "false",
        "geometries": "geojson",
        "overview": "false",
        "steps": "false",
        "annotations": "duration,distance",
    }
    if depart_at is not None:
        # Mapbox accepts local-time ISO (no seconds)
        params["depart_at"] = depart_at.strftime("%Y-%m-%dT%H:%M")
    try:
        resp = httpx.get(url, params=params, timeout=20)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise MapboxError(f"Directions request failed: {exc}") from exc
    data = resp.json()
    if data.get("code") != "Ok":
        raise MapboxError(f"Directions returned {data.get('code')}: {data.get('message', '')}")
    routes = data.get("routes") or []
    if not routes:
        raise MapboxError("No route returned")
    r = routes[0]
    return int(r["duration"]), int(r["distance"])


# ----------------------------------------------------------------------------
# Central London parking buffer.
# ----------------------------------------------------------------------------

# Outcodes that count as "central London" for the 15-min parking buffer.
# Single-digit outcodes (N1, E1, SE1, SW1, W1, NW1) and their subcodes
# (SW1A, W1D, etc.) are all central. Multi-digit outcodes (N10, E14, etc.)
# are mostly outer zones except E14 (Canary Wharf).
_CENTRAL_EXACT_OUTCODES = {
    "N1", "E1", "SE1", "SW1", "W1", "NW1", "EC1", "EC2", "EC3", "EC4",
    "WC1", "WC2", "E14",
}


def is_central_london(postcode: str) -> bool:
    """Is this postcode inside the zone that needs a parking buffer?

    Handles both spaced ("SW1A 1AA") and unspaced ("SW1A1AA") postcodes.
    """
    if not postcode:
        return False
    p = postcode.strip().upper()
    outcode = p.split()[0] if " " in p else p[:4]
    # Exact match (SW1, W1, E1, EC1, E14, ...)
    if outcode in _CENTRAL_EXACT_OUTCODES:
        return True
    # Subcodes like W1D, SW1A, N1C, EC1M, WC2E — prefix matches a base
    # central outcode AND the next character is a letter (subcode letter).
    # "N10" has a digit at position 2, so it correctly falls through.
    for base in _CENTRAL_EXACT_OUTCODES:
        if len(base) >= 2 and outcode.startswith(base):
            tail = outcode[len(base):]
            if tail and tail[0].isalpha():
                return True
    return False


def apply_parking_buffer(
    matrix: TravelMatrix,
    postcodes: list[str],
    n_engineers: int,
    buffer_seconds: int = CENTRAL_LONDON_PARKING_BUFFER_SECONDS,
) -> tuple[TravelMatrix, list[int]]:
    """Return a new TravelMatrix with `buffer_seconds` added to any arc that
    ends at a central-London JOB (node index >= n_engineers).

    Returns the new matrix + the list of destination-node indices that got the
    buffer, so the UI can label them.
    """
    n = len(postcodes)
    if matrix.n != n:
        raise ValueError(f"Postcode list size {n} != matrix size {matrix.n}")

    affected: list[int] = []
    for j in range(n):
        # Only jobs get the buffer — engineer homes don't.
        if j < n_engineers:
            continue
        if is_central_london(postcodes[j]):
            affected.append(j)

    if not affected:
        return matrix, affected

    new_seconds = [row[:] for row in matrix.seconds]
    affected_set = set(affected)
    for i in range(n):
        for j in affected_set:
            if i == j:
                continue
            new_seconds[i][j] = matrix.seconds[i][j] + buffer_seconds

    return TravelMatrix(seconds=new_seconds, distance_metres=matrix.distance_metres), affected


# ----------------------------------------------------------------------------
# Two-pass refinement: use Directions API to replace used-arc durations with
# true depart_at-aware times.
# ----------------------------------------------------------------------------

def refine_matrix_with_depart_at(
    matrix: TravelMatrix,
    coords: list[tuple[float, float]],
    used_arcs: list[tuple[int, int, int]],  # (from_idx, to_idx, depart_minute_of_day)
    token: str,
    day_date: datetime | None = None,
    parking_affected_nodes: set[int] | None = None,
    parking_seconds: int = CENTRAL_LONDON_PARKING_BUFFER_SECONDS,
    profile: str = "driving-traffic",
) -> TravelMatrix:
    """For each (i, j, depart_min) in `used_arcs`, replace matrix.seconds[i][j]
    with Mapbox's predicted travel time at that departure. Other arcs stay put.

    Central-London parking buffer is re-applied after the Mapbox lookup for
    any arc whose destination is in `parking_affected_nodes` (the solver works
    with "travel + buffer" as a single cost, so we keep consistent).

    If a Mapbox request fails, that arc keeps its current-matrix value and the
    error is swallowed — we never want a single flaky request to kill a whole
    optimisation.
    """
    if day_date is None:
        day_date = datetime.now()

    new_seconds = [row[:] for row in matrix.seconds]
    parking_affected = parking_affected_nodes or set()

    for from_idx, to_idx, depart_min in used_arcs:
        if from_idx == to_idx:
            continue
        depart_dt = datetime.combine(day_date.date(), time(0, 0))
        depart_dt = depart_dt.replace(hour=depart_min // 60, minute=depart_min % 60)
        try:
            dur_sec, _dist = mapbox_directions_duration(
                coords[from_idx],
                coords[to_idx],
                token,
                depart_at=depart_dt,
                profile=profile,
            )
        except MapboxError:
            # Keep the existing matrix value on failure.
            continue
        # Re-apply parking buffer if destination was a central-London job
        if to_idx in parking_affected:
            dur_sec += parking_seconds
        new_seconds[from_idx][to_idx] = dur_sec

    return TravelMatrix(seconds=new_seconds, distance_metres=matrix.distance_metres)


def compute_leave_time(
    home_coord: tuple[float, float],
    site_coord: tuple[float, float],
    site_postcode: str,
    desired_arrival: datetime,
    token: str,
    parking_seconds: int = CENTRAL_LONDON_PARKING_BUFFER_SECONDS,
    max_iterations: int = 3,
) -> dict:
    """Reverse-calculate what time an engineer should leave home.

    Mapbox Directions supports `depart_at` but not `arrive_by`, so we iterate:
      1. First guess: leave 45 min + parking before desired arrival
      2. Query Mapbox Directions with that departure → get real drive
      3. Recompute: leave_at = desired_arrival − drive − parking
      4. Re-query with updated leave_at — London traffic differs by 15-min band
      5. Converges in ≤2 iterations

    Returns a dict with:
      - `leave_at`: datetime to leave home
      - `drive_minutes`: int
      - `parking_minutes`: int (15 if central London, else 0)
      - `arrive_at_door`: datetime (= desired_arrival)
      - `distance_metres`: int
    """
    from datetime import timedelta

    parking_sec = parking_seconds if is_central_london(site_postcode) else 0
    # First guess at drive time (60 min is a safe starting point for London).
    drive_sec = 60 * 60
    dist_m = 0
    leave_at = desired_arrival - timedelta(seconds=drive_sec + parking_sec)

    for _ in range(max_iterations):
        dur_sec, dist_m = mapbox_directions_duration(
            home_coord, site_coord, token, depart_at=leave_at
        )
        new_leave = desired_arrival - timedelta(seconds=dur_sec + parking_sec)
        # Converged if the adjustment is under a minute
        if abs((new_leave - leave_at).total_seconds()) < 60:
            leave_at = new_leave
            drive_sec = dur_sec
            break
        leave_at = new_leave
        drive_sec = dur_sec

    return {
        "leave_at": leave_at,
        "drive_minutes": int(round(drive_sec / 60)),
        "parking_minutes": parking_sec // 60,
        "arrive_at_door": desired_arrival,
        "distance_metres": dist_m,
    }


def mock_travel_matrix(coords: list[tuple[float, float]]) -> TravelMatrix:
    """Build a mock travel matrix from (lat, lng) pairs.

    Uses haversine × sinuosity factor for distance, and a fixed average UK
    driving speed for time. Good enough to validate the optimiser end-to-end
    before live-traffic Mapbox data is wired in.
    """
    n = len(coords)
    seconds: list[list[int]] = [[0] * n for _ in range(n)]
    distances: list[list[int]] = [[0] * n for _ in range(n)]
    speed_mps = (UK_AVG_KMH * 1000.0) / 3600.0

    for i in range(n):
        for j in range(n):
            if i == j:
                continue
            straight = haversine_metres(*coords[i], *coords[j])
            road_metres = straight * ROAD_SINUOSITY
            distances[i][j] = int(road_metres)
            seconds[i][j] = int(road_metres / speed_mps)

    return TravelMatrix(seconds=seconds, distance_metres=distances)
