"""UK postcode geocoding via postcodes.io.

Free, no API key, batch endpoint accepts up to 100 postcodes per call. If a full
postcode is missing from postcodes.io's database (e.g. very recently issued),
we fall back to the outcode (the bit before the space) and flag the result as
approximate.
"""
from __future__ import annotations

from dataclasses import dataclass

import httpx

POSTCODES_IO_BULK = "https://api.postcodes.io/postcodes"
POSTCODES_IO_OUTCODE = "https://api.postcodes.io/outcodes/{}"
TIMEOUT_SECONDS = 15
BATCH_SIZE = 100  # postcodes.io max per request


class GeocodeError(Exception):
    """Raised when geocoding fails (network, API down, etc.)."""


@dataclass(frozen=True)
class GeocodeResult:
    postcode: str        # the original query
    lat: float
    lng: float
    found: bool
    approximate: bool = False  # True if we resolved via outcode fallback


def _outcode_for(postcode: str) -> str:
    """Return the outward part of a postcode (e.g. 'KT15 4BU' → 'KT15')."""
    s = postcode.strip().upper()
    if " " in s:
        return s.split(" ", 1)[0]
    # No space: assume last 3 chars are the inward part
    return s[:-3] if len(s) > 3 else s


def _try_outcode(outcode: str) -> tuple[float, float] | None:
    if not outcode:
        return None
    try:
        resp = httpx.get(POSTCODES_IO_OUTCODE.format(outcode), timeout=TIMEOUT_SECONDS)
    except httpx.HTTPError:
        return None
    if resp.status_code != 200:
        return None
    try:
        payload = resp.json().get("result")
        if not payload:
            return None
        return float(payload["latitude"]), float(payload["longitude"])
    except (KeyError, ValueError, TypeError):
        return None


def geocode_postcodes(postcodes: list[str]) -> list[GeocodeResult]:
    """Batch-geocode UK postcodes. Returns one result per input, in order.

    For full postcodes that aren't in the database, falls back to the outcode
    (with `approximate=True`). Only returns `found=False` if even the outcode
    is unrecognised.
    Raises GeocodeError on network/HTTP errors.
    """
    if not postcodes:
        return []

    results: list[GeocodeResult] = []
    outcode_cache: dict[str, tuple[float, float] | None] = {}

    for batch_start in range(0, len(postcodes), BATCH_SIZE):
        batch = postcodes[batch_start : batch_start + BATCH_SIZE]
        try:
            resp = httpx.post(
                POSTCODES_IO_BULK,
                json={"postcodes": batch},
                timeout=TIMEOUT_SECONDS,
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise GeocodeError(f"Geocoding request failed: {exc}") from exc

        data = resp.json()
        if data.get("status") != 200:
            raise GeocodeError(f"Geocoding API returned status {data.get('status')}")

        for item in data["result"]:
            query = item["query"]
            payload = item["result"]

            if payload is not None:
                results.append(
                    GeocodeResult(
                        postcode=query,
                        lat=float(payload["latitude"]),
                        lng=float(payload["longitude"]),
                        found=True,
                        approximate=False,
                    )
                )
                continue

            # Full postcode missing — fall back to outcode
            outcode = _outcode_for(query)
            if outcode in outcode_cache:
                fallback = outcode_cache[outcode]
            else:
                fallback = _try_outcode(outcode)
                outcode_cache[outcode] = fallback

            if fallback is not None:
                lat, lng = fallback
                results.append(
                    GeocodeResult(
                        postcode=query,
                        lat=lat,
                        lng=lng,
                        found=True,
                        approximate=True,
                    )
                )
            else:
                results.append(
                    GeocodeResult(
                        postcode=query, lat=0.0, lng=0.0, found=False
                    )
                )

    return results
