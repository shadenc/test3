"""
OS CSPRNG-backed values for scraper humanization (mouse jitter, sleeps).
Uses only the secrets module so static analysis does not flag PRNG misuse (Sonar S2245).
Not intended for deriving cryptographic keys or session tokens.
"""

import secrets


def stealth_randint(low: int, high: int) -> int:
    """Inclusive integer in [low, high]."""
    if high < low:
        return low
    return low + secrets.randbelow(high - low + 1)


def stealth_uniform(low: float, high: float) -> float:
    """Float in [low, high); sufficient for asyncio.sleep delays."""
    if high <= low:
        return float(low)
    return low + (high - low) * (secrets.randbits(53) / (1 << 53))
