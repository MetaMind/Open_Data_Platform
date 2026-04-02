"""Burst load scenario: 500 users for 30 seconds.

Expected:
    - No 5xx errors
    - Queue depth < 200 at peak
    - System recovers to baseline within 60s post-burst

Run:
    locust -f tests/load/locustfile.py \
           --config tests/load/scenarios/burst.py \
           --host http://localhost:8000
"""
from __future__ import annotations

BURST_USERS = 500
BURST_DURATION_SECONDS = 30
COOLDOWN_SECONDS = 60

SLO_THRESHOLDS = {
    "burst": {
        "error_rate_5xx_pct": 0.0,      # No 5xx errors
        "max_queue_depth": 200,
        "p99_ms": 10_000,               # 10s max during burst
    },
    "recovery": {
        "p99_ms": 2_000,                # Should recover to <2s within cooldown
    },
}

try:
    from locust import LoadTestShape

    class BurstShape(LoadTestShape):
        """Instantaneous burst: max users for 30s, then rapid ramp-down."""

        def tick(self):
            run_time = self.get_run_time()
            if run_time < 5:
                # Ramp up instantly (5s)
                return BURST_USERS, 100
            if run_time < 5 + BURST_DURATION_SECONDS:
                # Hold at burst level
                return BURST_USERS, 10
            if run_time < 5 + BURST_DURATION_SECONDS + COOLDOWN_SECONDS:
                # Cooldown
                return 0, 50
            return None  # Done

except ImportError:
    pass  # locust not installed
