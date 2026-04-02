"""Ramp-up load scenario: 0 → 100 users over 60s, hold 5 min, ramp down.

Run:
    locust -f tests/load/locustfile.py \
           --config tests/load/scenarios/ramp_up.py \
           --host http://localhost:8000

Expected SLOs at 100 users:
    - /api/v1/query p99 < 500ms
    - Error rate < 0.1%

Expected SLOs at 500 users:
    - /api/v1/query p99 < 2000ms
    - Error rate < 1%
"""
from __future__ import annotations

# Locust configuration — these are picked up when this file is used as --config

# Ramp: 0 → 100 users over 60s
spawn_rate = 100 / 60  # ~1.67 users/second

users = 100

# Run for 5 minutes after ramp-up
run_time = "7m"  # 60s ramp + 5min hold + 60s ramp-down

# Shape: linear ramp, flat hold, then ramp down
RAMP_UP_SECONDS = 60
HOLD_SECONDS = 300
RAMP_DOWN_SECONDS = 60

SLO_THRESHOLDS = {
    "users_100": {
        "p50_ms": 50,
        "p95_ms": 500,
        "p99_ms": 2000,
        "error_rate_pct": 0.1,
    },
    "users_500": {
        "p50_ms": 100,
        "p95_ms": 1000,
        "p99_ms": 5000,
        "error_rate_pct": 1.0,
    },
}

try:
    from locust import LoadTestShape

    class RampUpShape(LoadTestShape):
        """Linear ramp up, steady hold, ramp down."""

        stages = [
            {"duration": RAMP_UP_SECONDS, "users": 100, "spawn_rate": 2},
            {"duration": RAMP_UP_SECONDS + HOLD_SECONDS, "users": 100, "spawn_rate": 1},
            {"duration": RAMP_UP_SECONDS + HOLD_SECONDS + RAMP_DOWN_SECONDS, "users": 0, "spawn_rate": 5},
        ]

        def tick(self):
            run_time = self.get_run_time()
            for stage in self.stages:
                if run_time < stage["duration"]:
                    return stage["users"], stage["spawn_rate"]
            return None  # Stop

except ImportError:
    pass  # locust not installed
