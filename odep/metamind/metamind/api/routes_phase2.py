"""Legacy Phase 2 routes placeholder.

This module has been quarantined because it depends on legacy bootstrap
contracts (`Bootstrap`, `get_bootstrap`) that no longer exist in the active
runtime architecture.

Original implementation archived at:
  metamind/api/legacy/routes_phase2.py
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)
logger.warning(
    "metamind.api.routes_phase2 is deprecated and quarantined; "
    "see metamind/api/legacy/routes_phase2.py"
)
