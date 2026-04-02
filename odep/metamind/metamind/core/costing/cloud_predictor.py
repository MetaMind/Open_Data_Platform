"""F23 Cloud cost predictor."""
from __future__ import annotations
import logging
logger = logging.getLogger(__name__)
class CloudCostPredictor:
    _COST_PER_TB = {"bigquery": 5.0, "snowflake": 3.0, "redshift": 1.0, "athena": 5.0}
    def predict(self, backend: str, bytes_scanned: int) -> float:
        tb = bytes_scanned / (1024**4)
        return tb * self._COST_PER_TB.get(backend, 0.0)
