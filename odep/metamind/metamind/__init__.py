"""
MetaMind Enterprise Query Intelligence Platform v4.0

An adaptive, metadata-driven query intelligence platform that operates as a
control-plane optimization layer above heterogeneous execution engines.
"""

from __future__ import annotations

__version__ = "4.0.0"
__author__ = "MetaMind Team"
__license__ = "Apache-2.0"

from metamind.bootstrap import AppContext, bootstrap

__all__ = ["AppContext", "bootstrap", "__version__"]
