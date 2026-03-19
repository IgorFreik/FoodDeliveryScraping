"""
Pytest configuration and shared fixtures.

Ensures the project root is on sys.path so imports like `processing.parser`
work when running tests.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to path for all tests
_project_root = Path(__file__).resolve().parents[1]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
