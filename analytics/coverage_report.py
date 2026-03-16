from storage.db import get_session
from sqlalchemy import text
from typing import Any

def generate_coverage_report() -> dict[str, Any]:
    """Return per-market, per-platform merchant counts."""
    session = get_session()
    report: dict[str, Any] = {}
    try:
        rows = session.execute(text(
            "SELECT market, platform, COUNT(*) AS cnt "
            "FROM platform_merchants GROUP BY market, platform ORDER BY market, platform"
        )).fetchall()
        for market, platform, cnt in rows:
            report.setdefault(market, {})[platform] = cnt
        return report
    finally:
        session.close()

def print_report(report: dict[str, Any]) -> None:
    for market, platforms in sorted(report.items()):
        print(f"Market: {market}")
        for platform, cnt in sorted(platforms.items()):
            print(f"  {platform}: {cnt} merchants")
