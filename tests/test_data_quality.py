from unittest.mock import MagicMock, patch

from analytics.data_quality import get_dq_metrics


@patch("analytics.data_quality.get_session")
def test_get_dq_metrics_empty(mock_get_session):
    mock_session = MagicMock()
    # Mock total count returning 0
    mock_session.execute.return_value.scalar.return_value = 0
    mock_get_session.return_value = mock_session

    metrics = get_dq_metrics()

    assert metrics["total_merchants"] == 0
    assert metrics["overall_health"] == 0.0


@patch("analytics.data_quality.get_session")
def test_get_dq_metrics_perfect_health(mock_get_session):
    mock_session = MagicMock()
    # return values for the 4 queries in order: total, missing_addr, missing_cuis, stale, invalid_geom
    scalars = [100, 0, 0, 0, 0]

    def mock_scalar():
        return scalars.pop(0) if scalars else 0

    mock_exec = MagicMock()
    mock_exec.scalar.side_effect = mock_scalar
    mock_session.execute.return_value = mock_exec
    mock_get_session.return_value = mock_session

    metrics = get_dq_metrics()

    assert metrics["total_merchants"] == 100
    assert metrics["completeness_score"] == 100.0
    assert metrics["freshness_score"] == 100.0
    assert metrics["geo_accuracy_score"] == 100.0
    assert metrics["overall_health"] == 100.0
