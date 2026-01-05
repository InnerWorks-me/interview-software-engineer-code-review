import pytest
from app.metrics import ingest_metrics

def test_ingest_metrics():
    with pytest.raises(NotImplementedError):
        ingest_metrics('{"project_id": "abc123", "metrics": {}}')
