import pytest
from app.metrics import ingest_metrics

@pytest.mark.asyncio
async def test_ingest_metrics():
    with pytest.raises(NotImplementedError):
        await ingest_metrics('{"project_id": "abc123", "metrics": {}}')
