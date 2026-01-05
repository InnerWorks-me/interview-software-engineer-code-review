from app.metrics import ingest_metrics


if __name__ == "__main__":
    print(ingest_metrics('{"project_id": "foobar", "metrics": {}}'))

    # print(ingest_metrics(""))