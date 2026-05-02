# Nedbank DE Challenge Pipeline

## How to run:
docker build -t my-submission:test .
docker run --rm -v /path/to/data:/data my-submission:test

## Pipeline:
- Bronze: Raw ingestion
- Silver: Data quality
- Gold: Final output
- Streaming: Micro-batch
