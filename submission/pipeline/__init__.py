"""
Nedbank Data Engineering Challenge Pipeline

This package contains the medallion architecture pipeline:
- ingest.py: Bronze layer (raw data ingestion)
- transform.py: Silver layer (deduplication, DQ flags, type casting)
- provision.py: Gold layer (joins, aggregations, final output)
- stream_ingest.py: Stage 3 streaming ingestion
- run_all.py: Main orchestrator
"""