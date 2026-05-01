# Architecture Decision Record: Stage 3 Streaming Extension

**Author:** Letsoela Andries Sello
**Date:** 30 April 2026
**Status:** Final

## Context

In Stage 3, the mobile product team needed a way to show customers their current account balance and recent transactions in near real-time. The daily batch pipeline was too slow because it only ran once per day.

The new streaming data came as 12 JSONL files placed in the `/data/stream/` folder. Each file contains between 50 and 500 transaction events. The pipeline had to:
- Poll the folder and process files in order (by filename)
- Update `current_balances` table (one row per account, overwrite with latest balance)
- Update `recent_transactions` table (only keep last 50 transactions per account)
- The SLA required that `updated_at` timestamps be within 5 minutes of the source event time

Before Stage 3, my pipeline had about 500 lines of code split across `ingest.py`, `transform.py`, `provision.py`, `config_loader.py`, and `run_all.py`. The batch pipeline was working correctly for Stages 1 and 2.

## Decision 1: How did my Stage 1 architecture help or hurt the streaming extension?

**What helped:** My `config_loader.py` made adding new paths very easy. I already had a `get_config()` function that read from `pipeline_config.yaml`, so adding streaming input and output paths was just a matter of uncommenting a few lines in the YAML file. Also, I used Delta MERGE in Stage 2 for upserting data, and I could reuse the exact same pattern for the streaming tables. This saved me a lot of time.

**What hurt:** My `run_all.py` was written assuming only batch processing. When I added streaming, I had to put a conditional check to see if the streaming folder exists. This made the code a bit harder to read. Also, my Spark session was set up for batch jobs, and I wasn't sure if it would work correctly for a long-running polling loop.

**Code survival rate:** About 90% of my Stage 1 and Stage 2 code stayed the same. I didn't change `ingest.py`, `transform.py`, or `provision.py`. I only modified `run_all.py` to call streaming after batch, and I added `stream_ingest.py` as a new file (about 60 lines). The batch pipeline still runs exactly as it did before.

## Decision 2: What would I change about my Stage 1 design?

I would change how `run_all.py` decides to run streaming. Right now, it checks if the streaming folder exists. This is not reliable because the folder might exist even in Stage 1 or Stage 2.

**I would have added a command line argument instead.** For example: `--mode batch` or `--mode both`. This would make the execution path clear and predictable. The Docker CMD could then pass the correct argument based on the stage.

I would also create a shared `upsert_table()` function. In `provision.py`, I wrote custom merge code for `fact_transactions`. When I needed the same logic for `current_balances` in Stage 3, I copied the code into `stream_ingest.py`. This created duplicate code. A shared utility function in `config_loader.py` would have been cleaner.

## Decision 3: How would I build this differently if I knew Stage 3 was coming?

If I had known about Stage 3 from Day 1, I would have designed the pipeline with two separate entry points: one for batch and one for streaming. Both would share common library code from a `utils/` folder. The scoring system expects a single command, but `run_all.py` could just call both.

For state management, the Delta MERGE pattern worked well, so I would keep that. But I would have written the merge logic as a reusable function from the start.

For output structure, I would keep batch and streaming outputs separate (the way the spec requires). Mixing them would make SLA measurement confusing.

The biggest change would be to design `stream_ingest.py` to be idempotent from Day 1. I would track which files have been processed using a small checkpoint file saved to `/data/output/stream_checkpoint.json`. This would allow the pipeline to restart safely if interrupted.

## Appendix

No diagrams. My main lesson is: always plan for extension. Configuration-driven design saved me, but hardcoded entry point logic and duplicate code cost me time.