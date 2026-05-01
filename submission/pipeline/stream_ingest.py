"""
Streaming Layer: Stage 3 micro-batch processing.
Poll the /data/stream/ directory for JSONL files and update stream_gold tables.
"""

import os
import logging
import time
from datetime import datetime
from pyspark.sql.functions import col, current_timestamp, lit, when, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType

from pipeline.config_loader import get_config, get_spark_session

logger = logging.getLogger(__name__)


def get_stream_schema():
    """Define schema for streaming JSONL files (same as batch transactions)."""
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("transaction_time", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("merchant_subcategory", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("location", StructType([
            StructField("province", StringType(), True),
            StructField("city", StringType(), True),
            StructField("coordinates", StringType(), True)
        ]), True),
        StructField("metadata", StructType([
            StructField("device_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("retry_flag", BooleanType(), True)
        ]), True)
    ])


def get_processed_files(checkpoint_path):
    """Read list of already processed files from checkpoint."""
    if not os.path.exists(checkpoint_path):
        return set()
    with open(checkpoint_path, 'r') as f:
        return set(line.strip() for line in f)


def save_processed_files(checkpoint_path, processed_set):
    """Save processed files list to checkpoint."""
    os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
    with open(checkpoint_path, 'w') as f:
        for filename in sorted(processed_set):
            f.write(filename + '\n')


def update_current_balances(spark, batch_df, output_path):
    """Upsert current_balances table - one row per account with latest balance."""
    from delta.tables import DeltaTable

    # Calculate balance change for each transaction
    balance_updates = batch_df.withColumn(
        "balance_change",
        when(col("transaction_type") == "DEBIT", -col("amount"))
        .when(col("transaction_type") == "CREDIT", col("amount"))
        .otherwise(0)
    )

    # Aggregate by account
    account_balances = balance_updates.groupBy("account_id").agg(
        sum("balance_change").alias("balance_delta"),
        max("transaction_timestamp").alias("last_transaction_timestamp")
    )

    # Get starting balances from batch gold layer
    try:
        dim_accounts = spark.read.format("delta").load(f"{output_path.replace('stream_gold', 'gold')}/dim_accounts")
        current_balances = dim_accounts.select("account_id", "current_balance")
    except:
        # If batch gold not available, start from zero
        current_balances = spark.createDataFrame([], "account_id string, current_balance decimal(18,2)")

    # Join with updates
    updated = current_balances.join(account_balances, "account_id", "full")
    updated = updated.withColumn(
        "new_balance",
        when(col("balance_delta").isNotNull(),
             col("current_balance") + col("balance_delta"))
        .otherwise(col("current_balance"))
    )
    updated = updated.withColumn("updated_at", current_timestamp())

    # Prepare final dataframe
    final_balances = updated.select(
        col("account_id"),
        col("new_balance").alias("current_balance"),
        col("last_transaction_timestamp"),
        col("updated_at")
    ).filter(col("account_id").isNotNull())

    # Write as Delta (overwrite entire table each time - simple approach)
    final_balances.write.format("delta").mode("overwrite").save(f"{output_path}/current_balances")


def update_recent_transactions(spark, batch_df, output_path):
    """Update recent_transactions table - keep only last 50 per account."""
    from delta.tables import DeltaTable

    full_path = f"{output_path}/recent_transactions"

    # Prepare batch data with updated_at
    batch_with_timestamp = batch_df.withColumn("updated_at", current_timestamp())

    # Try to read existing table
    try:
        existing = spark.read.format("delta").load(full_path)
        combined = existing.union(batch_with_timestamp)
    except:
        combined = batch_with_timestamp

    # Keep only last 50 per account
    window_spec = Window.partitionBy("account_id").orderBy(col("transaction_timestamp").desc())
    deduped = combined.withColumn("rank", row_number().over(window_spec))
    top_50 = deduped.filter(col("rank") <= 50).drop("rank")

    # Remove duplicates by (account_id, transaction_id)
    final = top_50.dropDuplicates(["account_id", "transaction_id"])

    # Write as Delta
    final.write.format("delta").mode("overwrite").save(full_path)


def main():
    """Main streaming ingestion function."""
    logger.info("=" * 50)
    logger.info("STREAMING LAYER: Starting micro-batch processing")
    logger.info("=" * 50)

    config = get_config()
    streaming_config = config.get('streaming', {})
    stream_input_path = streaming_config.get('stream_input_path', '/data/stream')
    stream_output_path = streaming_config.get('stream_gold_path', '/data/output/stream_gold')
    poll_interval = streaming_config.get('poll_interval_seconds', 10)

    checkpoint_path = "/data/output/stream_checkpoint.txt"

    spark = get_spark_session(app_name="nedbank-stream-ingest", config=config)

    try:
        processed_files = get_processed_files(checkpoint_path)
        logger.info(f"Already processed {len(processed_files)} files")

        # Poll for files (process all existing files once)
        all_files = sorted([f for f in os.listdir(stream_input_path) if f.endswith('.jsonl')])

        for filename in all_files:
            if filename in processed_files:
                logger.info(f"Skipping already processed: {filename}")
                continue

            logger.info(f"Processing: {filename}")
            file_path = os.path.join(stream_input_path, filename)

            # Read JSONL file
            df = spark.read.schema(get_stream_schema()).json(file_path)

            # Add processing timestamp and flatten
            df = df.withColumn("ingestion_timestamp", current_timestamp())
            df = df.withColumn("province", col("location.province"))
            df = df.withColumn("transaction_timestamp",
                               to_timestamp(concat(col("transaction_date"), lit(" "), col("transaction_time")),
                                            "yyyy-MM-dd HH:mm:ss"))
            df = df.withColumn("amount", col("amount").cast("decimal(18,2)"))
            df = df.withColumn("currency", lit("ZAR"))
            df = df.drop("location", "metadata")

            # Update streaming tables
            update_current_balances(spark, df, stream_output_path)
            update_recent_transactions(spark, df, stream_output_path)

            # Mark as processed
            processed_files.add(filename)
            save_processed_files(checkpoint_path, processed_files)
            logger.info(f"Completed: {filename}")

        logger.info("=" * 50)
        logger.info(f"STREAMING LAYER: Completed. Processed {len(all_files)} files")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"STREAMING LAYER FAILED: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()