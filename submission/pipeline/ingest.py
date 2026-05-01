"""
Bronze Layer: Raw data ingestion.
Reads source files and writes them as Delta tables with ingestion timestamp.
"""

import logging
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from pipeline.config_loader import get_config, get_spark_session

logger = logging.getLogger(__name__)


def read_transactions_with_schema(spark, file_path):
    """
    Read transactions.jsonl with a defined schema for better performance.
    This avoids Spark scanning the entire huge file to infer schema.
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType,
        BooleanType, StructType as NestedStruct
    )

    # Define the schema based on what we saw in the sample
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("transaction_date", StringType(), True),  # Will cast to date later
        StructField("transaction_time", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("merchant_category", StringType(), True),
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

    return spark.read.schema(schema).json(file_path)


def main():
    """Ingest raw data from source files to Bronze Delta tables."""
    logger.info("=" * 50)
    logger.info("BRONZE LAYER: Starting data ingestion")
    logger.info("=" * 50)

    # Load configuration
    config = get_config()
    input_paths = config['input']
    output_paths = config['output']

    logger.info(f"Input accounts: {input_paths['accounts_path']}")
    logger.info(f"Input customers: {input_paths['customers_path']}")
    logger.info(f"Input transactions: {input_paths['transactions_path']}")
    logger.info(f"Output bronze path: {output_paths['bronze_path']}")

    # Create Spark session
    spark = get_spark_session(app_name="nedbank-bronze-ingest", config=config)

    try:
        # 1. Read accounts.csv
        logger.info("Reading accounts.csv...")
        accounts_df = spark.read.option("header", "true").csv(input_paths['accounts_path'])
        accounts_df = accounts_df.withColumn("ingestion_timestamp", current_timestamp())

        # 2. Read customers.csv
        logger.info("Reading customers.csv...")
        customers_df = spark.read.option("header", "true").csv(input_paths['customers_path'])
        customers_df = customers_df.withColumn("ingestion_timestamp", current_timestamp())

        # 3. Read transactions.jsonl (large file - use schema for efficiency)
        logger.info("Reading transactions.jsonl (this may take a moment for the large file)...")
        transactions_df = read_transactions_with_schema(spark, input_paths['transactions_path'])
        transactions_df = transactions_df.withColumn("ingestion_timestamp", current_timestamp())

        # Log row counts
        logger.info(f"Accounts loaded: {accounts_df.count()} rows")
        logger.info(f"Customers loaded: {customers_df.count()} rows")
        logger.info(f"Transactions loaded: {transactions_df.count()} rows")

        # 4. Write to Bronze Delta tables (overwrite mode)
        logger.info("Writing accounts to Bronze...")
        accounts_df.write.format("delta").mode("overwrite").save(f"{output_paths['bronze_path']}/accounts")

        logger.info("Writing customers to Bronze...")
        customers_df.write.format("delta").mode("overwrite").save(f"{output_paths['bronze_path']}/customers")

        logger.info("Writing transactions to Bronze...")
        transactions_df.write.format("delta").mode("overwrite").save(f"{output_paths['bronze_path']}/transactions")

        logger.info("=" * 50)
        logger.info("BRONZE LAYER: Ingestion completed successfully")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"BRONZE LAYER FAILED: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()