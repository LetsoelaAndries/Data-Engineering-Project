"""
Gold Layer: Final output tables.
Creates dim_accounts, dim_customers, and fact_transactions with surrogate keys.
"""

import logging
from pyspark.sql.functions import (
    col, sha2, conv, lit, when, coalesce
)
from pyspark.sql.types import LongType

from pipeline.config_loader import get_config, get_spark_session

logger = logging.getLogger(__name__)


def create_surrogate_key(natural_key_column):
    """
    Create a deterministic BIGINT surrogate key from a natural key string.
    Uses sha256 hash converted to BIGINT (taking first 16 hex chars / 8 bytes).
    """
    # Step 1: Create SHA256 hash (returns 64 hex chars)
    hash_hex = sha2(col(natural_key_column), 256)
    # Step 2: Take first 16 characters (8 bytes worth) and convert from base 16 to base 10
    # This gives us a number that fits in BIGINT range
    hash_subset = conv(hash_hex.substr(1, 16), 16, 10)
    # Step 3: Cast to Long and ensure positive
    return hash_subset.cast(LongType())


def main():
    """Transform Silver data to Gold layer with surrogate keys."""
    logger.info("=" * 50)
    logger.info("GOLD LAYER: Starting final provisioning")
    logger.info("=" * 50)

    # Load configuration
    config = get_config()
    output_paths = config['output']
    silver_path = output_paths['silver_path']
    gold_path = output_paths['gold_path']

    logger.info(f"Silver input path: {silver_path}")
    logger.info(f"Gold output path: {gold_path}")

    # Create Spark session
    spark = get_spark_session(app_name="nedbank-gold-provision", config=config)

    try:
        # 1. Read from Silver
        logger.info("Reading accounts from Silver...")
        silver_accounts = spark.read.format("delta").load(f"{silver_path}/accounts")

        logger.info("Reading customers from Silver...")
        silver_customers = spark.read.format("delta").load(f"{silver_path}/customers")

        logger.info("Reading transactions from Silver...")
        silver_transactions = spark.read.format("delta").load(f"{silver_path}/transactions")

        # ============================================================
        # 2. Build dim_accounts (11 fields)
        # ============================================================
        logger.info("Building dim_accounts...")

        dim_accounts = silver_accounts.select(
            # Field 1: account_sk (surrogate key)
            create_surrogate_key("account_id").alias("account_sk"),
            # Field 2: account_id (natural key)
            col("account_id"),
            # Field 3: customer_id (renamed from customer_ref - required for Validation Query 2)
            col("customer_ref").alias("customer_id"),
            # Field 4: account_type
            col("account_type"),
            # Field 5: account_status
            col("account_status"),
            # Field 6: open_date
            col("open_date"),
            # Field 7: product_tier
            col("product_tier"),
            # Field 8: digital_channel
            col("digital_channel"),
            # Field 9: credit_limit (nullable for non-CREDIT accounts)
            col("credit_limit"),
            # Field 10: current_balance
            col("current_balance"),
            # Field 11: last_activity_date
            col("last_activity_date")
        ).dropDuplicates(["account_id"])  # Ensure one row per account

        logger.info(f"dim_accounts created with {dim_accounts.count()} rows")

        # ============================================================
        # 3. Build dim_customers (9 fields)
        # ============================================================
        logger.info("Building dim_customers...")

        dim_customers = silver_customers.select(
            # Field 1: customer_sk (surrogate key)
            create_surrogate_key("customer_id").alias("customer_sk"),
            # Field 2: customer_id (natural key)
            col("customer_id"),
            # Field 3: gender
            col("gender"),
            # Field 4: province
            col("province"),
            # Field 5: income_band
            col("income_band"),
            # Field 6: segment
            col("segment"),
            # Field 7: risk_score
            col("risk_score"),
            # Field 8: kyc_status
            col("kyc_status"),
            # Field 9: age_band (derived in Silver)
            col("age_band")
        ).dropDuplicates(["customer_id"])  # Ensure one row per customer

        logger.info(f"dim_customers created with {dim_customers.count()} rows")

        # ============================================================
        # 4. Build fact_transactions (15 fields)
        # ============================================================
        logger.info("Building fact_transactions...")

        # First, join transactions with accounts to get customer_id
        tx_with_accounts = silver_transactions.join(
            silver_accounts.select("account_id", "customer_ref"),
            on="account_id",
            how="left"
        )

        # Then join with customers to get customer_sk
        tx_with_customers = tx_with_accounts.join(
            dim_customers.select("customer_id", "customer_sk"),
            tx_with_accounts["customer_ref"] == dim_customers["customer_id"],
            how="left"
        )

        # Now build fact_transactions with all required fields
        fact_transactions = tx_with_customers.select(
            # Field 1: transaction_sk (surrogate key)
            create_surrogate_key("transaction_id").alias("transaction_sk"),
            # Field 2: transaction_id
            col("transaction_id"),
            # Field 3: account_sk (from dim_accounts)
            create_surrogate_key("account_id").alias("account_sk"),
            # Field 4: customer_sk
            col("customer_sk"),
            # Field 5: transaction_date
            col("transaction_date"),
            # Field 6: transaction_timestamp
            col("transaction_timestamp"),
            # Field 7: transaction_type
            col("transaction_type"),
            # Field 8: merchant_category
            col("merchant_category"),
            # Field 9: merchant_subcategory (nullable - not in Stage 1 source)
            lit(None).alias("merchant_subcategory"),
            # Field 10: amount
            col("amount"),
            # Field 11: currency (already standardized to ZAR in Silver)
            col("currency"),
            # Field 12: channel
            col("channel"),
            # Field 13: province (flattened from location in Silver)
            col("province"),
            # Field 14: dq_flag
            col("dq_flag"),
            # Field 15: ingestion_timestamp
            col("ingestion_timestamp")
        )

        # Drop any rows where customer_sk is null (orphaned transactions)
        orphaned_count = fact_transactions.filter(col("customer_sk").isNull()).count()
        if orphaned_count > 0:
            logger.warning(f"Found {orphaned_count} transactions with no matching customer")

        # Keep only transactions with valid customer_sk
        fact_transactions = fact_transactions.filter(col("customer_sk").isNotNull())

        logger.info(f"fact_transactions created with {fact_transactions.count()} rows")

        # ============================================================
        # 5. Write to Gold Delta tables
        # ============================================================
        logger.info("Writing dim_accounts to Gold...")
        dim_accounts.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_accounts")

        logger.info("Writing dim_customers to Gold...")
        dim_customers.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_customers")

        logger.info("Writing fact_transactions to Gold...")
        fact_transactions.write.format("delta").mode("overwrite").save(f"{gold_path}/fact_transactions")

        # Log final statistics
        logger.info("=" * 50)
        logger.info("GOLD LAYER: Final Statistics")
        logger.info("=" * 50)
        logger.info(f"dim_accounts: {dim_accounts.count()} unique accounts")
        logger.info(f"dim_customers: {dim_customers.count()} unique customers")
        logger.info(f"fact_transactions: {fact_transactions.count()} transactions")

        # DQ summary from fact_transactions
        dq_summary = fact_transactions.groupBy("dq_flag").count().collect()
        logger.info("DQ Flag Summary:")
        for row in dq_summary:
            flag = row["dq_flag"] if row["dq_flag"] is not None else "CLEAN"
            logger.info(f"  {flag}: {row['count']} transactions")

        logger.info("=" * 50)
        logger.info("GOLD LAYER: Provisioning completed successfully")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"GOLD LAYER FAILED: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()