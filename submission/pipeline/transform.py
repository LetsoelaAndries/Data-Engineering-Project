"""
Silver Layer: Data transformation and quality.
"""

import logging
from pyspark.sql.functions import (
    col, current_timestamp, when, row_number, lit, 
    to_date, to_timestamp, concat, datediff, floor
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, IntegerType

from pipeline.config_loader import get_config, get_spark_session, get_dq_rules

logger = logging.getLogger(__name__)


def add_dq_flags(transactions_df, accounts_df, dq_rules):
    """Add data quality flags to transactions."""
    df = transactions_df
    
    # Ensure dq_flag exists
    if 'dq_flag' not in df.columns:
        df = df.withColumn("dq_flag", lit(None))
    
    # Null checks
    null_checks = dq_rules.get('null_checks', {}).get('fact_transactions', [])
    for field in null_checks:
        df = df.withColumn(
            "dq_flag",
            when(
                col(field).isNull() & col("dq_flag").isNull(),
                lit("NULL_REQUIRED")
            ).otherwise(col("dq_flag"))
        )
    
    # Domain checks
    domain_checks = dq_rules.get('domain_checks', {})
    
    if 'transaction_type' in domain_checks:
        allowed = domain_checks['transaction_type'].get('allowed', [])
        df = df.withColumn(
            "dq_flag",
            when(
                ~col("transaction_type").isin(allowed) & col("dq_flag").isNull(),
                lit("TYPE_MISMATCH")
            ).otherwise(col("dq_flag"))
        )
    
    if 'channel' in domain_checks:
        allowed = domain_checks['channel'].get('allowed', [])
        df = df.withColumn(
            "dq_flag",
            when(
                ~col("channel").isin(allowed) & col("dq_flag").isNull(),
                lit("TYPE_MISMATCH")
            ).otherwise(col("dq_flag"))
        )
    
    if 'currency' in domain_checks:
        allowed = domain_checks['currency'].get('allowed', [])
        df = df.withColumn(
            "dq_flag",
            when(
                ~col("currency").isin(allowed) & col("dq_flag").isNull(),
                lit("CURRENCY_VARIANT")
            ).otherwise(col("dq_flag"))
        )
    
    return df


def main():
    """Transform Bronze to Silver."""
    logger.info("=" * 50)
    logger.info("SILVER LAYER: Starting transformation")
    logger.info("=" * 50)
    
    config = get_config()
    output_paths = config['output']
    bronze_path = output_paths['bronze_path']
    silver_path = output_paths['silver_path']
    
    dq_rules = get_dq_rules(config)
    spark = get_spark_session(app_name="nedbank-silver-transform", config=config)
    
    try:
        # Read from Bronze
        accounts_df = spark.read.format("delta").load(f"{bronze_path}/accounts")
        customers_df = spark.read.format("delta").load(f"{bronze_path}/customers")
        transactions_df = spark.read.format("delta").load(f"{bronze_path}/transactions")
        
        # Deduplicate transactions
        window_spec = Window.partitionBy("transaction_id").orderBy("ingestion_timestamp")
        transactions_df = transactions_df.withColumn("row_num", row_number().over(window_spec))
        transactions_df = transactions_df.withColumn("dq_flag", lit(None))
        transactions_df = transactions_df.withColumn(
            "dq_flag",
            when(col("row_num") > 1, lit("DUPLICATE_DEDUPED")).otherwise(col("dq_flag"))
        )
        transactions_df = transactions_df.filter(col("row_num") == 1).drop("row_num")
        
        # Type casting for accounts
        accounts_df = accounts_df.withColumn("credit_limit", col("credit_limit").cast(DecimalType(18, 2)))
        accounts_df = accounts_df.withColumn("current_balance", col("current_balance").cast(DecimalType(18, 2)))
        accounts_df = accounts_df.withColumn("open_date", to_date(col("open_date"), "yyyy-MM-dd"))
        accounts_df = accounts_df.withColumn("last_activity_date", to_date(col("last_activity_date"), "yyyy-MM-dd"))
        
        # Customers with age_band
        customers_df = customers_df.withColumn("risk_score", col("risk_score").cast(IntegerType()))
        customers_df = customers_df.withColumn("dob", to_date(col("dob"), "yyyy-MM-dd"))
        
        current_date = spark.sql("SELECT CURRENT_DATE()").collect()[0][0]
        customers_df = customers_df.withColumn(
            "age", floor(datediff(lit(current_date), col("dob")) / 365.25)
        )
        customers_df = customers_df.withColumn(
            "age_band",
            when(col("age") >= 65, "65+")
            .when(col("age") >= 56, "56-65")
            .when(col("age") >= 46, "46-55")
            .when(col("age") >= 36, "36-45")
            .when(col("age") >= 26, "26-35")
            .when(col("age") >= 18, "18-25")
            .otherwise(lit(None))
        ).drop("age")
        
        # Flatten transactions
        transactions_df = transactions_df.withColumn("province", col("location.province"))
        transactions_df = transactions_df.withColumn("city", col("location.city"))
        transactions_df = transactions_df.withColumn("coordinates", col("location.coordinates"))
        transactions_df = transactions_df.withColumn("device_id", col("metadata.device_id"))
        transactions_df = transactions_df.withColumn("session_id", col("metadata.session_id"))
        transactions_df = transactions_df.withColumn("retry_flag", col("metadata.retry_flag"))
        transactions_df = transactions_df.drop("location", "metadata")
        
        # Create timestamp and cast types
        transactions_df = transactions_df.withColumn(
            "transaction_timestamp",
            to_timestamp(concat(col("transaction_date"), lit(" "), col("transaction_time")), "yyyy-MM-dd HH:mm:ss")
        )
        transactions_df = transactions_df.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
        transactions_df = transactions_df.withColumn("amount", col("amount").cast(DecimalType(18, 2)))
        transactions_df = transactions_df.withColumn("currency", lit("ZAR"))
        
        # Apply DQ flags
        transactions_df = add_dq_flags(transactions_df, accounts_df, dq_rules)
        
        # Write to Silver
        accounts_df.write.format("delta").mode("overwrite").save(f"{silver_path}/accounts")
        customers_df.write.format("delta").mode("overwrite").save(f"{silver_path}/customers")
        transactions_df.write.format("delta").mode("overwrite").save(f"{silver_path}/transactions")
        
        logger.info("SILVER LAYER: Completed successfully")
        
    except Exception as e:
        logger.error(f"SILVER LAYER FAILED: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
    