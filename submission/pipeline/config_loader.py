"""
Shared configuration loader and Spark session builder for the DE pipeline.
"""

import os
import yaml
from pyspark.sql import SparkSession


def get_config():
    """
    Load pipeline configuration from the injected config file.

    Returns:
        dict: Configuration dictionary
    """
    config_path = os.environ.get(
        "PIPELINE_CONFIG",
        "/data/config/pipeline_config.yaml"
    )

    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Configuration file not found at {config_path}. "
            f"Check that the scoring system mounted the config file correctly."
        )

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    return config


def get_spark_session(app_name=None, config=None):
    """
    Create or get a Spark session configured for the challenge constraints.

    Args:
        app_name: Optional app name (overrides config)
        config: Optional config dict (loads fresh if not provided)

    Returns:
        SparkSession: Configured Spark session
    """
    if config is None:
        config = get_config()

    spark_config = config.get('spark', {})
    app_name = app_name or spark_config.get('app_name', 'nedbank-de-pipeline')
    master = spark_config.get('master', 'local[2]')

    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1")

    # Set local directory for temporary files (respects 512MB tmpfs limit)
    builder = builder.config("spark.local.dir", "/tmp/spark-temp")

    # Memory optimization for 2GB constraint
    builder = builder.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark


def get_dq_rules(config=None):
    """
    Load data quality rules from the DQ config file.

    Returns:
        dict: DQ rules dictionary
    """
    if config is None:
        config = get_config()

    dq_config = config.get('dq', {})
    rules_path = dq_config.get('rules_path', '/data/config/dq_rules.yaml')

    if not os.path.exists(rules_path):
        raise FileNotFoundError(
            f"DQ rules file not found at {rules_path}. "
            f"This file is required from Stage 2 onward."
        )

    with open(rules_path, 'r') as f:
        dq_rules = yaml.safe_load(f)

    return dq_rules