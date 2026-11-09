from pyspark.sql import DataFrame
import logging

logger = logging.getLogger("TelecomLoad")

def load_data(df: DataFrame, output_path: str, format_type: str = "parquet") -> None:
    """Writes transformed DataFrame to storage (Parquet/CSV)."""
    logger.info(f"Loading data into destination path: {output_path} as {format_type}")
    df.write \
        .mode("overwrite") \
        .format(format_type) \
        .save(output_path)
    logger.info("Data loading completed successfully.")