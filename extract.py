from pyspark.sql import SparkSession, DataFrame
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TelecomExtract")

def create_spark_session(app_name="TelecomPipeline") -> SparkSession:
    """Creates and returns a SparkSession."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def extract_data(spark: SparkSession, file_path: str) -> DataFrame:
    """Reads raw CSV telemetry data into a Spark DataFrame."""
    logger.info(f"Extracting data from path: {file_path}")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)
    logger.info(f"Extracted {df.count()} rows successfully.")
    return df