import sys
import os

os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-17'        #  <----- 🔴JAVA PATH🔴
os.environ['HADOOP_HOME'] = r'C:\hadoop\bin'
# Add parent directory to path so modules can be imported when run from root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.extract import create_spark_session, extract_data
from src.transform import transform_data, compute_business_kpis
from src.load import load_data
import logging

logger = logging.getLogger("TelecomMainPipeline")

def run_pipeline():
    spark = create_spark_session()

    input_file = "raw_network_telemetry.csv"
    output_telemetry = "data/processed/telemetry_enriched"
    output_kpis = "data/processed/region_kpis"

    # 1. Extract
    raw_df = extract_data(spark, input_file)

    # 2. Transform
    transformed_df = transform_data(raw_df)
    kpi_df = compute_business_kpis(transformed_df)

    # Show samples for Data Analysis preview
    print("--- Sample Transformed Telemetry Data ---")
    transformed_df.select("device_id", "region", "packet_loss_pct", "latency_ms", "network_status").show(5, truncate=False)

    print("--- Business KPI Summary by Region ---")
    kpi_df.show(10, truncate=False)

    # 3. Load
    load_data(transformed_df, output_telemetry, format_type="parquet")
    load_data(kpi_df, output_kpis, format_type="parquet")

    spark.stop()
    logger.info("Pipeline execution completed successfully.")

if __name__ == "__main__":
    run_pipeline()