import argparse

from pyspark.sql import SparkSession

from framework.config import load_pipeline_config
from framework.layer_transitions import transition_bronze_to_raw


def main() -> None:
    """Run the configuration-driven Bronze-to-RAW Spark step."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = load_pipeline_config(args.config)
    spark = SparkSession.builder.appName("bronze-to-raw").getOrCreate()
    try:
        output_path = transition_bronze_to_raw(spark, config)
        print("RAW layer written to {}".format(output_path))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()