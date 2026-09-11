import argparse

from pyspark.sql import SparkSession

from framework.config import load_pipeline_config
from framework.layer_transitions import transition_raw_to_tech


def main() -> None:
    """Create TECH and automatically generated REJECT outputs from RAW."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = load_pipeline_config(args.config)
    spark = (
        SparkSession.builder.appName("raw-to-tech")
        .config("spark.sql.codegen.wholeStage", "false")
        .getOrCreate()
    )
    try:
        tech_path, reject_path = transition_raw_to_tech(spark, config)
        print("TECH written to {}".format(tech_path))
        print("REJECT written to {}".format(reject_path))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()