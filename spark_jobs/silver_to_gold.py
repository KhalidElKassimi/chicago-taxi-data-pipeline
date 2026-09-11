import argparse

from pyspark.sql import SparkSession

from framework.config import load_pipeline_config
from framework.layer_transitions import transition_silver_to_gold


def main() -> None:
    """Run the configuration-driven Silver-to-Gold Spark step."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = load_pipeline_config(args.config)
    spark = (
        SparkSession.builder.appName("silver-to-gold")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    try:
        output_path = transition_silver_to_gold(spark, config)
        print("GOLD written to {}".format(output_path))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()