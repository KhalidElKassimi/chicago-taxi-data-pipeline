import argparse

from pyspark.sql import SparkSession

from framework.config import load_pipeline_config
from framework.layer_transitions import transition_tech_to_func


def main() -> None:
    """Run the configuration-driven TECH-to-FUNC Spark step."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = load_pipeline_config(args.config)
    spark = (
        SparkSession.builder.appName("tech-to-func")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    try:
        output_path = transition_tech_to_func(spark, config)
        print("FUNC written to {}".format(output_path))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()