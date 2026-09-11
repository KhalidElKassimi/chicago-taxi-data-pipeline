"""Reusable transformations between medallion data layers."""

from datetime import datetime, timezone
from functools import reduce
from typing import Any, Dict, List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    current_timestamp,
    expr,
    input_file_name,
    lead,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    sha2,
    sum as spark_sum,
    trim,
    when,
)
from pyspark.sql.window import Window


def build_raw_storage_paths(config: Dict[str, Any]) -> Tuple[str, str]:
    """Build Bronze and RAW Silver base paths from the pipeline configuration.
    Uses the configured MinIO bucket and layer prefixes.
    Returns the base input and output paths used by Spark.
    """
    bronze = config["bronze"]
    raw = config["silver"]["raw"]
    return (
        "s3a://{}/{}".format(bronze["bucket"], bronze["prefix"]),
        "s3a://{}/{}".format(raw["bucket"], raw["prefix"]),
    )


def _partition_path(base_path: str, partition_name: str, partition_value: str) -> str:
    """Build a single partition path under a Spark dataset."""
    return "{}/{}={}".format(base_path.rstrip("/"), partition_name, partition_value)


def latest_partition_value(
    spark: SparkSession, base_path: str, partition_name: str
) -> str:
    """Return the latest partition value under an S3A path.
    Values are compared lexicographically, matching timestamp folder names.
    Raises ValueError when no matching partition exists.
    """
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(base_path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not filesystem.exists(hadoop_path):
        raise ValueError("Path does not exist: {}".format(base_path))
    prefix = "{}=".format(partition_name)
    values = []
    for status in filesystem.listStatus(hadoop_path):
        name = status.getPath().getName()
        if status.isDirectory() and name.startswith(prefix):
            values.append(name[len(prefix) :])
    if not values:
        raise ValueError(
            "No {} partition found under {}".format(partition_name, base_path)
        )
    return sorted(values)[-1]


def _monitoring_path(config: Dict[str, Any]) -> str:
    """Build the configured monitoring path, defaulting under the Bronze bucket."""
    monitoring = config.get("monitoring", {})
    bucket = monitoring.get("bucket", config["bronze"]["bucket"])
    prefix = monitoring.get(
        "prefix", "{}/monitoring".format(config["pipeline"]["name"])
    )
    return "s3a://{}/{}".format(bucket, prefix)


def write_monitoring_event(
    spark: SparkSession, config: Dict[str, Any], event: Dict[str, Any]
) -> None:
    """Append one layer monitoring event under a normalized folder path."""
    event_data = {
        "pipeline": config["pipeline"]["name"],
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "layer": event["layer"],
        "dataset_name": event.get("dataset_name", ""),
        "dataset_type": event.get("dataset_type", ""),
        "status": event.get("status", "success"),
        "batch_id": event.get("batch_id", ""),
        "input_path": event.get("input_path", ""),
        "output_path": event.get("output_path", ""),
        "row_count": int(event.get("row_count", 0)),
        "rejected_count": int(event.get("rejected_count", 0)),
        "reject_path": event.get("reject_path", ""),
        "write_mode": event.get("write_mode", ""),
        "dedup_strategy": event.get("dedup_strategy", ""),
        "error_type": event.get("error_type", ""),
        "error_message": event.get("error_message", ""),
    }
    output_path = "{}/{}".format(_monitoring_path(config).rstrip("/"), event_data["layer"])
    spark.createDataFrame([event_data]).coalesce(1).write.mode("append").parquet(
        output_path
    )


def write_gold_monitoring_event(
    spark: SparkSession, config: Dict[str, Any], event: Dict[str, Any]
) -> None:
    """Append one table-level Gold monitoring event under a normalized path."""
    event_data = {
        "pipeline": config["pipeline"]["name"],
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "layer": "gold",
        "status": event.get("status", "success"),
        "input_path": event.get("input_path", ""),
        "output_path": event.get("output_path", ""),
        "row_count": int(event.get("row_count", 0)),
        "error_type": event.get("error_type", ""),
        "error_message": event.get("error_message", ""),
        "dataset_type": event["dataset_type"],
        "dataset_name": event["dataset_name"],
    }
    dataset_type_path = "dimensions" if event_data["dataset_type"] == "dimension" else "facts"
    output_path = "{}/gold/{}/{}".format(
        _monitoring_path(config).rstrip("/"), dataset_type_path, event_data["dataset_name"]
    )
    spark.createDataFrame([event_data]).coalesce(1).write.mode("append").parquet(
        output_path
    )


def read_bronze_as_raw(spark: SparkSession, input_path: str) -> DataFrame:
    """Read Bronze JSONL records without applying business transformations.
    Adds the source file and RAW load timestamp as technical metadata.
    Returns a Spark DataFrame ready for RAW Silver persistence.
    """
    return (
        spark.read.option("primitivesAsString", "true")
        .json(input_path)
        .withColumn("_source_file", input_file_name())
        .withColumn("_raw_loaded_at", current_timestamp())
    )


def write_raw_silver(raw_frame: DataFrame, output_path: str) -> None:
    """Write the uncast RAW Silver DataFrame as Parquet.
    New RAW batches are appended to the configured output path.
    The function does not perform quality checks or reject rows.
    """
    raw_frame.write.mode("append").parquet(output_path)


def transition_bronze_to_raw(spark, config):
    """Coordinate the Bronze JSONL to RAW transition.
    Reads paths from YAML, loads records and writes Parquet to MinIO.
    Returns the RAW Silver output path.
    """
    bronze_base_path, raw_base_path = build_raw_storage_paths(config)
    batch_id = latest_partition_value(spark, bronze_base_path, "ingestion_timestamp")
    input_path = "{}/data.jsonl".format(
        _partition_path(bronze_base_path, "ingestion_timestamp", batch_id)
    )
    output_path = _partition_path(raw_base_path, "ingestion_timestamp", batch_id)
    write_monitoring_event(
        spark,
        config,
        {
            "layer": "raw",
            "status": "started",
            "batch_id": batch_id,
            "input_path": input_path,
            "output_path": output_path,
        },
    )
    try:
        raw_frame = read_bronze_as_raw(spark, input_path)
        row_count = raw_frame.count()
        status = "success" if row_count else "no_data"
        if row_count:
            write_raw_silver(raw_frame, output_path)
        write_monitoring_event(
            spark,
            config,
            {
                "layer": "raw",
                "status": status,
                "batch_id": batch_id,
                "input_path": input_path,
                "output_path": output_path,
                "row_count": row_count,
            },
        )
    except Exception as exc:
        write_monitoring_event(
            spark,
            config,
            {
                "layer": "raw",
                "status": "failed",
                "batch_id": batch_id,
                "input_path": input_path,
                "output_path": output_path,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
        )
        raise
    return output_path


def _spark_type(type_name: str) -> str:
    """Translate a framework type name into a Spark SQL type.
    Handles framework aliases such as integer to int.
    Returns the type name consumed by DataFrame.cast.
    """
    return {"integer": "int", "timestamp": "timestamp"}.get(type_name, type_name)


def apply_technical_rules(raw_frame: DataFrame, config: Dict[str, Any]) -> Tuple[DataFrame, DataFrame]:
    """Apply TECH casts, null checks, range checks and null filling.
    Splits valid records from rejected records with quality reasons.
    Returns the TECH DataFrame and the automatically generated REJECT DataFrame.
    """
    rules = {item["name"]: item for item in config["silver"]["tech"].get("columns", [])}
    quality_errors = []
    rejected_fields = []
    technical_frame = raw_frame
    source_columns = []

    for column_name in raw_frame.columns:
        source_column_name = "__source_{}".format(column_name)
        source_columns.append(source_column_name)
        technical_frame = technical_frame.withColumn(
            source_column_name,
            when(trim(col(column_name).cast("string")) == "", lit(None)).otherwise(
                col(column_name)
            ),
        )
        rule = rules.get(column_name, {})
        target_type = _spark_type(rule.get("type", "string"))
        source_column = col(source_column_name)
        cast_column = source_column.cast(target_type)
        cast_failed = source_column.isNotNull() & cast_column.isNull()
        null_failed = lit(False)
        if rule.get("check_nullability") and rule.get("is_critical"):
            null_failed = source_column.isNull()
        range_rule = rule.get("check_range", {})
        below_minimum = lit(False)
        above_maximum = lit(False)
        if "min" in range_rule:
            below_minimum = cast_column < lit(range_rule["min"])
        if "max" in range_rule:
            above_maximum = cast_column > lit(range_rule["max"])
        accepted_values = rule.get("accepted_values", [])
        accepted_values_failed = lit(False)
        if accepted_values:
            accepted_values_failed = cast_column.isNotNull() & ~cast_column.isin(
                accepted_values
            )
        quality_errors.append(
            when(
                cast_failed,
                lit("field={}; check=unexpected_value; reason=cast_failed".format(column_name)),
            )
            .when(
                null_failed,
                lit("field={}; check=unexpected_null; reason=required_field_missing".format(column_name)),
            )
            .when(
                below_minimum,
                lit("field={}; check=unexpected_value; reason=below_minimum".format(column_name)),
            )
            .when(
                above_maximum,
                lit("field={}; check=unexpected_value; reason=above_maximum".format(column_name)),
            )
            .when(
                accepted_values_failed,
                lit("field={}; check=unexpected_value; reason=not_accepted".format(column_name)),
            )
        )
        rejected_fields.append(
            when(
                cast_failed
                | null_failed
                | below_minimum
                | above_maximum
                | accepted_values_failed,
                lit(column_name),
            )
        )
        filled_column = cast_column
        if "fill_nulls" in rule:
            filled_column = when(
                filled_column.isNull(), lit(rule["fill_nulls"])
            ).otherwise(filled_column)
        technical_frame = technical_frame.withColumn(column_name, filled_column)

    technical_frame = technical_frame.withColumn(
        "rejection_reason",
        concat_ws("; ", *quality_errors),
    ).withColumn(
        "rejected_fields",
        concat_ws(",", *rejected_fields),
    )
    rejected = technical_frame.filter(col("rejection_reason") != "")
    valid = technical_frame.filter(col("rejection_reason") == "").drop(
        "rejection_reason", "rejected_fields", *source_columns
    )
    rejected = rejected.drop(*source_columns)
    rejected = rejected.withColumn("rejection_stage", lit("tech")).withColumn(
        "rejected_at", current_timestamp()
    )
    return valid, rejected


def transition_raw_to_tech(spark: SparkSession, config: Dict[str, Any]) -> Tuple[str, str]:
    """Coordinate the RAW to TECH and REJECT Spark transition.
    Writes valid records to TECH and invalid records to the derived REJECT path.
    Returns both output paths for logging and orchestration.
    """
    raw = config["silver"]["raw"]
    tech = config["silver"]["tech"]
    tech_prefix_parts = tech["prefix"].strip("/").split("/")
    if "tech" not in tech_prefix_parts:
        raise ValueError("TECH prefix must contain a tech path segment")
    tech_prefix_parts[tech_prefix_parts.index("tech")] = "reject"
    reject_prefix = "/".join(tech_prefix_parts)
    raw_path = "s3a://{}/{}".format(raw["bucket"], raw["prefix"])
    tech_path = "s3a://{}/{}".format(tech["bucket"], tech["prefix"])
    reject_path = "s3a://{}/{}".format(tech["bucket"], reject_prefix)
    batch_id = latest_partition_value(spark, raw_path, "ingestion_timestamp")
    input_path = _partition_path(raw_path, "ingestion_timestamp", batch_id)
    tech_output_path = _partition_path(tech_path, "ingestion_timestamp", batch_id)
    reject_output_path = _partition_path(reject_path, "ingestion_timestamp", batch_id)
    write_monitoring_event(
        spark,
        config,
        {
            "layer": "tech",
            "status": "started",
            "batch_id": batch_id,
            "input_path": input_path,
            "output_path": tech_output_path,
            "reject_path": reject_output_path,
        },
    )
    try:
        raw_frame = spark.read.parquet(input_path)
        valid_frame, rejected_frame = apply_technical_rules(raw_frame, config)
        valid_count = valid_frame.count()
        rejected_count = rejected_frame.count()
        status = "success" if valid_count or rejected_count else "no_data"
        if valid_count:
            valid_frame.write.mode("append").parquet(tech_output_path)
        if rejected_count:
            rejected_frame.write.mode("append").parquet(reject_output_path)
        write_monitoring_event(
            spark,
            config,
            {
                "layer": "tech",
                "status": status,
                "batch_id": batch_id,
                "input_path": input_path,
                "output_path": tech_output_path,
                "row_count": valid_count,
                "rejected_count": rejected_count,
                "reject_path": reject_output_path,
            },
        )
    except Exception as exc:
        write_monitoring_event(
            spark,
            config,
            {
                "layer": "tech",
                "status": "failed",
                "batch_id": batch_id,
                "input_path": input_path,
                "output_path": tech_output_path,
                "reject_path": reject_output_path,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
        )
        raise
    return tech_output_path, reject_output_path


def _storage_path(layer_config: Dict[str, Any]) -> str:
    """Build a MinIO S3A path from a layer configuration."""
    return "s3a://{}/{}".format(layer_config["bucket"], layer_config["prefix"])


def apply_func_columns(frame: DataFrame, columns: List[Dict[str, str]]) -> DataFrame:
    """Select and rename columns configured for the FUNC layer."""
    if not columns:
        return frame
    selected_columns = []
    for column_config in columns:
        source_name = column_config.get("source", column_config["name"])
        target_name = column_config["name"]
        selected_columns.append(col(source_name).alias(target_name))
    return frame.select(*selected_columns)


def _aggregate_function(function_name: str):
    """Return the Spark aggregate function matching a YAML command."""
    functions = {
        "avg": avg,
        "count": count,
        "max": spark_max,
        "min": spark_min,
        "sum": spark_sum,
    }
    if function_name not in functions:
        raise ValueError("Unsupported aggregate function: {}".format(function_name))
    return functions[function_name]


def apply_custom_commands(frame: DataFrame, commands: List[Dict[str, Any]]) -> DataFrame:
    """Apply YAML-driven PySpark expressions and grouped aggregate commands."""
    result_frame = frame
    for command in commands:
        command_type = command.get("type", "expression")
        if command_type == "expression":
            result_frame = result_frame.withColumn(
                command["name"], expr(command["expression"])
            )
        elif command_type == "aggregate":
            group_by = command["group_by"]
            aggregations = []
            for item in command["metrics"]:
                metric_column = item.get("column", "*")
                metric_input = lit(1) if metric_column == "*" else col(metric_column)
                aggregations.append(
                    _aggregate_function(item["function"])(metric_input).alias(item["name"])
                )
            aggregate_frame = result_frame.groupBy(*group_by).agg(*aggregations)
            result_frame = result_frame.join(aggregate_frame, on=group_by, how="left")
        else:
            raise ValueError("Unsupported custom command type: {}".format(command_type))
    return result_frame


def _hash_columns(frame: DataFrame, dedup_config: Dict[str, Any]) -> List[str]:
    """Choose the columns included in the history hash_data value."""
    hash_config = dedup_config.get("hash", {})
    if "columns" in hash_config:
        return hash_config["columns"]
    excluded = set(hash_config.get("exclude", []))
    return [column_name for column_name in frame.columns if column_name not in excluded]


def apply_func_dedup(frame: DataFrame, dedup_config: Dict[str, Any]) -> DataFrame:
    """Apply FUNC deduplication in snapshot or history mode."""
    if not dedup_config:
        return frame
    keys = dedup_config["keys"]
    mode = dedup_config.get("strategy", "snapshot")
    order_by = dedup_config.get("order_by", [])
    ordered_columns = [col(column_name).desc_nulls_last() for column_name in order_by]

    if mode == "snapshot":
        window = Window.partitionBy(*keys).orderBy(*ordered_columns)
        return (
            frame.withColumn("_dedup_rank", row_number().over(window))
            .filter(col("_dedup_rank") == 1)
            .drop("_dedup_rank")
        )
    if mode == "history":
        hash_column = dedup_config.get("hash", {}).get("column", "hash_data")
        hash_input = concat_ws(
            "||",
            *[
                col(column_name).cast("string")
                for column_name in _hash_columns(frame, dedup_config)
            ]
        )
        history_frame = frame.withColumn(hash_column, sha2(hash_input, 256))
        dedup_keys = keys + [hash_column]
        if not ordered_columns:
            return history_frame.dropDuplicates(dedup_keys)
        window = Window.partitionBy(*dedup_keys).orderBy(*ordered_columns)
        return (
            history_frame.withColumn("_dedup_rank", row_number().over(window))
            .filter(col("_dedup_rank") == 1)
            .drop("_dedup_rank")
        )
    raise ValueError("Unsupported dedup mode: {}".format(mode))


def transition_tech_to_func(spark: SparkSession, config: Dict[str, Any]) -> str:
    """Coordinate the TECH to FUNC Spark transition."""
    tech = config["silver"]["tech"]
    func = config["silver"]["func"]
    tech_path = _storage_path(tech)
    batch_id = latest_partition_value(spark, tech_path, "ingestion_timestamp")
    input_path = _partition_path(tech_path, "ingestion_timestamp", batch_id)
    output_path = _storage_path(func)
    dedup_strategy = func.get("deduplication", {}).get("strategy", "snapshot")
    write_mode = "append" if dedup_strategy == "history" else "overwrite"
    write_monitoring_event(
        spark,
        config,
        {
            "layer": "func",
            "status": "started",
            "batch_id": batch_id,
            "input_path": input_path,
            "output_path": output_path,
            "write_mode": write_mode,
            "dedup_strategy": dedup_strategy,
        },
    )
    try:
        func_frame = spark.read.parquet(input_path)
        func_frame = apply_func_columns(func_frame, func.get("columns", []))
        func_frame = apply_custom_commands(func_frame, func.get("transformations", []))
        func_frame = apply_custom_commands(func_frame, func.get("enrichments", []))
        func_frame = apply_func_dedup(func_frame, func.get("deduplication", {}))
        if "output_partitions" in func:
            func_frame = func_frame.coalesce(int(func["output_partitions"]))
        row_count = func_frame.count()
        status = "success" if row_count else "no_data"
        if row_count:
            func_frame.write.mode(write_mode).parquet(output_path)
        write_monitoring_event(
            spark,
            config,
            {
                "layer": "func",
                "status": status,
                "batch_id": batch_id,
                "input_path": input_path,
                "output_path": output_path,
                "row_count": row_count,
                "write_mode": write_mode,
                "dedup_strategy": dedup_strategy,
            },
        )
    except Exception as exc:
        write_monitoring_event(
            spark,
            config,
            {
                "layer": "func",
                "status": "failed",
                "batch_id": batch_id,
                "input_path": input_path,
                "output_path": output_path,
                "write_mode": write_mode,
                "dedup_strategy": dedup_strategy,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
        )
        raise
    return output_path


def _hash_expression(column_names: List[str]):
    """Build a stable SHA-256 expression from configured columns."""
    return sha2(concat_ws("||", *[col(column_name).cast("string") for column_name in column_names]), 256)


def _select_gold_columns(frame: DataFrame, columns: List[Dict[str, str]]) -> DataFrame:
    """Select configured Gold columns using source aliases or Spark SQL expressions."""
    selected_columns = []
    for column_config in columns:
        target_name = column_config["name"]
        if "expression" in column_config:
            selected_columns.append(expr(column_config["expression"]).alias(target_name))
        else:
            selected_columns.append(col(column_config.get("source", target_name)).alias(target_name))
    return frame.select(*selected_columns)


def build_gold_dimension(func_frame: DataFrame, dimension: Dict[str, Any]) -> DataFrame:
    """Build one configured Gold dimension with SCD 1 or SCD 2 semantics."""
    business_key = dimension["business_key"]
    surrogate_key = dimension["surrogate_key"]
    scd_config = dimension.get("scd", {"type": 1})
    scd_type = int(scd_config.get("type", 1))
    order_by = [scd_config.get("valid_from")] if scd_config.get("valid_from") else []
    ordered_columns = [col(column_name).desc_nulls_last() for column_name in order_by]

    if "source_columns" in dimension:
        value_name = dimension["columns"][0]["name"]
        dimension_frame = None
        for source_column in dimension["source_columns"]:
            candidate = func_frame.select(col(source_column).alias(value_name))
            dimension_frame = candidate if dimension_frame is None else dimension_frame.unionByName(candidate)
    else:
        dimension_frame = _select_gold_columns(func_frame, dimension.get("columns", []))

    for key_column in business_key:
        dimension_frame = dimension_frame.filter(col(key_column).isNotNull())

    if scd_type == 1:
        if ordered_columns:
            window = Window.partitionBy(*business_key).orderBy(*ordered_columns)
            dimension_frame = (
                dimension_frame.withColumn("_dedup_rank", row_number().over(window))
                .filter(col("_dedup_rank") == 1)
                .drop("_dedup_rank")
            )
        else:
            dimension_frame = dimension_frame.dropDuplicates(business_key)
        return dimension_frame.withColumn(surrogate_key, _hash_expression(business_key))

    if scd_type == 2:
        valid_from_source = scd_config["valid_from"]
        tracked_columns = scd_config.get("tracked_columns", business_key)
        dimension_frame = dimension_frame.withColumnRenamed(
            valid_from_source, "valid_from"
        ).withColumn("hash_data", _hash_expression(tracked_columns))
        version_window = Window.partitionBy(*(business_key + ["hash_data"])).orderBy(
            col("valid_from").asc_nulls_last()
        )
        dimension_frame = (
            dimension_frame.withColumn("_dedup_rank", row_number().over(version_window))
            .filter(col("_dedup_rank") == 1)
            .drop("_dedup_rank")
        )
        history_window = Window.partitionBy(*business_key).orderBy(col("valid_from").asc_nulls_last())
        dimension_frame = dimension_frame.withColumn(
            "_next_valid_from", lead(col("valid_from")).over(history_window)
        ).withColumn(
            "valid_to", expr("_next_valid_from - INTERVAL 1 MICROSECOND")
        ).withColumn(
            "is_current", col("_next_valid_from").isNull()
        ).drop(
            "_next_valid_from"
        )
        return dimension_frame.withColumn(surrogate_key, _hash_expression(business_key + ["hash_data"]))

    raise ValueError("Unsupported SCD type: {}".format(scd_type))


def build_gold_fact(
    func_frame: DataFrame, fact: Dict[str, Any], dimensions: Dict[str, DataFrame], dimension_configs: Dict[str, Dict[str, Any]]
) -> DataFrame:
    """Build one configured fact table and join dimension surrogate keys."""
    fact_frame = _select_gold_columns(func_frame, fact.get("columns", []))
    for join_config in fact.get("joins", []):
        dimension_name = join_config["dimension"]
        dimension_frame = dimensions[dimension_name]
        dimension_config = dimension_configs[dimension_name]
        source_keys = join_config["source_key"]
        target_keys = join_config["target_key"]
        output_key = join_config.get("output_key", dimension_config["surrogate_key"])
        join_conditions = [
            col("fact.{}".format(source_key)) == col("dim.{}".format(target_key))
            for source_key, target_key in zip(source_keys, target_keys)
        ]
        if "effective_at" in join_config and int(dimension_config.get("scd", {}).get("type", 1)) == 2:
            join_conditions.append(col("fact.{}".format(join_config["effective_at"])) >= col("dim.valid_from"))
            join_conditions.append(
                col("dim.valid_to").isNull()
                | (col("fact.{}".format(join_config["effective_at"])) <= col("dim.valid_to"))
            )
        condition = reduce(lambda left, right: left & right, join_conditions)
        fact_frame = fact_frame.alias("fact").join(
            dimension_frame.alias("dim"), condition, "left"
        ).select(col("fact.*"), col("dim.{}".format(dimension_config["surrogate_key"])).alias(output_key))
    return fact_frame


def transition_silver_to_gold(spark: SparkSession, config: Dict[str, Any]) -> str:
    """Build configured Gold dimensions and facts from the Silver FUNC layer."""
    func = config["silver"]["func"]
    gold = config["gold"]
    input_path = _storage_path(func)
    output_path = _storage_path(gold)
    func_frame = spark.read.parquet(input_path)
    dimensions = {}
    dimension_configs = {dimension["name"]: dimension for dimension in gold.get("dimensions", [])}
    for dimension in gold.get("dimensions", []):
        dimension_output_path = "{}/{}".format(output_path, dimension.get("path", dimension["name"]))
        write_gold_monitoring_event(
            spark,
            config,
            {
                "dataset_name": dimension["name"],
                "dataset_type": "dimension",
                "status": "started",
                "input_path": input_path,
                "output_path": dimension_output_path,
            },
        )
        try:
            dimension_frame = build_gold_dimension(func_frame, dimension)
            dimension_count = dimension_frame.count()
            if "output_partitions" in gold:
                dimension_frame = dimension_frame.coalesce(int(gold["output_partitions"]))
            dimensions[dimension["name"]] = dimension_frame
            dimension_frame.write.mode("overwrite").parquet(dimension_output_path)
            write_gold_monitoring_event(
                spark,
                config,
                {
                    "dataset_name": dimension["name"],
                    "dataset_type": "dimension",
                    "status": "success" if dimension_count else "no_data",
                    "input_path": input_path,
                    "output_path": dimension_output_path,
                    "row_count": dimension_count,
                },
            )
        except Exception as exc:
            write_gold_monitoring_event(
                spark,
                config,
                {
                    "dataset_name": dimension["name"],
                    "dataset_type": "dimension",
                    "status": "failed",
                    "input_path": input_path,
                    "output_path": dimension_output_path,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
            )
            raise

    fact_row_count = 0
    for fact in gold.get("facts", []):
        fact_output_path = "{}/{}".format(output_path, fact.get("path", fact["name"]))
        write_gold_monitoring_event(
            spark,
            config,
            {
                "dataset_name": fact["name"],
                "dataset_type": "fact",
                "status": "started",
                "input_path": input_path,
                "output_path": fact_output_path,
            },
        )
        try:
            fact_frame = build_gold_fact(func_frame, fact, dimensions, dimension_configs)
            fact_count = fact_frame.count()
            if "output_partitions" in gold:
                fact_frame = fact_frame.coalesce(int(gold["output_partitions"]))
            fact_row_count += fact_count
            fact_frame.write.mode("overwrite").parquet(fact_output_path)
            write_gold_monitoring_event(
                spark,
                config,
                {
                    "dataset_name": fact["name"],
                    "dataset_type": "fact",
                    "status": "success" if fact_count else "no_data",
                    "input_path": input_path,
                    "output_path": fact_output_path,
                    "row_count": fact_count,
                },
            )
        except Exception as exc:
            write_gold_monitoring_event(
                spark,
                config,
                {
                    "dataset_name": fact["name"],
                    "dataset_type": "fact",
                    "status": "failed",
                    "input_path": input_path,
                    "output_path": fact_output_path,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
            )
            raise
    return output_path