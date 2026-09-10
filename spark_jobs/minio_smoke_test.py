import os

from pyspark.sql import SparkSession


output_path = "s3a://datalake/smoke_test/"

spark = SparkSession.builder.appName("taxi-minio-smoke-test").getOrCreate()

data = [
    ("2026-01-01", "zone-a", 12.5),
    ("2026-01-01", "zone-b", 8.0),
]
result = spark.createDataFrame(data, ["trip_date", "pickup_zone", "fare"])
result.write.mode("overwrite").parquet(output_path)
spark.stop()
print(f"Wrote Parquet to {output_path}")