from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("taxi-spark-smoke-test").getOrCreate()

result = spark.createDataFrame([(1, "spark-ok")], ["id", "status"])
result.show()

spark.stop()