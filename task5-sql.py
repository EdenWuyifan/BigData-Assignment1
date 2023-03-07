import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit task5-sql.py <issued>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    spark = SparkSession.builder.appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    issued = spark.read.format('csv').options(header='true',inferschema='true').load("/shared/CS-GY-6513/parking-violations/parking-violations-header.csv")
    issued.createOrReplaceTempView("issued")

    result = spark.sql("SELECT vehicle_make, COUNT(*) AS count FROM issued GROUP BY vehicle_make ORDER BY count DESC LIMIT 1")
    result.select(format_string('%s\t %d', result.vehicle_make, result.count)).write.save("task5-sql.out", format="text")

    sc.stop()