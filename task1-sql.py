import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, format_string

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit task1.py <isssued> <open>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    spark = SparkSession.builder.appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    issued = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
    opened = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])

    issued.createOrReplaceTempView("issued")
    opened.createOrReplaceTempView("opened")

    result = spark.sql("SELECT issued.summons_number, issued.violation_county, issued.registration_state, issued.violation_code, issued.issue_date FROM issued LEFT JOIN opened ON issued.summons_number=opened.summons_number WHERE opened.summons_number is NULL")
    result.select(format_string('%d\t%s, %s, %d, %s', result.summons_number, result.violation_county, result.registration_state, result.violation_code, date_format(result.issue_date, 'yyyy-MM-dd'))).write.save("yfw215_task1-sql.out",format="text")

    sc.stop()
