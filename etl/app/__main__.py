from pyspark.sql import SparkSession
from emrpkg import emr_test

if __name__ == '__main__':
    spark = SparkSession\
        .builder \
        .appName('EMR-Test') \
        .getOrCreate()

    emr_test.run_app(spark)
