from datetime import datetime as dt
from pyspark.sql import Functions as F


def run_app(spark):
    df = spark.read.csv('s3://sid-emr-test-data-bucket/gold_prices.csv', header=True)

    df = df \
        .withColumn(
            'Year',
            F.year(
                F.to_date(
                    df.Date,
                    'dd-MM-yyyy'
                )
            )
        ) \
        .withColumn(
            'Price_USD_',
            F.col('Price_USD').cast('float')
        )

    now = dt.now().strftime('%Y%m%d-%H%M%S')

    df \
        .groupBy('Year') \
        .agg(
            F \
            .avg('Price_USD_') \
            .alias("Avg_price_USD")
        ) \
        .sort('Year') \
        .write \
        .csv(f's3://sid-emr-test-data-bucket/ouptut/{now}')

