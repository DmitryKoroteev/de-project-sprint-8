import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType


def main():
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .config("spark.driver.cores", "2") \
        .config("spark.driver.memory", "2g") \
        .appName("project_7") \
        .getOrCreate()

    path_event_prqt = '/user/master/data/geo/events'

    path_city_data = '/user/dmitrykoro/data/events/geo.csv'

    events_city = event_with_city(path_event_prqt, spark)
    df_city = event_corr_city(events_city, spark)
    df_travel = travel_geo(df_city, spark)
    df_home = home_geo(df_travel, spark)

    return df_home


def event_with_city(path_event_prqt: str, path_city_data: str, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    events_geo = spark.read.parquet(path_event_prqt) \
        .sample(0.05) \
        .where("event_type == 'message'") \
        .withColumn('user_id', F.col('event.message_from')) \
        .withColumnRenamed('lat', 'msg_lat') \
        .withColumnRenamed('lon', 'msg_lon') \
        .withColumn('event_id', F.monotonically_increasing_id())

    city = spark.read.csv('/user/dmitrykoro/data/events/geo.csv', sep = ";", header = True )\
        .withColumn('lat', F.regexp_replace('lat', ',', '.'))\
        .withColumn('lng', F.regexp_replace('lng', ',', '.'))

    events_city = events_geo \
        .crossJoin(city) \
        .withColumn('diff', F.acos(
            F.sin(F.col('msg_lat').cast("decimal(14,2)")) * F.sin(F.col('lat').cast("decimal(14,2)")) + F.cos(
                F.col('msg_lat').cast("decimal(14,2)")) * F.cos(F.col('lat').cast("decimal(14,2)")) * F.cos(
                F.col('msg_lon').cast("decimal(14,2)") - F.col('lng').cast("decimal(14,2)"))) * F.lit(6371)).persist() \
        .withColumn("TIME", F.col("event.datetime").cast("Timestamp")) \
        .withColumn("timezone", F.concat(F.lit("Australia"), F.col('city'))) \
        .withColumn("local_time", F.from_utc_timestamp(F.col("TIME"), F.col('timezone')))

    return events_city


def event_corr_city(events_city: pyspark.sql.DataFrame, spark: pyspark.sql.SparkSession):
    window = Window().partitionBy('event.message_from').orderBy(F.col('diff').asc())
    df_city = events_city \
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col('row_number') == 1) \
        .drop('row_number')

    return df_city


def DF_local_time(events_city: pyspark.sql.DataFrame, spark: pyspark.sql.SparkSession):
    DF_local_time = events_city \
        .withColumn("TIME", F.col("event.datetime").cast("Timestamp")) \
        .withColumn("timezone", F.concat(F.lit("Australia"), F.col('city'))) \
        .withColumn("local_time", F.from_utc_timestamp(F.col("TIME"), F.col('timezone'))) \
        .selectExpr('local_time', 'TIME', 'city')

    return DF_local_time


def actual_geo(df_city: pyspark.sql.DataFrame, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    window = Window().partitionBy('event.message_from').orderBy(F.col('date').desc())
    df_actual = df_city \
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col('row_number') == 1) \
        .selectExpr('event.message_from as user', 'city', 'id as city_id') \
        .persist()

    return df_actual


def travel_geo(df_city: pyspark.sql.DataFrame, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    window = Window().partitionBy('event.message_from', 'id').orderBy(F.col('date'))
    df_travel = df_city \
        .withColumn("dense_rank", F.dense_rank().over(window)) \
        .withColumn("date_diff",
                    F.datediff(F.col('date').cast(DateType()), F.to_date(F.col("dense_rank").cast("string"), 'dd'))) \
        .selectExpr('date_diff', 'event.message_from as user', 'date', "id") \
        .groupBy("user", "date_diff", "id") \
        .agg(F.countDistinct(F.col('date')).alias('cnt_city'))

    return df_travel


def home_geo(df_travel: pyspark.sql.DataFrame, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    df_home_candidates = df_travel \
        .filter(F.col('cnt_city') > 27) \
        .persist()
    df_home = df_home_candidates \
        .withColumn('max_dt', F.max(F.col('date_diff')) \
                    .over(Window().partitionBy('user'))) \
        .filter(F.col('date_diff') == F.col('max_dt')) \
        .persist()

    return df_home


if __name__ == "__main__":
    main()
