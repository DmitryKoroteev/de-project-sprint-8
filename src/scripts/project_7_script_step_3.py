import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType
from scripts\project_7_script_step_2 import event_with_city, event_corr_city, travel_geo, home_geo


def main():
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .config("spark.driver.cores", "2") \
        .config("spark.driver.memory", "2g") \
        .appName("project_7_3") \
        .getOrCreate()

    path_event_prqt = '/user/master/data/geo/events'

    path_city_data = '/user/dmitrykoro/data/events/geo.csv'

    events_city = event_with_city(path_event_prqt, spark)
    df_city = event_corr_city(events_city, spark)
    df_pivot = df_pivot_step_3(df_city, spark)

    return df_pivot


def df_pivot_step_3(df_city: pyspark.sql.DataFrame, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    w_week = Window.partitionBy(['city', F.trunc(F.col("date"), "week")])
    w_month = Window.partitionBy(['city', F.trunc(F.col("date"), "month")])
    window = Window().partitionBy('event.message_from').orderBy(F.col('date'))

    df_registration = df_city \
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col('row_number') == 1) \
        .selectExpr('date', 'event.message_from as user') \
        .groupBy('date') \
        .agg(F.countDistinct(F.col('user')).alias('cnt_reg'))

    df_pivot = df_city\
        .join(df_registration, df_city.date == df_registration.date_reg,"left")\
        .withColumn("month", F.trunc(F.col("date"), "month"))\
        .withColumn("week", F.trunc(F.col("date"), "week"))\
        .withColumn("rn", F.row_number().over(window))\
        .withColumn("week_message", F.sum(F.when(df_city.event_type == "message",1).otherwise(0)).over(w_week))\
        .withColumn("week_reaction", F.sum(F.when(df_city.event_type == "reaction",1).otherwise(0)).over(w_week))\
        .withColumn("week_subscription", F.sum(F.when(df_city.event_type == "subscription",1).otherwise(0)).over(w_week))\
        .withColumn('week_user', F.sum('cnt_reg').over(w_week))\
        .withColumn("month_message", F.sum(F.when(df_city.event_type == "message",1).otherwise(0)).over(w_month))\
        .withColumn("month_reaction", F.sum(F.when(df_city.event_type == "reaction",1).otherwise(0)).over(w_month))\
        .withColumn("month_subscription", F.sum(F.when(df_city.event_type == "subscription",1).otherwise(0))\
                    .over(w_month))\
        .withColumn('month_user', F.sum('cnt_reg').over(w_month))\
        .selectExpr("month", "week", "week_message", "week_reaction", "week_subscription",'week_user', "month_message",
                    "month_reaction", "month_subscription", 'month_user') \
        .distinct()

    return df_pivot


if __name__ == "__main__":
    main()
