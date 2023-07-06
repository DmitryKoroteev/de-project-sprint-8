import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType
from scripts\project_7_script_step_2 import event_with_city, event_corr_city, travel_geo, home_geo
from scripts\project_7_script_step_3 import df_pivot_step_3


def main():
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .config("spark.driver.cores", "2") \
        .config("spark.driver.memory", "2g") \
        .appName("project_7_4") \
        .getOrCreate()

    path_event_prqt = '/user/master/data/geo/events'

    path_city_data = '/user/dmitrykoro/data/events/geo.csv'

    events_city = event_with_city(path_event_prqt, spark)
    df_city = event_corr_city(events_city, spark)
    df_friends = df_friends_final(df_city, spark)

    return df_friends


def df_friends_final(df_city: pyspark.sql.DataFrame, DF_local_time: pyspark.sql.DataFrame,
                     spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    window = Window().partitionBy('user').orderBy('date')
    df_city_from = df_city.selectExpr('event.message_from as user','msg_lat', 'msg_lon', 'date', 'city')
    df_city_to = df_city.selectExpr('event.message_to as user','msg_lat', 'msg_lon', 'date', 'city')
    df = df_city_from.union(df_city_to) \
        .select(F.col('user'),
                F.last(F.col('date'), ignorenulls=True).over(window).alias('date_to'),
                F.last(F.col('msg_lat'), ignorenulls=True).over(window).alias('lat_to'),
                F.last(F.col('msg_lon'), ignorenulls=True).over(window).alias('lng_to'),
                F.col('city')) \
        .distinct()

    df_2 = df \
        .withColumnRenamed('user', 'user_2') \
        .withColumnRenamed('date_to', 'date_to_2') \
        .withColumnRenamed('lat_to', 'lat_to_2') \
        .withColumnRenamed('lng_to', 'lng_to_2') \
        .withColumnRenamed('city', 'city_2')

    # определим пользователей, расстояние между которыми менее 1 км
    df_close = df \
        .join(df_2, df.date_to == df_2.date_to_2, "inner") \
        .withColumn('dif', F.acos(
        F.sin(F.col('lat_to')) * F.sin(F.col('lat_to_2')) + F.cos(F.col('lat_to')) * F.cos(F.col('lat_to_2')) * F.cos(
            F.col('lng_to') - F.col('lng_to_2'))) * F.lit(6371)) \
        .filter(F.col('dif') <= 1)

    df_city_2 = df_city \
        .withColumnRenamed('event', 'event_2')
    # определим пары пользователей, подписанных на один канал
    df_friends_channels = df_city \
        .join(df_city_2, (df_city.event.subscription_channel == df_city_2.event_2.subscription_channel) & (
                df_city.event.user != df_city_2.event_2.user), "inner") \
        .selectExpr('event.user as user_left', 'event_2.user as user_right') \
        .distinct()

    df_close_one_channel = df_close \
        .join(df_friends_channels,
              (df_close.user == df_friends_channels.user_left) & (df_close.user_2 == df_friends_channels.user_right),
              "inner")

    df_close_one_channel_copy = df_close_one_channel \
        .withColumnRenamed('user', 'user_3') \
        .withColumnRenamed('user_2', 'user_4') \
        .withColumnRenamed('date_to', 'date_to_3') \
        .withColumnRenamed('date_to_2', 'date_to_4') \
        .withColumnRenamed('lat_to', 'lat_to_3') \
        .withColumnRenamed('lng_to', 'lng_to_3') \
        .withColumnRenamed('city', 'city_3') \
        .withColumnRenamed('lat_to_2', 'lat_to_4') \
        .withColumnRenamed('lng_to_2', 'lng_to_4') \
        .withColumnRenamed('city_2', 'city_4')

    conversations = df_city \
        .join(df_city_2, (df_city.event.message_from == df_city_2.event_2.message_to) & (
            (df_city.event.message_to == df_city_2.event_2.message_from)), "inner") \
        .distinct()

    df = df_close_one_channel.join(conversations, (df_close_one_channel.user == conversations.message_from) &
                                   (df_close_one_channel.user_2 == conversations.message_to), 'left_anti')

    df_friends = df \
        .join(DF_local_time, df.city == DF_local_time.city, "left") \
        .withColumn("processed_dt", F.current_date()) \
        .selectExpr('user as user_left', 'user_2 as user_right', 'city_2 as city', 'local_time', 'processed_dt') \
        .distinct()

    return df_friends


if __name__ == "__main__":
    main()
