from pyspark.sql import SparkSession, DataFrame
import findspark

findspark.init()
findspark.find()

import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from datetime import date, datetime
from scripts\project_7_script_step_2 import event_with_city, event_corr_city, travel_geo, home_geo

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 1),
    }

dag = DAG(
    dag_id="datalake_project_7_step_2",
    default_args=default_args,
    schedule_interval=None,
    tags=['sprint_7', 'project'],
    )

load_events_city = PythonOperator(
        task_id='load_events_city',
        python_callable=event_with_city(),
        op_kwargs={
            'path_event_prqt': '/user/master/data/geo/events',
            'path_city_data': '/user/dmitrykoro/data/events/geo.csv',
            'spark': SparkSession
                .builder
                .master("yarn")
                .config("spark.driver.cores", "2")
                .config("spark.driver.memory", "2g")
                .appName("project_7")
                .getOrCreate(),
        },
    )

load_df_city = PythonOperator(
        task_id='load_df_city',
        python_callable=event_corr_city(),
        op_kwargs={
            'events_city': events_city,
            'spark': SparkSession
                .builder
                .master("yarn")
                .config("spark.driver.cores", "2")
                .config("spark.driver.memory", "2g")
                .appName("project_7")
                .getOrCreate(),
        },
    )

load_df_travel = PythonOperator(
        task_id='load_df_travel',
        python_callable=travel_geo(),
        op_kwargs={
            'df_city': df_city,
            'spark': SparkSession
                .builder
                .master("yarn")
                .config("spark.driver.cores", "2")
                .config("spark.driver.memory", "2g")
                .appName("project_7")
                .getOrCreate(),
        },
    )

load_df_home = PythonOperator(
        task_id='load_df_home',
        python_callable=home_geo(),
        op_kwargs={
            'df_city': df_travel,
            'spark': SparkSession
                .builder
                .master("yarn")
                .config("spark.driver.cores", "2")
                .config("spark.driver.memory", "2g")
                .appName("project_7")
                .getOrCreate(),
        },
    )


load_events_city >> load_df_city >> load_df_travel >> load_df_home