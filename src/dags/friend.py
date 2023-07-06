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
from scripts\project_7_script_step_4 import df_friends_final

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 1),
    }

dag = DAG(
    dag_id="datalake_project_7_step_4",
    default_args=default_args,
    schedule_interval=None,
    tags=['sprint_7', 'project'],
    )

load_df_friends = PythonOperator(
        task_id='load_df_friends',
        python_callable=df_friends_final(),
        op_kwargs={
            'events_city': events_city,
            'DF_local_time': DF_local_time,
            'spark': SparkSession
                .builder
                .master("yarn")
                .config("spark.driver.cores", "2")
                .config("spark.driver.memory", "2g")
                .appName("project_7")
                .getOrCreate(),
        },
    )


load_df_friends