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
from scripts\project_7_script_step_3 import df_pivot_step_3

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 1),
    }

dag = DAG(
    dag_id="datalake_project_7_step_3",
    default_args=default_args,
    schedule_interval=None,
    tags=['sprint_7', 'project'],
    )

load_zones = PythonOperator(
        task_id='load_zones',
        python_callable=df_pivot_step_3(),
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

load_zones