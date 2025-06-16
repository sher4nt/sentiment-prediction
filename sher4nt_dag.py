from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

pyspark_python = "/opt/conda/envs/dsenv/bin/python"
base_dir = '{{ dag_run.conf["base_dir"] if dag_run else "" }}'

with DAG(
    dag_id='sher4nt_dag',
    start_date=datetime(2025, 5, 1),
    schedule=None,
    catchup=False,
    description='dag for hw6'
) as dag:
    feature_eng_task_train = SparkSubmitOperator(
        task_id='feature_eng_train',
        application=f'{base_dir}/feature_eng.py',
        spark_binary='/usr/bin/spark3-submit',
        application_args=['/datasets/amazon/amazon_extrasmall_train.json', 'sher4nt_train_out'],
        env_vars={'PYSPARK_PYTHON': pyspark_python}
    )

    download_train_task = SparkSubmitOperator(
        task_id='download_train',
        application=f'{base_dir}/train_download.py',
        spark_binary='/usr/bin/spark3-submit',
        application_args=['sher4nt_train_out', f'{base_dir}/sher4nt_train_out_local'],
        env_vars={'PYSPARK_PYTHON': pyspark_python}
    )

    train_task = BashOperator(
        task_id='train',
        bash_command=f'{pyspark_python} {base_dir}/train.py {base_dir}/sher4nt_train_out_local {base_dir}/6.joblib'
    )

    model_sensor = FileSensor(
        task_id='sensor',
        filepath=f'{base_dir}/6.joblib',
        poke_interval=10,
        timeout=120
    )
    
    feature_eng_task_test = SparkSubmitOperator(
        task_id='feature_eng_test',
        application=f'{base_dir}/feature_eng.py',
        spark_binary='/usr/bin/spark3-submit',
        application_args=['/datasets/amazon/amazon_extrasmall_test.json', 'sher4nt_test_out'],
        env_vars={'PYSPARK_PYTHON': pyspark_python}
    )

    predict_task = SparkSubmitOperator(
        task_id='predict',
        application=f'{base_dir}/predict.py',
        spark_binary='/usr/bin/spark3-submit',
        application_args=['sher4nt_test_out', 'sher4nt_hw6_prediction', f'{base_dir}/6.joblib'],
        env_vars={'PYSPARK_PYTHON': pyspark_python}
    )

    feature_eng_task_train >> download_train_task >> train_task >> model_sensor >> feature_eng_task_test >> predict_task