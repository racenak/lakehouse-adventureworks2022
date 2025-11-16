from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="echo_hello",
    default_args=default_args,
    catchup=False,
    tags=["example"],
) as dag:
    echo = BashOperator(
        task_id="echo_hello",
        bash_command='echo "hello"',
    )

    echo