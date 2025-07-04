from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "second_dag",
    default_args=default_args,
    description="A simple second DAG",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def training_model_a():
        return 1

    def training_model_b():
        return 2

    def training_model_c():
        return 3

    def choose_best_model(accuracies):
        if max(accuracies) > 2:
            return "accurate"
        return "inaccurate"

    training_a = PythonOperator(
        task_id="training_model_a", python_callable=training_model_a
    )
    training_b = PythonOperator(
        task_id="training_model_b", python_callable=training_model_b
    )
    training_c = PythonOperator(
        task_id="training_model_c", python_callable=training_model_c
    )

    accuracies = [training_a.output, training_b.output, training_c.output]

    choose_best = PythonOperator(
        task_id="choose_best_model",
        python_callable=choose_best_model,
        op_args=[accuracies],
    )

    accurate = BashOperator(task_id="accurate", bash_command="echo 'accurate'")
    inaccurate = BashOperator(task_id="inaccurate", bash_command="echo 'inaccurate'")

    training_a >> training_b >> training_c >> choose_best >> [accurate, inaccurate]
