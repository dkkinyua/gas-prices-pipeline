from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'dee',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

@dag(dag_id='test_dag', default_args=default_args, start_date=datetime(2025, 5, 26), schedule='@hourly', catchup=False)
def test_dag_working():
    @task
    def print_hello():
        print('hey, the DAG is working!')

    print_hello()

test_dag = test_dag_working()