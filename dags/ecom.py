from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration
from include.datasets import DATASET_COCKTAIL

@dag(start_date=datetime(2025, 1 ,1),
     schedule=[DATASET_COCKTAIL],
     catchup=True,
     description="This DAG processes ecommerce data",
     tags=["team_a", "ecom"],
     default_args={"retries": 2},
     dagrun_timeout=duration(minutes=20),
     max_consecutive_failed_dag_runs=2)
def ecom():

    ta = EmptyOperator(task_id='ta')
    
ecom()
