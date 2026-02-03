from airflow.decorators import dag, task_group, task
from airflow.operators.python import PythonOperator
from pendulum import datetime, duration
from include.datasets import DATASET_COCKTAIL
from include.tasks import _get_cocktail, _check_size, _validate_json_fields
from include.extractor.callbacks import _handle_empty_size, _handle_failed_dag_run
import json
#import airflow.utils.trigger_rule as trigger_rule
from airflow.utils.trigger_rule import TriggerRule

@dag(
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    default_args={
        'retries': 2,
        'retry_delay': duration(seconds=2),
    },
    tags = ['ecom'],
    catchup=False,
    on_failure_callback=_handle_failed_dag_run
)
def extractor():
    
    get_cocktail = PythonOperator(
        task_id='get_cocktail',
        python_callable=_get_cocktail,
        outlets=[DATASET_COCKTAIL],
        retry_exponential_backoff=True,
        max_retry_delay=duration(minutes=15)
    )
    
    @task_group(
    )
    def checks():
        validate_json = PythonOperator(
        task_id='validate_json',
        python_callable=_validate_json_fields,
        )
    
        check_size = PythonOperator(
        task_id='check_size',
        python_callable=_check_size,
        on_failure_callback=_handle_empty_size
        )

        check_size >> validate_json 
    
    @task.branch()
    def branch_cocktail_type():
        with open(DATASET_COCKTAIL.uri, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if data['drinks'][0]['strAlcoholic'] == 'Alcoholic':
                return 'alcoholic_drink'
            else:
                return 'non_alcoholic_drink'
    
    @task()
    def alcoholic_drink():
        print("This is an alcoholic drink.")

    @task()
    def non_alcoholic_drink():
        print("This is a non-alcoholic drink.")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def clean_data():
        import os
        if os.path.exists(DATASET_COCKTAIL.uri):
            os.remove(DATASET_COCKTAIL.uri)
            print(f"Removed file: {DATASET_COCKTAIL.uri}")
        else:
            print(f"File not found, nothing to remove: {DATASET_COCKTAIL.uri}")

    get_cocktail >> checks() >> branch_cocktail_type() >> [alcoholic_drink(), non_alcoholic_drink()] >> clean_data()
    
my_extractor = extractor()

if __name__ == "__main__":
    my_extractor.test()