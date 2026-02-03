from include.datasets import DATASET_COCKTAIL
from airflow.exceptions import AirflowFailException

def _get_cocktail(ti=None):
    import requests
    
    api = "https://www.thecocktaildb.com/api/json/v1/1/random.php"
    response = requests.get(api)
    with open(DATASET_COCKTAIL.uri, "wb") as f:
        f.write(response.content)
    ti.xcom_push(key='request_size', value=len(response.content))
        
def _check_size(ti=None):
    size = ti.xcom_pull(key='request_size', task_ids='get_cocktail')
    print(size)
    raise AirflowFailException