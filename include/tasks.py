from include.datasets import DATASET_COCKTAIL
from airflow.exceptions import AirflowFailException

def _get_cocktail(ti=None):
    import requests
    from airflow.models import Variable
    
    #api = "https://www.thecocktaildb.com/api/json/v1/1/random.php"
    api = Variable.get('api')
    response = requests.get(api)
    with open(DATASET_COCKTAIL.uri, "wb") as f:
        f.write(response.content)
    ti.xcom_push(key='request_size', value=len(response.content))
        
def _check_size(ti=None):
    size = ti.xcom_pull(key='request_size', task_ids='get_cocktail')
    print(f"download size: {size}")
    if not size:
        raise AirflowFailException("Downloaded content is empty")
    return size

def _validate_json_fields(ti=None):
    import json
    from airflow.exceptions import AirflowFailException

    path = DATASET_COCKTAIL.uri
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        raise AirflowFailException(f"Failed to read/parse JSON at {path}: {e}")

    if 'drinks' not in data or not isinstance(data['drinks'], list) or not data['drinks']:
        raise AirflowFailException("JSON does not contain a non-empty 'drinks' list")

    drink = data['drinks'][0]

    required_keys = [
        'idDrink', 'strDrink', 'strDrinkAlternate', 'strTags', 'strVideo', 'strCategory',
        'strIBA', 'strAlcoholic', 'strGlass', 'strInstructions', 'strInstructionsES',
        'strInstructionsDE', 'strInstructionsFR', 'strInstructionsIT', 'strInstructionsZH-HANS',
        'strInstructionsZH-HANT', 'strDrinkThumb',
        'strImageSource', 'strImageAttribution', 'strCreativeCommonsConfirmed', 'dateModified'
    ]
    # add ingredient and measure fields 1..15
    required_keys += [f'strIngredient{i}' for i in range(1, 16)]
    required_keys += [f'strMeasure{i}' for i in range(1, 16)]

    missing = [k for k in required_keys if k not in drink]
    if missing:
        raise AirflowFailException(f"JSON is missing expected keys in the first drink: {missing}")

    ti.xcom_push(key='json_validated', value=True)
    print("JSON validation passed")