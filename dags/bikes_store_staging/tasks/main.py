from airflow.decorators import task,task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from bikes_store_staging.tasks.components.extract import Extract
from bikes_store_staging.tasks.components.load import Load
from airflow.datasets import Dataset

@task_group
def extract_db(incremental):            
    #table_to_extract = eval(Variable.get('BIKES_STORE_SOURCE__table_to_extract'))
    table_pkey = eval(Variable.get('BIKES_STORE_STAGING__table_to_extract_and_load'))
    table_to_extract = list(table_pkey.keys())

    for table_name in table_to_extract:
        current_task = PythonOperator(
            task_id = f'{table_name}',
            python_callable = Extract._bikestore,
            trigger_rule = 'none_failed',
            outlets = [Dataset(f'postgres://sources:5432/postgres.{table_name}')],
            op_kwargs = {
                'table_name': f'{table_name}',
                'incremental': incremental,
                'table_pkey': table_pkey,
            }
        )

        current_task
        
@task
def extract_api():
    extract = PythonOperator(
        task_id = 'extract_api',
        python_callable = Extract._src_api,
        trigger_rule = 'none_failed',
        outlets = [Dataset(f'https://api-currency-five.vercel.app/api/currencydata')]
    )

@task_group
def load_db(incremental):
    table_pkey = eval(Variable.get('BIKES_STORE_STAGING__table_to_extract_and_load'))
    table_to_load = list(table_pkey.keys())
    previous_task = None
    
    for table_name in table_to_load:
        current_task = PythonOperator(
            task_id = f'{table_name}',
            python_callable = Load._bikestore,
            trigger_rule = 'none_failed',
            outlets = [Dataset(f'postgres://warehouse:5432/postgres.bikes_store_staging.{table_name}')],
            op_kwargs = {
                'table_name': table_name,
                'table_pkey': table_pkey,
                'incremental': incremental
            },
        )

        if previous_task:
            previous_task >> current_task

        previous_task = current_task

@task
def load_api():
        table_name ='currency'
        table_pkey='currencycode'
        api = PythonOperator(
            task_id = f'{table_name}',
            python_callable = Load._api,
            trigger_rule = 'none_failed',
            outlets = [Dataset(f'postgres://warehouse:5432/postgres.bikes_store_staging.{table_name}')],
            op_kwargs = {
                'table_name': table_name,
                'table_pkey': table_pkey,
            },
        )

