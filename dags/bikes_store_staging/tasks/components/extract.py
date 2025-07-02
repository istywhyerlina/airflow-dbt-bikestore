from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow import AirflowException
from airflow.models import Variable

from helper.minio import CustomMinio
from datetime import timedelta
import json
import pandas as pd
import requests


class Extract:
    @staticmethod
    def _bikestore(table_name, incremental, **kwargs):
        try:
            pg_hook = PostgresHook(postgres_conn_id='bikes-store-db')
            connection = pg_hook.get_conn()
            cursor = connection.cursor()
            #object_file=table_name.split(".")[-1]
            table_pkey = kwargs.get('table_pkey')
            
            query = f"SELECT * FROM {table_pkey[table_name][0]}.{table_name}"
            if incremental:
                date = kwargs['ds']
                query += f" WHERE modifieddate::DATE = '{date}'::DATE - INTERVAL '1 DAY';"
            
                
                object_name = f'/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv'
            
            else:
                object_name = f'/{table_name}.csv'

            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            connection.commit()
            connection.close()

            column_list = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_list)
        
            if df.empty:
                raise AirflowSkipException(f"{table_name} doesn't have new data. Skipped...")

            bucket_name = 'bikes-store'
            CustomMinio._put_csv(df, bucket_name, object_name)

        except AirflowSkipException as e:
            raise e
        except Exception as e:
            raise AirflowException(f"Error when extracting {table_name} : {str(e)}")
            
    @staticmethod
    def _src_api():
        """
        Extract data from data API.

        Args:
            ds (str): Date string.

        Raises:
            AirflowException: If failed to fetch data from data API.
            AirflowSkipException: If no new data is found.
        """
        try:
            print("--------------Extracting data from API------------------")
            url_api=Variable.get('BIKES_STORE_API_URL')
            print(f"--------------URL API: {url_api} ------------------")
            response = requests.get(
                    url=url_api
                )
            

            if response.status_code != 200:
                raise AirflowException(f"Failed to fetch data from data API. Status code: {response.status_code}")

            json_data = response.json()
            print(type(json_data))

            if not json_data:
                raise AirflowSkipException("No new data in data API. Skipped...")
            
            df = pd.json_normalize(json_data)
            bucket_name = 'bikes-store'
            object_name = f'/currency.csv'
            CustomMinio._put_csv(df, bucket_name, object_name)
            
        except AirflowSkipException as e:
            raise e
        
        
        except Exception as e:
            raise AirflowException(f"Error when extracting data API: {str(e)}")