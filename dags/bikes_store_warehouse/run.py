from datetime import datetime
from airflow.datasets import Dataset
from helper.callbacks.slack_notifier import slack_notifier
from airflow.decorators import dag
from airflow.models.variable import Variable
from airflow.decorators import task
from airflow.providers.slack.notifications.slack import send_slack_notification

from cosmos.constants import TestBehavior
from cosmos.config import ProjectConfig, ProfileConfig, RenderConfig,LoadMode
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping
from cosmos import DbtDag, DbtTaskGroup

import os

# default_args = {
#     'on_failure_callback': slack_notifier
# }
default_args = {
    'on_failure_callback': send_slack_notification(
        slack_conn_id="slack_conn",
        channel="airflow-notifications",
        text="There is an ERROR in DAG bikes_store_warehouse_dbt")}

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/bikes_store_warehouse/bikes_store_warehouse_dbt"

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    project_name="datawarehouse",
    install_dbt_deps=True
)

profile_config = ProfileConfig(
    profile_name="warehouse",
    target_name="warehouse",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id='warehouse',
        profile_args={"schema": "warehouse"}
    )
)

render_config1=RenderConfig(
        dbt_executable_path="/opt/airflow/dbt_venv/bin",
        emit_datasets=True,
        test_behavior=TestBehavior.AFTER_ALL,
        exclude=["dim_date"]

)

render_config_init=RenderConfig(
        dbt_executable_path="/opt/airflow/dbt_venv/bin",
        emit_datasets=True,
        test_behavior=TestBehavior.AFTER_ALL,
        

)

@dag(
    dag_id='bikes_store_warehouse',
    description='Extract data and load into warehouse area',
    start_date=datetime(2024, 1, 9),
    schedule=None,
    default_args=default_args
)

def bikes_store_warehouse():
    @task.branch
    def check_is_warehouse_init():
        BIKES_STORE_WAREHOUSE_INIT = eval(Variable.get('BIKES_STORE_WAREHOUSE_INIT'))
        if BIKES_STORE_WAREHOUSE_INIT==True:
            return 'init_warehouse'
        else:
            return 'warehouse_pipeline'



    init_warehouse = DbtTaskGroup(
        group_id="init_warehouse",
        # schedule=None,
        # catchup=False,
        # start_date=datetime(2024, 9, 30),
        project_config=project_config,
        profile_config=profile_config,
        render_config=render_config_init,
        default_args={
            'on_failure_callback': slack_notifier
        }
    )

    warehouse_pipeline = DbtTaskGroup(
        group_id="warehouse_pipeline",
        # schedule=None,
        # catchup=False,
        # start_date=datetime(2024, 9, 30),
        project_config=project_config,
        profile_config=profile_config,
        render_config=render_config1,
        default_args={
            'on_failure_callback': slack_notifier
        }
    )
    check_is_warehouse_init() >> [init_warehouse, warehouse_pipeline]
    
bikes_store_warehouse()