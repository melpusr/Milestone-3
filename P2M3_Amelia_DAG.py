from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine #connect to postgres
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

def fetch_data():
    # fetch data
    database = "airflow"
    username = "airflow"
    password = "airflow"
    host = "postgres"

    # URL connection to postgres
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # connect to sql alchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    data = pd.read_sql_query("SELECT * FROM public.table_m3", conn) 

    data.to_csv('/opt/airflow/dags/P2M3_Amelia_dataraw.csv', sep=',', index=False)

def preprocessing(): 
    data = pd.read_csv("/opt/airflow/dags/P2M3_Amelia_dataraw.csv")
    # cleaning data 
    data.columns = data.columns.str.lower()
    data.columns = data.columns.str.replace(' ', '_')
    data.columns = data.columns.str.replace('/', '_')  
    data.columns = data.columns.str.replace('-', '_')  
    data.dropna(inplace=True)
    data.drop_duplicates(inplace=True)
    data.to_csv('/opt/airflow/dags/P2M3_Amelia_cleaned.csv', index=False)
    
def upload_to_elasticsearch():
    es = Elasticsearch("http://elasticsearch:9200")
    data = pd.read_csv('/opt/airflow/dags/P2M3_Amelia_cleaned.csv')
    
    for i, r in data.iterrows():
        doc = r.to_dict()  
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
        
default_args = {
    'owner': 'Amel', 
    'start_date': datetime(2024, 5, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG("P2M3_AmeliaPS_DAG", 
         description='Milestone_3', 
         schedule_interval='30 6 * * *', 
         default_args=default_args,
) as dag: 
    
    # Task: 1
    fetch_data_pg = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data) 
    
    # Task: 2
    edit_data = PythonOperator(
        task_id='edit_data',
        python_callable=preprocessing)

    # Task: 3
    upload_data = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)

    #running airflow
    fetch_data_pg >> edit_data >> upload_data