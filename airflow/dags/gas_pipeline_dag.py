import os
import requests
import pandas as pd
from airflow.decorators import dag, task
from dotenv import load_dotenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine

load_dotenv()
API_KEY = os.getenv("API_KEY")
DB_URL = os.getenv("DB_URL")

default_args = {
    'owner': 'deecodes',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

@dag(dag_id='gas_prices_dag',  default_args=default_args, start_date=datetime(2025, 5, 26), schedule='@daily')
def gas_prices():
    @task
    def extract_data():
        url = "https://api.collectapi.com/gasPrice/stateUsaPrice?state=WA" 
        headers = {
            'content-type': "application/json",
            'authorization': f"{API_KEY}"
        }

        response = requests.get(url=url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Requests error: {response.status_code}, {response.text}")

    @task
    def transform_data(data):
        try:
            now = datetime.now()
            df = pd.DataFrame({
                'city_name': [i['name'] for i in data['result']['cities']],
                'gasoline' : [i['gasoline'] for i in data['result']['cities']],
                'mid_grade' : [i['midGrade'] for i in data['result']['cities']],
                'premium' : [i['premium'] for i in data['result']['cities']],
                'date' : [now] * len(data['result']['cities'])
            })
            # set datetime column into a datetime obj and set as index
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            # transform dtypes of columns from object to float64
            df['gasoline'] = df['gasoline'].astype(float)
            df['mid_grade'] = df['mid_grade'].astype(float)
            df['premium'] = df['premium'].astype(float)

            # return a clean dataframe ready for loading
            return df
        except Exception as e:
            print(f"Dataframe error: {str(e)}")
    
    @task
    def load_to_db(df):
        try:
            engine = create_engine(DB_URL)
            df.to_sql(name='washington_gas_prices', con=engine, schema='public', index=False, if_exists='append')
            print("Data loaded successfully!")
        except Exception as e:
            print(f"Postgres loading error: {str(e)}")

     

    data = extract_data()
    df = transform_data(data)
    load_to_db(df)

gas_dag = gas_prices()