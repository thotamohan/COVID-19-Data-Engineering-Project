import os
import tempfile
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import requests
import json
import pandas as pd
import time
from dateutil import parser


class RestAPICallOperator(BaseOperator):
    '''
    A single DAG to for the entire ETL process, API response is made, and appropriate transformations are made
    to store the transformed database into tables on Postgres db.
    
    Input: BaseOperator
    Ouptut: created Tables in Postgres
    '''
    
    ui_color = '#a0e08c'

    def __init__(self,*args,**kwargs):
        super(RestAPICallOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        #to load date in county table
        timestamp = str(parser.parse(time.ctime()))
        
        #api call is made
        response=requests.get("https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD")
        #json file read 
        data=json.loads(response.content)

        #getting all the column names
        column_names=[]
        for columns in data['meta']['view']['columns']:
            column_names.append(columns['name'])

        tempDf = pd.DataFrame(data.get("data"), columns = [x for x in column_names])
    
        columnNames=["Test Date","New Positives","Cumulative Number of Positives","Total Number of Tests Performed",\
                     "Cumulative Number of Tests Performed","County"]
        finalDF=tempDf[columnNames]
        convert_DataType={"New Positives":int,"Cumulative Number of Positives":int,"Total Number of Tests Performed":int,\
                         "Cumulative Number of Tests Performed":int,"County":str}
        #finalDF with appropriate data types and required columns
        finalDF=finalDF.astype(convert_DataType)

        #get Date from timestamp in Test Date
        finalDF['Test Date']=finalDF['Test Date'].apply(lambda x: x.split('T')[0]).apply(pd.to_datetime)
        finalDF['Load Date']=[timestamp.split(' ')[0]]*len(finalDF)
        
        #data validation check, loading only records where county is not null
        finalDF=finalDF.loc[finalDF['County'].notnull()]
        #finalDF.insert(5,"Load Date",[timestamp.split(' ')[0]]*len(finalDF),True)


        
        
        #df is grouped by county
        for county in finalDF.groupby(['County']):
            #postgres engine is created to store individual county dataframe as a county table 
            engine = create_engine('postgresql://airflow:airflow@beb17d42bc74:5432/airflow')
            county[1].drop("County",axis=1).to_sql(str(county[0]), engine,if_exists='append')