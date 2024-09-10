import datetime
import pandas as pd
import requests
import json
from assets.cleansing import corrige_hora
from assets.utils import sqlite_action, data_sanitize, feat_eng
from assets.config.config_ingestion import logger, metadados, qcreate_table, key, api_sample


def create_table():
    sqlite_action("execute", query=qcreate_table)

def db_pipeline(metadados):
    logger.info(f'db-pipeline iniciado; {datetime.datetime.now()}')
    df = pd.read_csv(metadados["path"])
    df["data_voo"] = pd.to_datetime(df[['year', 'month', 'day']]) 
    data = data_sanitize(df, metadados)

    for col in metadados["corrige_hr"]:
        lst_col = data.loc[:,col].apply(lambda x: corrige_hora(x))
        data[f'{col}_formatted'] = pd.to_datetime(data.loc[:,'data_voo'].astype(str) + " " + lst_col)
    
    sqlite_action("insert", df=feat_eng(data, "db-pipeline", metadados), table="nyflights")
    logger.info(f'db-pipeline concluído; {datetime.datetime.now()}')
    
def api_pipeline(metadados, env, origem, destino):
    logger.info(f'api-pipeline iniciado; {datetime.datetime.now()}')
    
    if env == "prod":
        api_result = requests.get(
        metadados["path"],
        params = {
            'access_key': key,
            "dep_iata" : origem,
            "arr_iata" : destino,
            "limit" : 1
        })
        api_response = api_result.json()
    else: 
        api_response=api_sample

    df = pd.json_normalize(api_response["data"])
    
    data = feat_eng(
        data_sanitize(df, metadados),
        "api-pipeline",
        metadados,
        origem=origem,
        destino=destino)
    
    logger.info(f'api-pipeline concluído; {datetime.datetime.now()}')
    return data

if __name__ == "__main__":
    create_table()
    db_pipeline(metadados["db_ingestion"])
    print(sqlite_action("execute", query="SELECT * from nyflights LIMIT 5"))
    print(api_pipeline(metadados["api_ingestion"], "dev", "EWR", "IAH").head())