
import os
import logging
import json
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(filename=os.getenv('PIPE_FILE_PATH'), level=logging.INFO)
logger = logging.getLogger()
nyflightsDB = os.getenv('DB_PATH')
key = os.getenv('ACCESS_KEY')

with open(os.getenv('SAMPLE_PATH'), 'r') as file:
    api_sample = json.loads(file.read())

metadados = {
    "api_ingestion" : {
        "path":'https://api.aviationstack.com/v1/flights',
        "cols_originais" : [
            'flight_date',
            'departure.iata',
            'arrival.iata',
            'airline.iata',
            'departure.estimated',
            'arrival.estimated'
            ],
        "cols_renamed" : {
            'flight_date':"data_voo",
            'departure.iata':"origem",
            'arrival.iata':"destino",
            'airline.iata':"companhia",
            'departure.estimated':"datetime_partida_formatted",
            'arrival.estimated':"datetime_chegada_formatted"
        },
        "tipos" : {
            'data_voo': "datetime",
            'origem' : "string",
            'destino': "string",
            'companhia': "string",
            'datetime_partida_formatted': "datetime",
            'datetime_chegada_formatted': "datetime"
        },
        "cols_chaves":[
            "datetime_partida_formatted",
            "datetime_chegada_formatted",
            "data_voo",
            "companhia"
        ],
        "null_tolerance":{
            "data_voo":0,
            "datetime_partida_formatted":0,
            "datetime_chegada_formatted":0,
            "origem":0.05,
            "destino":0.05,
            "companhia":0
        },
        "fillna":{
            "tempo_voo":0
        },
        "std_str": [
            'origem',
            'destino',
            'companhia']
    },
    "db_ingestion":{
        "path": os.getenv('DATA_PATH'),
        "cols_originais":[
            "data_voo",
            "dep_time",
            "arr_time",
            "origin",
            "dest",
            "carrier",
            "flight",
            "tailnum",
            "air_time",
            "distance"
            ],
        "cols_renamed":{
            "data_voo":"data_voo",
            "dep_time":"datetime_partida",
            "arr_time":"datetime_chegada",
            "origin":"origem",
            "dest":"destino",
            "carrier":"companhia",
            "flight":"id_voo",
            "tailnum":"id_aeronave",
            "air_time":"tempo_voo",
            "distance":"distancia"
        },
        "tipos":{
            "data_voo":"datetime",
            "datetime_partida":"string",
            "datetime_chegada":"string",
            "origem":"string",
            "destino":"string",
            "companhia":"string",
            "id_voo":"string",
            "id_aeronave":"string",
            "tempo_voo":"float",
            "distancia":"float"
        },
        "cols_chaves":[
            "datetime_partida",
            "datetime_chegada",
            "companhia",
            "id_voo"
        ],
        "fillna":{
            "tempo_voo":0
        },
        "null_tolerance":{
            "data_voo":0,
            "datetime_partida":0,
            "datetime_chegada":0,
            "origem":0.05,
            "destino":0.05,
            "companhia":0,
            "id_voo":0,
            "id_aeronave":0.05,
            "tempo_voo":0.05,
            "distancia":0.05
        },
        "std_str":[
            'origem',
            'destino',
            'companhia',
            "id_voo",
            "id_aeronave"
        ],
        "corrige_hr":[
            "datetime_partida",
            "datetime_chegada"
        ],
        "cols_db":[
            "data_voo",
            "companhia_formatted",
            "id_aeronave_formatted",
            "datetime_partida_formatted",
            "datetime_chegada_formatted",
            "origem_formatted",
            "destino_formatted",
            "tempo_voo",
            "distancia",
            "tempo_voo_esperado",
            "tempo_voo_hr",
            "atraso",
            "dia_semana",
            "horario",
            "flg_status"
            ]
        }
    }
    
qcreate_table = '''
    CREATE TABLE IF NOT EXISTS nyflights (
        id INTEGER PRIMARY KEY,
        data_voo DATETIME NOT NULL,
        companhia_formatted TEXT NOT NULL,
        id_aeronave_formatted TEXT,
        datetime_partida_formatted DATETIME NOT NULL,
        datetime_chegada_formatted DATETIME NOT NULL,
        origem_formatted TEXT NOT NULL,
        destino_formatted TEXT NOT NULL,
        tempo_voo FLOAT,
        distancia FLOAT,
        tempo_voo_esperado FLOAT,
        tempo_voo_hr FLOAT,
        atraso FLOAT,
        dia_semana TEXT NOT NULL,
        horario TEXT NOT NULL,
        flg_status TEXT NOT NULL
    )
    '''