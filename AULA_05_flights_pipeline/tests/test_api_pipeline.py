""" 
import sys
sys.path.append('../src')
"""
from pipeline import api_pipeline
from assets.utils import sqlite_action
from assets.config.config_ingestion import metadados

df_api_cols =  ["data_voo",  "tempo_voo_esperado", "datetime_partida_formatted", "origem_formatted", "companhia_formatted",  "dia_semana",  "distancia", "horario"]

def test_api_pipeline_cols():
    df = api_pipeline(metadados["api_ingestion"], "dev", "EWR", "IAH")
    assert set(df.columns) == set(df_api_cols)