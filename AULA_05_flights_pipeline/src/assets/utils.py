import datetime
import sqlite3
import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from assets.cleansing import Saneamento
from assets.config.config_ingestion import logger, nyflightsDB
from assets.config.config_model import dist_query, cols_pre_proc


def data_sanitize(df, metadados):

    san = Saneamento(df, metadados)

    #esse pacote vem do sklearn, a ideia é aplicar metodos ou funções de forma sequencial
    Pipeline([
        ('select_rename', san.select_rename()),
        ('exclui nulos',san.null_exclude()),
        ('convert_data_type', san.convert_data_type()),
        ('padroniza_str',san.string_std()),
        ('valida_nulos', san.null_check())
        ])
    
    return san.fetch_df()

def sqlite_action(method, **kwargs):
    try:
        conn = sqlite3.connect(nyflightsDB)
        c = conn.cursor()
        logger.info(f'Conexao com banco estabelecida ; {datetime.datetime.now()}')
    except:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')
    
    df = kwargs.get('df')
    query = kwargs.get('query')
    table = kwargs.get('table')
    fetch = None

    if method == "insert":
        df.to_sql(name=table, con=conn, if_exists='replace')
        logger.info(f'Dados salvos com sucesso; {datetime.datetime.now()}')
    elif method == "execute":
        c.execute(query)
        fetch = c.fetchall()
        logger.info(f'Query executada com sucesso; {datetime.datetime.now()}')
    else:
        logger.error(f'Metodo nao definido; {datetime.datetime.now()}')
    
    conn.commit()
    conn.close()
    return fetch
                        
def classifica_hora(hra):
    if 0 <= hra < 6: return "MADRUGADA"
    elif 6 <= hra < 12: return "MANHA"
    elif 12 <= hra < 18: return "TARDE"
    else: return "NOITE"

def feat_eng(df, step, metadados, **kwargs):
    data = df.copy()
    data["tempo_voo_esperado"] = (
        data["datetime_chegada_formatted"]-data["datetime_partida_formatted"]
        ) / pd.Timedelta(hours=1)
    data["dia_semana"] = data["data_voo"].dt.day_of_week 
    data["horario"] = data["datetime_partida_formatted"].dt.hour.apply(
                            lambda x: classifica_hora(x))
    if step == "db-pipeline":
        df["datetime_chegada_formatted"] = np.where(
            df["datetime_partida_formatted"] > df["datetime_chegada_formatted"],
            df["datetime_chegada_formatted"] + pd.Timedelta(days=1),
            df["datetime_chegada_formatted"]
            )
        data["tempo_voo_hr"] = data["tempo_voo"] /60
        data["atraso"] = data["tempo_voo_hr"] - data["tempo_voo_esperado"]
        data["flg_status"] = np.where(data["atraso"] > 0.5, "ATRASO", "ONTIME")
        return data[metadados["cols_db"]]
    else:
        query = dist_query.replace("?dest", kwargs.get("origem"))
        query = query.replace("?orig", kwargs.get("destino"))
        distancia = sqlite_action("execute", query=query, table="nyflights")
        data["distancia"] = distancia[0][0]
        return data[cols_pre_proc]



