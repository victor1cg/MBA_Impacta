import os
from dotenv import load_dotenv

load_dotenv()

model_path = os.getenv('MODEL_PATH')

dist_query = '''
    SELECT distancia FROM nyflights 
    WHERE origem_formatted LIKE '?dest' 
    AND destino_formatted LIKE '?orig' LIMIT 1 
'''

categorical_cols = ["origem_formatted","companhia_formatted",  "dia_semana", "horario"]
num_cols = ["tempo_voo_esperado", "distancia"]

cols_pre_proc = [
    "data_voo",
    "tempo_voo_esperado",
    "datetime_partida_formatted",
    "origem_formatted","companhia_formatted", 
    "dia_semana",
    "distancia",
    "horario"]

cols_modelo =  [
    'distancia',
    "tempo_voo_esperado",
    'companhia_formatted_9E',
    'companhia_formatted_AA',
    'companhia_formatted_AS',
    'companhia_formatted_B6',
    'companhia_formatted_DL',
    'companhia_formatted_EV',
    'companhia_formatted_F9',
    'companhia_formatted_FL',
    'companhia_formatted_HA',
    'companhia_formatted_MQ',
    'companhia_formatted_OO',
    'companhia_formatted_UA',
    'companhia_formatted_US',
    'companhia_formatted_VX',
    'companhia_formatted_WN',
    'companhia_formatted_YV',
    'origem_formatted_EWR',
    'origem_formatted_JFK',
    'origem_formatted_LGA',
    'dia_semana_0',
    'dia_semana_1',
    'dia_semana_2',
    'dia_semana_3',
    'dia_semana_4',
    'dia_semana_5',
    'dia_semana_6',
    'horario_MADRUGADA',
    'horario_MANHA',
    'horario_NOITE',
    'horario_TARDE'
]