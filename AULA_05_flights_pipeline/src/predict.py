import pandas as pd
import pickle
import datetime
import random
from sklearn.preprocessing import StandardScaler
from pipeline import api_pipeline
from assets.config.config_model import cols_modelo, model_path, categorical_cols, num_cols
from assets.config.config_ingestion import logger, metadados

def pre_process(df):
    logger.info(f'pre_process iniciado; {datetime.datetime.now()}')
    df_categorical = df[categorical_cols].copy()
    for col in categorical_cols:
        df_categorical = pd.get_dummies(df_categorical, columns=[col])
    df_std = pd.DataFrame(StandardScaler().fit_transform(df[num_cols]),columns=num_cols)
    df_std = df[num_cols]
    df_processed = pd.concat([df_std, df_categorical], axis=1)
    logger.info(f'pre_process finalizado; {datetime.datetime.now()}')
    return df_processed    

def predict(df):
    logger.info(f'predict iniciado; {datetime.datetime.now()}')
    df_processed = pre_process(df)
    cols_missing = set(cols_modelo) - set(df_processed.columns)
    for col in cols_missing: df_processed[col] = False
    df_processed = df_processed[cols_modelo]
    clf_reg = pickle.load(open(model_path, 'rb'))
    predict = clf_reg.predict(df_processed)
    predict = StandardScaler().fit_transform(predict.reshape(-1,1))
    logger.info(f'predict finalizado; {datetime.datetime.now()}')
    ind = random.choice(range(len(predict)))
    tempo = df_processed["tempo_voo_esperado"][ind] +  predict[ind][0]
    return round(tempo,1)


if __name__ == "__main__":
    df = api_pipeline(metadados["api_ingestion"], "dev", "EWR", "IAH")
    print(predict(df))