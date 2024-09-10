import pandas as pd
import re
import warnings
import datetime
from pathlib import Path
from assets.config.config_ingestion import logger
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)


class Saneamento:
    '''
    > Attributes
    -------
    dataframe: (df) df a ser subtmetido pelo processamento
    metadados: (dict) dicionario com todas as informações de para o tratamento
    
    > Methods
    -------
    select_rename(): Realize a seleção dos dados de interesse, assim como renomeação de colunas        
    '''
    
    def __init__ (self, data, metadados): #self serve para referenciar o objeto e inicializar ele
        self.data = data
        self.metadados = metadados 
    
    def select_rename(self): #aqui não precisamos repetir os parametros data e metadados, pois ja foi referenciado acima 
        self.data = self.data.loc[:,self.metadados['cols_originais']] 
        self.data.rename(columns = self.metadados['cols_renamed'], inplace= True)
        
        pass
    



def corrige_hora(hr_str, dct_hora = {1:"000?",2:"00?",3:"0?",4:"?"}):
    if hr_str == "2400":
        return "00:00"
    elif (len(hr_str) == 2) & (int(hr_str) <= 12):
        return f"0{hr_str[0]}:{hr_str[1]}0"
    else:
        hora = dct_hora[len(hr_str)].replace("?", hr_str)
        return f"{hora[:2]}:{hora[2:]}"
    
    
if __name__ == "__main__":
    
    #CAMINHO DO PROJETO
    caminho_projeto = Path()
    print('caminho\n')
    # print(caminho_projeto.absolute())
    #* C:\Users\vcamargg\Impacta\AULA_05
    
    #CAMINHO DO ARQUIVO
    caminho_arquivo = Path(__file__)
    # print(caminho_arquivo)
    #* c:\Users\vcamargg\Impacta\AULA_05\flights-data-pipeline\src\assets\cleansing.py
    
    #CAMINHO DO PAI DO ARQUIVO
    print()
    # print(caminho_arquivo.parent)
    #* c:\Users\vcamargg\Impacta\AULA_05\flights-data-pipeline\src\assets
    
    #Criando arquivos e caminhos > utilizamos a barra normal / (true div), pois ele automaticamente coloca como barra invertida \ (backslash)
    ideias = caminho_arquivo.parent / 'ideias'
    print(ideias / 'ideias.txt')
    #* c:\Users\vcamargg\Impacta\AULA_05\flights-data-pipeline\src\assets\ideias\ideias.txt
    
    #Criando o arquivo e escrevendo dentro dele
    '''
    caminho_arquivo2 = Path.home() / 'Downloads' /'arquivo_test.txt'
    caminho_arquivo2.touch()                        #salva o arquivo
    
    with open (caminho_arquivo2, 'a+') as file:
        file.write('uma linha \r\n')
        file.write('duas linhas \r\n')
        file.write('tres linhas \r\n')
        
    print(caminho_arquivo2.read_text())
    '''
    #Criando uma pasta nova
    caminho_arquivo3 = Path.home() / 'Downloads'/ 'Teste_Path_lib'
    caminho_arquivo3.mkdir(exist_ok=True) #se existir, não faça nada