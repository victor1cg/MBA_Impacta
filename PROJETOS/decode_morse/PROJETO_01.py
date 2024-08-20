import os
import sys
import datetime
from config import dict_morse


def decode_morse (msg):
    '''função que decodifica frases de codigo morse'''
    frase = []
    palavras = msg.split("  ") #split a cada palavra
    x = len(palavras)
    y=0
    
    for palavra in palavras:
        letra = palavra.split(" ")
        for letra_dict in letra:
            letra_dict = dict_morse.get(letra_dict)
            frase.append(letra_dict)
        
    #condicional para inserir espaço entre as palavras
        y+=1
        if  x > y:
            frase.append(" ")
            
    #timestamp
    now = datetime.datetime.now()
    now = now.replace(second=0, microsecond=0)

    return print("".join(frase)+" - "+str(now))



if __name__ == '__main__' :
    decode_morse("-- -... .-  .. -- .--. .- -.-. - .-")
    decode_morse(".--. .- ---  ..-. .-. .- -. -.-. . ...")
    decode_morse(".--. -.--  .-.. . --. .- .-..")
