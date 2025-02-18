# Importação das bibliotecas a serem utilizadas durante todo o processo

import pandas as pd
import numpy as np
from datetime import datetime
import time
import math
import random
import sys
import pytz
import os
from dotenv import load_dotenv
import boto3

from src.utils.calcular_ressarcimento import calcular_ressarcimento
from src.utils.calcular_gti import gti_pra_cima, gti_pra_baixo
from src.utils.ler_arquivos import ler_arquivo_para_dataframe, salvar_dataframe_no_s3
# Leitura do arquivo da tabela 2

start = time.time()

# Carregando variáveis de ambiente
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
print(f"Carregando .env de: {dotenv_path}")
load_dotenv(dotenv_path, override=True)

bucket_name = '4btaxtech'

s3 = boto3.client('s3', 
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), 
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                  region_name=os.getenv('AWS_DEFAULT_REGION')
                  )

# Definição da empresa a ser gerado os dados
nome_empresa = input("Digite o nome da empresa: ")

if nome_empresa.lower() == 'tateti':
    cnpj = "65369985000334"


tabela_2 = ler_arquivo_para_dataframe(bucket_name, f'Cat42/{nome_empresa.title()}/Tabela 2/tabela_2_{nome_empresa.title()}_{cnpj}.xlsx', file_type='xlsx')
tabela_2['COD_ITEM'] = tabela_2['COD_ITEM'].astype(str)

ficha_3 = calcular_ressarcimento(tabela_2)

gti = input('Precisa de GTI?: ')

if gti.lower() == 'sim':

    meta = 0.085
    if ficha_3['VLR_RESSARCIMENTO'].sum() / ficha_3[ficha_3['CFOP'] == 5405]['VALOR'].sum()  > 0.085:
       ficha_3_final = gti_pra_baixo(ficha_3, meta)
    else:
        ficha_3_final = gti_pra_cima(ficha_3, meta)
else:
    ficha_3_final = ficha_3

salvar_dataframe_no_s3(ficha_3_final, bucket_name=bucket_name,s3_key=f'Cat42/{nome_empresa.title()}/Ficha 3/ficha_3_{nome_empresa.title()}_{cnpj}.xlsx',
                       file_type='xlsx')