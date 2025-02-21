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

from src.utils.calcular_ressarcimento_v2 import calcular_ressarcimento
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
    cnpj = "65369985000504"

if nome_empresa.lower() == 'ladakh':
    cnpj = "07318052000150"


tabela_2 = ler_arquivo_para_dataframe(bucket_name, f'Cat42/{nome_empresa.title()}/Tabela 2/tabela_2_{nome_empresa.title()}_{cnpj}.xlsx', file_type='xlsx')
tabela_2['COD_ITEM'] = tabela_2['COD_ITEM'].astype(str)

ficha_3 = calcular_ressarcimento(tabela_2)

gti = input('Precisa de GTI?: ')

if gti.lower() == 'sim':

    meta_ = input('Qual é o valor da meta de ressarcimento ?: ')
    meta = float(meta_)
    if ficha_3['VLR_RESSARCIMENTO'].sum() < meta:
       top_prods = input('Qual o top de produtos para a conta?: ')
       ficha_3_final = gti_pra_cima(ficha_3, meta_ressarc=meta, top_prods=int(top_prods))
    else:
        # ficha_3_final = gti_pra_cima(ficha_3, meta_ressarc=meta)
        ficha_3_final = ficha_3
else:
    ficha_3_final = ficha_3

salvar_dataframe_no_s3(ficha_3_final, bucket_name=bucket_name,s3_key=f'Cat42/{nome_empresa.title()}/Ficha 3/ficha_3_{nome_empresa.title()}_{cnpj}_v2.xlsx',
                       file_type='xlsx')