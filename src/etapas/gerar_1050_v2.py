# Importação das bibliotecas a serem utilizadas durante todo o processo

import pandas as pd
import numpy as np
from datetime import datetime
import math
import sys
import os
from dotenv import load_dotenv
import boto3

from src.utils.ler_arquivos import ler_arquivo_para_dataframe, salvar_dataframe_no_s3

# Carregando variáveis de ambiente
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
print(f"Carregando .env de: {dotenv_path}")
load_dotenv(dotenv_path, override=True)

# Definição da empresa a ser gerado os dados
nome_empresa = input("Digite o nome da empresa: ")

if nome_empresa.lower() == 'tateti':
    cnpj = "65369985000504"

if nome_empresa.lower() == 'ladakh':
    cnpj = "07318052000150"

# Variáveis para acesso ao s3
bucket_name = '4btaxtech'

s3 = boto3.client('s3', 
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), 
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                  region_name=os.getenv('AWS_DEFAULT_REGION')
                  )

df_2 = ler_arquivo_para_dataframe(bucket_name=bucket_name, s3_key=f'Cat42/{nome_empresa.title()}/Ficha 3/ficha_3_{nome_empresa.title()}_{cnpj}_v2.xlsx',
                                  file_type='xlsx')

df = df_2[['DATA','COD_ITEM', 'QTD_INI', 'ICMS_INI', 'SALDO_FINAL_MES_QTD', 'SALDO_FINAL_MES_ICMS']]

# Checagem de ICMS negativo

if df[df['SALDO_FINAL_MES_ICMS'] < 0].shape[0] > 0:
    print('EXISTE SALDO DE ICMS_NEGATIVO, FAVOR CHECAR!!')
    sys.exit()

df['DATA'] = df['DATA'].astype(str)
df['REF'] = [datetime.strptime(x, '%Y-%m-%d').strftime('%Y%m') for x in df['DATA']]
df['REF'] = df['REF'].astype(str)

grouped = df.groupby(['COD_ITEM', 'REF']).agg(
    QTD_INI=('QTD_INI', 'first'),
    ICMS_INI=('ICMS_INI', 'first'),
    SALDO_FINAL_MES_QTD=('SALDO_FINAL_MES_QTD', 'last'),
    SALDO_FINAL_MES_ICMS=('SALDO_FINAL_MES_ICMS', 'last')
).reset_index()

df_1050 = grouped.copy()

df_1050['QTD_INI'] = np.where(df_1050['QTD_INI'].isnull(),
                             df_1050['SALDO_FINAL_MES_QTD'].shift(),
                             df_1050['QTD_INI'])

df_1050['ICMS_INI'] = np.where(df_1050['ICMS_INI'].isnull(),
                             df_1050['SALDO_FINAL_MES_ICMS'].shift(),
                             df_1050['ICMS_INI'])

df_1050['ICMS_INI'] = np.where(df_1050['QTD_INI'] == 0,
                              0,
                               df_1050['ICMS_INI'])

if len(df_1050[df_1050['SALDO_FINAL_MES_ICMS'] < 0]) > 0:
    print('ATENÇAO. EXISTE SALDO DE ICMS NEGATIVO')
    print('ATENÇÃO. FAVOR CHECAR SOBRE VALOR DE SALDO DE ICMS NEGATIVO')
    sys.exit()

if len(df_1050[df_1050['SALDO_FINAL_MES_QTD'] < 0]) > 0:
    print('ATENÇAO. EXISTE SALDO DE QUANTIDADE NEGATIVO')
    print('ATENÇÃO. FAVOR CHECAR SOBRE VALOR DE SALDO DE QUANTIDADE NEGATIVO')
    sys.exit()

if df_1050[(df_1050['QTD_INI'] == 0) & (df_1050['ICMS_INI'] != 0)].shape[0] > 0:
    mensagem = 'ATENÇÂO. ICMS DIFERENTE DE ZERO PARA QUANTIDADE IGUAL A ZERO. FAVOR CHECAR'
    print(mensagem)
    sys.exit()

if df_1050[(df_1050['SALDO_FINAL_MES_QTD'] == 0) & (df_1050['SALDO_FINAL_MES_ICMS'] != 0)].shape[0] > 0:
    mensagem = 'ATENÇÂO. ICMS DIFERENTE DE ZERO PARA QUANTIDADE IGUAL A ZERO. FAVOR CHECAR'
    print(mensagem)
    sys.exit()

salvar_dataframe_no_s3(df_1050, bucket_name,
                       s3_key=f'Cat42/{nome_empresa.title()}/1050/1050_{nome_empresa.title()}_{cnpj}_v2.xlsx',
                       file_type='xlsx')