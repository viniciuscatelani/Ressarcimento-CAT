# Importação das bibliotecas necessárias
import pandas as pd
import numpy as np
import psycopg2
import math
from datetime import datetime
import sys
import os
import openpyxl
import boto3
from io import BytesIO
from dotenv import load_dotenv

from src.utils.ler_arquivos import ler_arquivo_para_dataframe, salvar_dataframe_no_s3

# Carregando variáveis de ambiente
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
print(f"Carregando .env de: {dotenv_path}")
load_dotenv(dotenv_path, override=True)

nome_empresa = input("Digite o nome da empresa: ")

if nome_empresa.lower() == 'tateti':
    cnpj = "65369985000504"
    cnpj_produtos = "65369985000334"
    cnpjs = [cnpj]

if nome_empresa.lower() == 'ladakh':
    cnpj = "07318052000150"
    cnpj_produtos = "07318052000150"
    cnpjs = [cnpj]

# Variáveis para acesso ao s3
bucket_name = '4btaxtech'

s3 = boto3.client('s3', 
                  aws_access_key_id='AKIA4RCAOBRSFONXEUTE', 
                  aws_secret_access_key='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',
                  region_name='us-east-1'
                  )

tabela_2 = ler_arquivo_para_dataframe(bucket_name, f'Cat42/{nome_empresa.title()}/Tabela 2/tabela_2_{nome_empresa.title()}_{cnpj}.xlsx', file_type='xlsx')

tabela_mapeada = (
        tabela_2.groupby(['COD_ITEM', 'IND_OPER'])['UNIDADE']
        .agg(lambda x: list(x.unique()))  # Obter valores únicos de 'UNIDADE'
        .reset_index(name='UNIDADES')     # Renomear a coluna de saída
    )

salvar_dataframe_no_s3(tabela_mapeada,
                       bucket_name,
                       s3_key=f'Cat42/{nome_empresa.title()}/analise_unidades_{nome_empresa}_{cnpj}.xlsx', file_type='xlsx')