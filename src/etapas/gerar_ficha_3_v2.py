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

# Carregando variáveis de ambiente
dotenv_path = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '../../', '.env'))
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
    cnpj_produtos = "65369985000334"


if nome_empresa.lower() == 'ladakh':
    cnpj = "07318052000150"
    cnpj_produtos = "07318052000150"


if nome_empresa.lower() == 'sonda':
    cnpj = "01937635001316"
    cnpj_produtos = "01937635001316"


if nome_empresa.lower() == 'mensa':
    cnpj = "10290457000484"
    cnpj_produtos = "10290457000212"

if nome_empresa.lower() == 'casa mimosa':
    cnpj = "62978978000180"
    cnpj_produtos = "62978978000180"
    cnpjs = [cnpj]


try:
    tabela_2 = ler_arquivo_para_dataframe(
        bucket_name, f'Cat42/{nome_empresa.title()}/Tabela 2/tabela_2_{nome_empresa.title()}_{cnpj}.xlsx', file_type='xlsx')

except:
    tabela_2_p1 = ler_arquivo_para_dataframe(
        bucket_name, f'Cat42/{nome_empresa.title()}/Tabela 2/tabela_2_{nome_empresa.title()}_{cnpj}_p1.xlsx', file_type='xlsx')
    tabela_2_p2 = ler_arquivo_para_dataframe(
        bucket_name, f'Cat42/{nome_empresa.title()}/Tabela 2/tabela_2_{nome_empresa.title()}_{cnpj}_p2.xlsx', file_type='xlsx')
    tabela_2_p3 = ler_arquivo_para_dataframe(
        bucket_name, f'Cat42/{nome_empresa.title()}/Tabela 2/tabela_2_{nome_empresa.title()}_{cnpj}_p3.xlsx', file_type='xlsx')

    tabela_2 = pd.concat([tabela_2_p1, tabela_2_p2, tabela_2_p3])

tabela_2['COD_ITEM'] = tabela_2['COD_ITEM'].astype(str)

ficha_3 = calcular_ressarcimento(tabela_2, cnpj_produtos)

gti = input('Precisa de GTI?: ')

if gti.lower() == 'sim':

    meta_ = input('Qual é o valor da meta de ressarcimento ?: ')
    meta = float(meta_)
    if ficha_3['VLR_RESSARCIMENTO'].sum() < meta:
        top_prods = input('Qual o top de produtos para a conta?: ')
        ficha_3_final = gti_pra_cima(
            ficha_3, meta_ressarc=meta, top_prods=int(top_prods), cnpj_produtos=cnpj_produtos)
    else:
        # ficha_3_final = gti_pra_cima(ficha_3, meta_ressarc=meta)
        ficha_3_final = ficha_3
else:
    ficha_3_final = ficha_3

print('Ressarcimento total:', f"R$ {ficha_3_final['VLR_RESSARCIMENTO'].sum():,.2f}".replace(
    ',', 'v').replace('.', ',').replace('v', '.'))
print('Complemento total:', f"R$ {ficha_3_final['VLR_COMPLEMENTO'].sum():,.2f}".replace(
    ',', 'v').replace('.', ',').replace('v', '.'))

print('Ressarcimento total CE 1:', f"R$ {ficha_3_final[ficha_3_final['COD_LEGAL'] == 1]['VLR_RESSARCIMENTO'].sum():,.2f}".replace(
    ',', 'v').replace('.', ',').replace('v', '.'))
print('Ressarcimento total CE 2:', f"R$ {ficha_3_final[ficha_3_final['COD_LEGAL'] == 2]['VLR_RESSARCIMENTO'].sum():,.2f}".replace(
    ',', 'v').replace('.', ',').replace('v', '.'))
print('Ressarcimento total CE 4:', f"R$ {ficha_3_final[ficha_3_final['COD_LEGAL'] == 4]['VLR_RESSARCIMENTO'].sum():,.2f}".replace(
    ',', 'v').replace('.', ',').replace('v', '.'))

if ficha_3_final.shape[0] > 1000000:
    ficha_3_final_p1 = ficha_3_final[:ficha_3_final.shape[0]//3]
    ficha_3_final_p2 = ficha_3_final[ficha_3_final.shape[0] //
                                     3:(ficha_3_final.shape[0]*2)//3]
    ficha_3_final_p3 = ficha_3_final[(ficha_3_final.shape[0]*2)//3:]

    salvar_dataframe_no_s3(ficha_3_final_p1, bucket_name=bucket_name, s3_key=f'Cat42/{nome_empresa.title()}/Ficha 3/ficha_3_{nome_empresa.title()}_{cnpj}_p1_v2.xlsx',
                           file_type='xlsx')
    salvar_dataframe_no_s3(ficha_3_final_p2, bucket_name=bucket_name, s3_key=f'Cat42/{nome_empresa.title()}/Ficha 3/ficha_3_{nome_empresa.title()}_{cnpj}_p2_v2.xlsx',
                           file_type='xlsx')
    salvar_dataframe_no_s3(ficha_3_final_p3, bucket_name=bucket_name, s3_key=f'Cat42/{nome_empresa.title()}/Ficha 3/ficha_3_{nome_empresa.title()}_{cnpj}_p3_v2.xlsx',
                           file_type='xlsx')
else:
    salvar_dataframe_no_s3(ficha_3_final, bucket_name=bucket_name, s3_key=f'Cat42/{nome_empresa.title()}/Ficha 3/ficha_3_{nome_empresa.title()}_{cnpj}_v2.xlsx',
                           file_type='xlsx')
