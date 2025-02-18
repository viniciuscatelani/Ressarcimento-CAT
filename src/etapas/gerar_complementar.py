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
from dotenv import load_dotenv



# Carregando variáveis de ambiente
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
print(f"Carregando .env de: {dotenv_path}")
load_dotenv(dotenv_path, override=True)


# Diretório do script atual (dentro de 'src/etapas')
current_dir = os.path.dirname(os.path.abspath(__file__))

# Caminho do diretório 'src'
src_dir = os.path.abspath(os.path.join(current_dir, '..'))

# Adiciona 'src' ao sys.path
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

print("Caminhos no sys.path:", sys.path)

from utils.ler_arquivos import ler_arquivo_para_dataframe, salvar_dataframe_no_s3
# Definição da empresa a ser gerado os dados
nome_empresa = input("Digite o nome da empresa: ")

if nome_empresa.lower() == 'tateti':
    cnpj = "65369985000504"

connection = psycopg2.connect(
        user=os.getenv('DATABASE_USER'),
        password=os.getenv('DATABASE_PASS'),
        host=os.getenv('DATABASE_HOST'),
        port="3361",
        database=os.getenv('DATABASE_NAME')
    )

# Variáveis para acesso ao s3
bucket_name = '4btaxtech'

s3 = boto3.client('s3', 
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), 
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                  region_name=os.getenv('AWS_DEFAULT_REGION')
                  )

# Leitura da tabela 1 gerada em etapa anterior
tabela_1 = ler_arquivo_para_dataframe(bucket_name, 'Cat42/Tateti/Tabela 1/tabela_1.xlsx', file_type='xlsx')
print(tabela_1)

tabela_1['Código Produto ou Serviço'] = tabela_1['Código Produto ou Serviço'].astype(str)

# Leitura das informações da tabela de produtos
# do banco de dados para um dataframe
query = f'SELECT * from produtos'
df_produtos = pd.read_sql_query(query, connection)

# Formatação da coluna de data
df_produtos['data_valida'] = df_produtos['data_valida'].str[:10]
df_produtos['data_valida'] = pd.to_datetime(df_produtos['data_valida'], format='%d/%m/%Y')

# Leitura da tabela de EFD do banco de dados
# para um dataframe

query = f'SELECT * FROM modelo55 WHERE cnpj = {cnpj};'
efd = pd.read_sql_query(query, connection)
if efd.shape[0] == 0:
    print(f'EFD Modelo 55 da loja {nome_empresa}:{cnpj} não existe no banco.')
    sys.exit()

# Seleção dos produtos a serem utilizados

prods_comp = tabela_1[tabela_1['Tipo'] == 'entrada'].copy()
prods_comp = prods_comp[prods_comp['Chave Acesso NFe'].str.slice(6, 20) == cnpj]

df_produtos['anvisa'] = df_produtos['anvisa'].replace('None', np.nan)
df_produtos.replace('None', np.nan, inplace = True)

prods_comp.rename(columns={'Código GTIN': 'EAN'}, inplace=True)

# Organização das informações da coluna EAN
prods_comp['EAN'] = np.where((prods_comp['EAN'] == 'SEM GTIN      ') | (prods_comp['EAN'] == 'nan'), np.nan, prods_comp['EAN'])
prods_comp['EAN'] = prods_comp['EAN'].astype(str).str.strip()

efd['codigo_do_item'] = efd['codigo_do_item'].astype(str)
prods_comp['Número Item'] = prods_comp['Número Item'].astype(str)
efd['chave_nfe'] = efd['chave_nfe'].astype(str)
prods_comp['Chave Acesso NFe'] = prods_comp['Chave Acesso NFe'].astype(str)

df_merged = efd.merge(prods_comp, left_on=['chave_nfe', 'codigo_do_item'], 
                         right_on=['Chave Acesso NFe', 'Número Item'], how='left')

df_merged = df_merged[(df_merged['Chave Acesso NFe'].notnull()) & (df_merged['Número Item'].notnull())] 
if df_merged.shape[0] == 0:
    print('Erro encontrado. Nenhuma combinação chave-item localizada, favor verificar')
    sys.exit()

df_merged = df_merged.drop_duplicates()
df_merged_novo = df_merged.loc[(df_merged['CFOP'] == '1405') | (df_merged['CFOP'] == '1403')]

df_merged_novo['Produto'] = df_merged_novo['Código Produto ou Serviço']

df_produtos['codigo_produto'] = df_produtos['codigo_produto'].astype(str)
df_merged_novo['Produto'] = df_merged_novo['Produto'].astype(str)
df = df_merged_novo.merge(df_produtos, left_on=['Produto'],right_on=['codigo_produto'],
                    how='left')

df = df.drop_duplicates()

prods_mva = df[df['anvisa'].isnull()]

prods_mva['Valor Produto ou Serviço'] = prods_mva['Valor Produto ou Serviço'].str.replace('.', '').str.replace(',', '.').astype(float)

prods_mva['mva_antes'] = prods_mva['mva_antes'].str.replace(',','.').replace('None', np.nan).astype(float)
prods_mva['mva_depois'] = prods_mva['mva_depois'].str.replace(',','.').replace('None', np.nan).astype(float)
prods_mva['Valor Base Cálculo ICMS ST Retido Operação Anterior'] = prods_mva['Valor Base Cálculo ICMS ST Retido Operação Anterior'].str.replace('.','').str.replace(',', '.').fillna(0).astype(float)
prods_mva['icms'] = prods_mva['icms'].astype(str).replace('None', np.nan).fillna(0).astype(float)

prods_mva['Data Emissão'] = pd.to_datetime(prods_mva['Data Emissão'], format='%Y-%m-%d')

prods_mva['MVA_A_USAR'] = np.where((prods_mva['Data Emissão'] < prods_mva['data_valida']) | (prods_mva['mva_depois'].isnull()),
                               prods_mva['mva_antes'], prods_mva['mva_depois'])

prods_mva['vBCST'] = (prods_mva['Valor Produto ou Serviço'] * (1+prods_mva['MVA_A_USAR']/100))

prods_mva['vBCST_Complementar'] = prods_mva['vBCST'] - prods_mva['Valor Base Cálculo ICMS ST Retido Operação Anterior']

prods_mva['ICMS_ST'] = prods_mva['vBCST_Complementar'] * (prods_mva['icms']/100)

prods_mva['vPMC'] = prods_mva['MVA_A_USAR']
prods_mva['anvisa'] = [np.nan]*prods_mva.shape[0]
prods_mva['DESCONTO_PORCENTAGEM'] = [np.nan]*prods_mva.shape[0]
mva_complementar = prods_mva[['Chave Acesso NFe', 'Número Item', 'EAN_y', 'Produto',
                       'Quantidade Comercial', 'Valor Produto ou Serviço', 'Valor Base Cálculo ICMS ST Retido Operação Anterior',
                       'vPMC', 'anvisa', 'vBCST', 'vBCST_Complementar','icms', 'ICMS_ST']]

mva_complementar.rename(columns={'Número Item': 'Item NFE Origem',
                                'EAN_y':'EAN',
                                'vBCST_Complementar': 'ICMS ST Retido-Base de Cálculo',
                                'icms': 'ALIQUOTA',
                                'ICMS_ST': 'ICMS ST Retido- Valor'}, inplace=True)

mva_complementar['Valor Produto ou Serviço'] = mva_complementar['Valor Produto ou Serviço'].astype(str)
mva_complementar['ICMS ST Retido-Base de Cálculo'] = mva_complementar['ICMS ST Retido-Base de Cálculo'].round(2).astype(str)
mva_complementar['ICMS ST Retido- Valor'] = mva_complementar['ICMS ST Retido- Valor'].round(2).astype(str)
mva_complementar['ALIQUOTA'] = mva_complementar['ALIQUOTA'].astype(float)

salvar_dataframe_no_s3(mva_complementar, bucket_name, s3_key=f'Cat42/{nome_empresa.title()}/Complementar/complementar.xlsx',
                       file_type='xlsx')