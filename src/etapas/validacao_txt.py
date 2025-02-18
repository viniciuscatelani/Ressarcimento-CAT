# Importação das bibliotecas necessárias

import pandas as pd
import numpy as np
import glob
import os
import re
from datetime import datetime
import locale
import sys
from dotenv import load_dotenv
import boto3
import psycopg2
from io import StringIO
import warnings
warnings.filterwarnings('ignore')

from src.utils.ler_arquivos import ler_arquivo_para_dataframe

# Carregando variáveis de ambiente
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
print(f"Carregando .env de: {dotenv_path}")
load_dotenv(dotenv_path, override=True)

# Definição da empresa a ser gerado os dados
nome_empresa = input("Digite o nome da empresa: ")

if nome_empresa.lower() == 'tateti':
    cnpj = "65369985000504"

connection = psycopg2.connect(
        user=  'cat',
        password=  '5pM2h0MBQu9JHkxHud2A',
        host=  '177.11.49.194',
        port="3361",
        database=  '4btaxtech'
    )

# Variáveis para acesso ao s3
bucket_name = '4btaxtech'

s3 = boto3.client('s3', 
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), 
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                  region_name=os.getenv('AWS_DEFAULT_REGION')
                  )

ficha_3 = ler_arquivo_para_dataframe(bucket_name=bucket_name, s3_key=f'Cat42/{nome_empresa.title()}/Ficha 3/ficha_3_{nome_empresa.title()}_{cnpj}.xlsx',
                                  file_type='xlsx')

def recalcular_digito_verificador(nfe_completa):
    # Verifica se a NF-e com o dígito verificador tem o tamanho correto
    if pd.isnull(nfe_completa) or len(nfe_completa) != 44:
        raise ValueError(f"Valor inválido na coluna 'CHV_DOC': {nfe_completa}")

    # Remove o último caractere (dígito verificador)
    nfe_sem_dv = nfe_completa[:-1]

    # Restante da lógica de cálculo do dígito verificador
    soma = 0
    peso = 2
    nfe_invertida = nfe_sem_dv[::-1]

    for digito in nfe_invertida:
        soma += int(digito) * peso
        peso += 1
        if peso > 9:
            peso = 2

    resto = soma % 11
    dv = 0 if resto == 0 or resto == 1 else 11 - resto

    return dv

# Iterate through each file in the folder
query = 'SELECT * FROM "0000" '
df_0000 = pd.read_sql_query(query, connection)
df_0000['data'] = df_0000['data'].astype(str).str.slice(0, 10)
df_0000['data'] = [datetime.strptime(x, '%d/%m/%Y').strftime('%m%Y') for x in df_0000['data']]
df_0000['COD_VER'] = ['01'] * df_0000.shape[0]
df_0000['COD_FIN'] = ['00'] * df_0000.shape[0]
df_0000 = df_0000[['empresa','registro', 'data', 'razao_social', 'cnpj', 'ie',
        'codigo_municipio', 'COD_VER', 'COD_FIN']]

cnpj_loja = df_0000[df_0000['empresa'] == cnpj]['cnpj'].values[0]

prefix = f"Cat42/{nome_empresa.title()}/Txts/Teste_CNPJ_{cnpj}/"
files = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

# Filtrar arquivos que começam com '1100'
filtered_files = [file['Key'] for file in files.get('Contents', []) if file['Key'].startswith(f'{prefix}1100')]

# Inicializar uma lista para armazenar os dataframes
dfs = []

# Ler e processar cada arquivo
for file_key in filtered_files:
    # Baixar o conteúdo do arquivo S3
    file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = file_obj['Body'].read().decode('latin-1')  # Ajuste de encoding caso necessário

    # Converter o conteúdo do arquivo para um DataFrame
    # Aqui assumimos que os arquivos têm dados separados por '|' (se for outro delimitador, ajuste conforme necessário)
    df = pd.read_csv(StringIO(file_content), sep='|',header=None,
                     names=['Cod_reg', 'Chv_doc', 'data', 'num_item', 'ind_oper', 
                             'cod_prod', 'cfop', 'qtd', 'icms_tot', 'vlr_confr', 'cod_legal'])

    # Adicionar o DataFrame à lista
    dfs.append(df)

# Concatenar todos os DataFrames em um único DataFrame
final_df = pd.concat(dfs, ignore_index=True)

df_1100_icms_tot = final_df[['Chv_doc', 'num_item', 'icms_tot']]
ficha_3_icms_tot = ficha_3[['CHV_DOC', 'NUM_ITEM', 'ICMS_TOT_PCAT']]

ficha_3_icms_tot['NUM_ITEM'] = ficha_3_icms_tot['NUM_ITEM'].astype(str)
df_1100_icms_tot['num_item'] = df_1100_icms_tot['num_item'].astype(str)

df_1100_icms_tot['Chv_item'] = df_1100_icms_tot['Chv_doc'] + '-' + df_1100_icms_tot['num_item']
ficha_3_icms_tot['CHV_ITEM'] = ficha_3_icms_tot['CHV_DOC'] + '-' + ficha_3_icms_tot['NUM_ITEM']

df_1100_icms_tot['icms_tot'] = df_1100_icms_tot['icms_tot'].str.replace(',', '.').astype(float)

merged = df_1100_icms_tot.merge(ficha_3_icms_tot[['CHV_ITEM', 'ICMS_TOT_PCAT']],
                               left_on=['Chv_item'],
                               right_on=['CHV_ITEM'])

merged = merged.drop_duplicates()

merged['Diffs'] = np.abs(merged['icms_tot'] - merged['ICMS_TOT_PCAT'])

if merged[merged['Diffs'] >= 0.1].shape[0] > 0:
    print('Há erro na geração do TXT em relação ao ICMS_TOT_PCAT, favor verificar')
    sys.exit()

final_df['cfop'] = final_df['cfop'].astype(str)
# Checagem de existência de ICMS_TOT

if final_df[(final_df['cfop'].isin(['1403', '1409', '1411', '5411'])) & (final_df['icms_tot'].isnull())].shape[0] > 0:
    print('Existem entradas, devoluções de entrada e devoluções de saída com ICMS_TOT nulo. Favor checar')
    sys.exit()
    
if final_df[(final_df['cfop'].isin(['5409', '5405', '5403', '5401'])) & (final_df['icms_tot'].notnull())].shape[0] > 0:
    print('Existem saídas com campo ICMS_TOT preenchido. Favor checar')
    sys.exit()

# Checagem de Valor de confronto

if final_df[(final_df['cfop'].isin(['1403', '1409', '5411'])) & (final_df['vlr_confr'].notnull())].shape[0] > 0:
    print('Existem entradas e devoluções de entrada com Valor de Confronto não nulo. Favor, checar.')
    sys.exit()
    
if final_df[(final_df['cfop'].isin(['5405', '1411', '5409', '5403', '5401'])) & (final_df['cod_legal'] == '1') & (final_df['vlr_confr'].isnull())].shape[0] > 0:
    print('Existem sáidas e devoluções de saída com Valor de Confronto nulo. Favor, checar.')
    sys.exit()
    
if final_df[(final_df['cfop'].isin(['5405', '1411', '5409', '5403', '5401'])) & (final_df['cod_legal'] == '0') & (final_df['vlr_confr'].notnull())].shape[0] > 0:
    print('Existem saídas e devoluções de saída com Valor de Confronto não nulo quando Código Legal igual a 0. Favor, checar')
    sys.exit()

# Cheagem de código legal

if final_df[(final_df['cfop'].isin(['1403', '1409', '5411'])) & (final_df['cod_legal'].notnull())].shape[0] > 0:
    print('Existem entradas e devoluções de entrada com Código Legal não nulo. Favor, checar.')
    sys.exit()
    
if final_df[(final_df['cfop'].isin(['5405', '1411', '5409', '5403', '5401'])) & (final_df['cod_legal'].isnull())].shape[0] > 0:
    print('Existem sáidas e devoluções de saída com Código Legal nulo. Favor, checar.')
    sys.exit()

print('Checagem completa! Nenhum problema encontrado.')