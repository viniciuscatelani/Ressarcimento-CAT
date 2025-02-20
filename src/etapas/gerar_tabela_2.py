# Importação das bibliotecas necessárias
import pandas as pd
import numpy as np
import psycopg2
import math
from datetime import datetime
import sys
import openpyxl
import os
import boto3
from dotenv import load_dotenv

from src.utils.ler_arquivos import ler_arquivo_para_dataframe, salvar_dataframe_no_s3

# Carregando variáveis de ambiente
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../', '.env'))
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
                  aws_access_key_id='AKIA4RCAOBRSFONXEUTE', 
                  aws_secret_access_key='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',
                  region_name='us-east-1'
                  )

# Leitura da tabela 1 gerada em etapa anterior
tabela_1 = ler_arquivo_para_dataframe(bucket_name, f'Cat42/{nome_empresa.title()}/Tabela 1/tabela_1_{nome_empresa}.csv', file_type='csv')
tabela_1 = tabela_1.dropna(subset='Número Item')

tabela_1['Código Produto ou Serviço'] = tabela_1['Código Produto ou Serviço'].astype(str)
tabela_1['Data Emissão'] = pd.to_datetime(tabela_1['Data Emissão'].str.slice(0,10),format='mixed')

uso_complementar = input('Haverá uso de nota complementar ? ')

# Leitura do arquivo da complementar
if uso_complementar.lower() == 'sim':
 
    complementar = ler_arquivo_para_dataframe(bucket_name, f"Cat42/{nome_empresa.title()}/Complementar/complementar.xlsx", file_type='xlsx')
    complementar['Produto'] = complementar['Produto'].astype(str)
    complementar['Item NFE Origem'] = complementar['Item NFE Origem'].astype(int)

if uso_complementar.lower() == 'não':
    colunas = ['Chave Acesso NFe', 'Item NFE Origem', 'EAN', 'Produto',
                       'Quantidade Comercial', 'Valor Produto ou Serviço', 'Valor Base Cálculo ICMS ST Retido Operação Anterior',
                       'vPMC', 'anvisa', 'vBCST', 'ICMS ST Retido-Base de Cálculo','ALIQUOTA', 'ICMS ST Retido- Valor']
    
    complementar = pd.DataFrame(columns=colunas)
    for coluna in colunas:
        complementar[coluna] = np.zeros(shape=tabela_1.shape[0])

# Leitura da tabela da efd_mod55 para um data frame

query = f"SELECT * FROM modelo55 WHERE cnpj = '{cnpj}';"

efd = pd.read_sql_query(query, connection)
if efd.shape[0] == 0:
    print(f'Tabela modelo 55 da loja {nome_empresa.title()}:{cnpj} não consta no banco. Favor verificar')
    sys.exit()

# Alteração do tipo de dado

efd['codigo_do_item'] = efd['codigo_do_item'].astype(float)

# Leitura da tabela da efd_mod59 para um data frame

query = f'SELECT * FROM modelo59 WHERE cnpj = {cnpj}'

efd_mod59 = pd.read_sql_query(query, connection)
if (efd_mod59.shape[0] == 0) and (nome_empresa != 'ladakh'):
    print(f'Tabela modelo 59 da loja {nome_empresa.title()}:{cnpj} não consta no banco. Favor verificar')
    sys.exit()


# Formatação da coluna de data
efd_mod59['data_efds'] = pd.to_datetime(efd_mod59['data_efds'], format='%m/%Y')

# Alteração do tipo de dado

# efd_mod59['codigo_produto'] = efd_mod59['codigo_produto'].astype(str)

# Retirada dos valores negativos de base de calculo da complementar

complementar['ICMS ST Retido-Base de Cálculo'] = complementar['ICMS ST Retido-Base de Cálculo'].astype(float)
complementar['ICMS ST Retido- Valor'] = complementar['ICMS ST Retido- Valor'].astype(float)
complementar_final = complementar[complementar['ICMS ST Retido-Base de Cálculo'] >= 0]

# Formatação da coluna de data da tabela 1

tabela_1['Data Emissão'] = pd.to_datetime(tabela_1['Data Emissão'],format='mixed')

# Formatação da coluna Valor Produto ou Serviço para o tipo correto

tabela_1['Valor Produto ou Serviço'] = tabela_1['Valor Produto ou Serviço'].astype(float)

# Preenchimento da coluna bc_complementar_total_complementar

tabela_1['Número Item'] = tabela_1['Número Item'].astype(float)
tabela_1['Chave Acesso NFe'] = tabela_1['Chave Acesso NFe'].astype(str)

complementar_final['Item NFE Origem'] = complementar_final['Item NFE Origem'].astype(float)
complementar_final['Chave Acesso NFe'] = complementar_final['Chave Acesso NFe'].astype(str)
bc_df = complementar_final[['Chave Acesso NFe', 'Item NFE Origem', 'vBCST', 'ICMS ST Retido-Base de Cálculo', 'Produto', 'vPMC']]
merged_df = pd.merge(tabela_1, bc_df, left_on=['Chave Acesso NFe', 'Número Item'],
                    right_on=['Chave Acesso NFe', 'Item NFE Origem'], how='left')

# merged_df = merged_df.drop('Unnamed: 0', axis=1)
merged_df = merged_df.drop_duplicates()

# Preenchimento da coluna bc_complementar_total_item_original

merged_df['Valor Base de Cálculo ICMS ST Retido'] = merged_df['Valor Base de Cálculo ICMS ST Retido'].astype(float).fillna(0)
merged_df['Valor ICMS Operação'] = merged_df['Valor ICMS Operação'].astype(float).fillna(0)

merged_df['bc_complementar_total_item_original'] = merged_df['ICMS ST Retido-Base de Cálculo']

# Formatação da coluna de data
efd['data_saida'] = efd['data_saida'].replace('', np.nan)
efd['data_saida'] = pd.to_datetime(efd['data_saida'], format='%d/%m/%Y')

# Mudança do tipo de dado para string

efd['codigo_do_item'] = efd['codigo_do_item'].astype(float)
merged_df['Número Item'] = merged_df['Número Item'].astype(float)

# Mudança do tipo de dado para string

efd['chave_nfe'] = efd['chave_nfe'].astype(str)
merged_df['Chave Acesso NFe'] = merged_df['Chave Acesso NFe'].astype(str)

#Preenchimento das colunas referentes a informações da efd
merged = pd.merge(merged_df, efd[['chave_nfe','data_saida', 'codigo_do_item',
                                  'cfop', 'codigo_produto', 'quantidade',
                                 'descricao_produto', 'ean', 'ncm', 'cest', 'indice_operacao']],
                 left_on=['Chave Acesso NFe', 'Número Item'],
                 right_on=['chave_nfe', 'codigo_do_item'],
                 how='left')

merged = merged.drop_duplicates()

# Definição de coluna com para chave-item para identificação de erros

merged['CHAVE_ITEM'] = merged['Chave Acesso NFe'].astype(str) + '-' + merged['Número Item'].astype(str)
merged = merged.drop_duplicates(subset='CHAVE_ITEM', keep='first')

# Identificação de algum possível erro em relação à unicidade de pares chave-item

merged['CHAVE_ITEM'] = merged['Chave Acesso NFe'].astype(str) + '-' + merged['Número Item'].astype(str)

duplicate_mask = merged['CHAVE_ITEM'].duplicated(keep=False)
duplicate_df = merged[duplicate_mask]
if duplicate_df.shape[0] > 0:
    print('Erro encontrado: combinação Chave-Item duplicada. Por favor verificar')
    sys.exit()

# Leitura para um dataframe da tabela de produtos
# do banco de dados

query = f"SELECT * FROM produtos where empresa = '{cnpj_produtos}'"
produtos = pd.read_sql_query(query, connection)

# Checagem de erro em relação à duplicidade de aliquota na tabela de produtos

mask = produtos.groupby('codigo_produto')['icms'].transform('nunique') > 1

# Filtrar as linhas que atendem à condição
result = produtos[mask]
result = result.sort_values(by='codigo_produto')
if result.shape[0] > 0:
    print('DUPLICAÇÃO DE ALIQUOTA NA TABELA DE PRODUTOS, FAVOR CHECAR')
    sys.exit()

# Ajustes de dados e definição de informações para aplicação da regras de 
# filtragem dos dados presentes na efd

efd['codigo_produto'] = efd['codigo_produto'].astype(str)
efd['CHAVE_ITEM'] = efd['chave_nfe'] + '-' + efd['codigo_do_item'].astype(str)
cods = efd['codigo_produto'].unique()
merged['Produto'] = merged['Produto'].astype(str)
merged['Chave-Item'] = merged['Chave Acesso NFe'] + '-' + merged['Número Item'].astype(str)

chave_mod_59 = efd_mod59['chv_cfe'].unique()
chave_mod_55 = efd['chave_nfe'].unique()

# Aplicação das regras

# Regra 1
df_1 = merged[merged['Chave-Item'].isin(efd['CHAVE_ITEM'])]
df_1['Tipo'] = 'entrada'

# Regra 2
efd_filtrado  = efd[(efd['codigo_do_item'].isna()) & (efd['indice_operacao'] == '1')]
df_2 = merged[merged['Chave Acesso NFe'].isin(efd_filtrado['chave_nfe'])]
df_2['Tipo'] = 'saida'
# Regra 3
df_3 =  merged[merged['Chave Acesso NFe'].isin(chave_mod_59)]
df_3['Tipo'] = 'saida'

merged_novo = pd.concat([df_1,  df_2, df_3])

merged_novo['IND_OPER'] = np.where((merged_novo['Tipo'] == 'saida'),
                            1, 0)

merged_novo = merged_novo.drop_duplicates()
merged_novo['Produto'] = np.where((merged_novo['Tipo'] == 'entrada') & (~merged_novo['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)) & (merged_novo['CFOP'].astype(float) != 5409),
                               merged_novo['codigo_produto'], merged_novo['Código Produto ou Serviço'])
merged_novo['Produto'] = merged_novo['Produto'].astype(str)

df_merged = pd.merge(merged_novo, produtos[['codigo_produto', 'icms', 'cest']],
                    left_on=['Produto'], right_on=['codigo_produto'],
                    how='left')

# Checagem de duplicação de chave-item

df_merged['CHAVE_ITEM'] = df_merged['Chave Acesso NFe'].astype(str) + '-' + df_merged['Número Item'].astype(str)

duplicate_mask = df_merged['CHAVE_ITEM'].duplicated(keep=False)
duplicate_df = df_merged[duplicate_mask]
if duplicate_df.shape[0] > 0:
    print('Erro encontrado: combinação Chave-Item duplicada. Por favor verificar')
    sys.exit()

df_merged = df_merged.drop_duplicates()

df = df_merged.copy()

# Retirada de linhas duplicadas

df = df.drop_duplicates()

# Preenchimento da coluna ICMS_TOT
df['Valor Base Cálculo ICMS ST Retido Operação Anterior'] = df['Valor Base Cálculo ICMS ST Retido Operação Anterior'].fillna(0)
df['Valor ICMS Substituição Tributária'] = df['Valor ICMS Substituição Tributária'].astype(float).fillna(0)
df['Valor ICMS Operação'] = df['Valor ICMS Operação'].astype(float).fillna(0)
df['Valor ICMS ST Retido'] = df['Valor ICMS ST Retido'].astype(float).fillna(0)
df['Valor ICMS Substituto'] = df['Valor ICMS Substituto'].astype(float).fillna(0)
df['icms'] = df['icms'].astype(str).replace('None', np.nan)

df['icms'] = df['icms'].str.replace('-', '0').fillna('0').str.replace('nan', '0')
df['vBCST'] = df['vBCST'].astype(float).fillna(0)

# Geração da coluna CHV_DOC
tabela_2 = pd.DataFrame()

tabela_2['CHV_DOC'] = df['Chave Acesso NFe']

# Geração da coluna DATA

tabela_2['DATA'] = np.where((df['Tipo'] == 'entrada') & (~df['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)),
                            df['data_saida'], df['Data Emissão'])

# Geração da coluna NUM_ITEM

tabela_2['NUM_ITEM'] = df['Número Item']

# Preenchimento da coluna CFOP

tabela_2['CFOP'] = np.where((df['Tipo'] == 'entrada') & (~df['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)),
                               df['cfop'], df['CFOP'])

tabela_2['CFOP'] = tabela_2['CFOP'].astype(str).str.replace(r'\.0$', '', regex=True)

# Preenchimento da coluna COD_ITEM

tabela_2['COD_ITEM'] = np.where((df['Tipo'] == 'entrada') & (~df['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)) & (~df['CFOP'].astype(float).isin([5202, 6202, 5409, 5411])),
                               df['Produto'], 
                               df['Código Produto ou Serviço'])

tabela_2['COD_ITEM'] = np.where((df['Tipo'] == 'entrada') & (~df['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)) & (df['CFOP'].astype(float).isin([5405, 5152])) & (df['cfop'].astype(float) == 1409),
                                df['Código Produto ou Serviço'],
                                tabela_2['COD_ITEM'])

tabela_2['COD_ITEM'] = np.where((df['Tipo'] == 'entrada') & (~df['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)) & (df['CFOP'].astype(float).isin([5411, 5202])),
                                df['Produto'],
                                tabela_2['COD_ITEM'])


# Preenchimento das colunas QTD_NOTA, QTD_CAT e QTD_EFD

tabela_2['QTD_NOTA'] = df['Quantidade Comercial'].astype(float)
tabela_2['QTD_CAT'] = np.where((df['Tipo'] == 'entrada') & (~df['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)) & (~df['CFOP'].astype(float).isin([5202, 6202, 5409, 5411])),
                                df['quantidade'], df['Quantidade Comercial'])
tabela_2['QTD_CAT'] = np.where((df['Tipo'] == 'entrada') & (~df['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)) & (df['CFOP'].astype(float).isin([5405, 5152])) & (df['cfop'].astype(float) == 1409),
                                df['Quantidade Comercial'],
                                tabela_2['QTD_CAT'])

tabela_2['QTD_CAT'] = np.where((df['Tipo'] == 'entrada') & (~df['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)) & (df['CFOP'].astype(float).isin([5411, 5202])),
                                df['quantidade'],
                                tabela_2['QTD_CAT'])

tabela_2['QTD_NOTA'] = tabela_2['QTD_NOTA'].astype(str).str.replace(r'\.0$', '', regex=True)
tabela_2['QTD_CAT'] = tabela_2['QTD_CAT'].astype(str).str.replace(r'\.0$', '', regex=True)
tabela_2['QTD_EFD'] = np.where((df['Tipo'] == 'entrada') & (~df['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)),
                                df['quantidade'], np.nan)


# Preenchimento da coluna DESCRICAO

tabela_2['DESCRICAO'] = np.where((df['Tipo'] == 'entrada') & (~df['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)) & (~df['CFOP'].astype(float).isin([5202, 6202, 5409, 5411])),
                               df['descricao_produto'], df['Descrição Produto'])

tabela_2['DESCRICAO'] = np.where((df['Tipo'] == 'entrada') & (~df['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)) & (df['CFOP'].astype(float).isin([5405, 5152])) & (df['cfop'].astype(float) == 1409),
                                df['Descrição Produto'],
                                tabela_2['DESCRICAO'])

tabela_2['DESCRICAO'] = np.where((df['Tipo'] == 'entrada') & (~df['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)) & (df['CFOP'].astype(float).isin([5411, 5202])),
                                df['descricao_produto'],
                                tabela_2['DESCRICAO'])

# Preenchimento da coluna CODIGO_BARRA


tabela_2['CODIGO_BARRA'] = np.where((df['Tipo'] == 'entrada') & (~df['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)),
                               df['ean'], df['Código GTIN'])

# Preenchimento da coluna UNIDADE

tabela_2['UNIDADE'] = df['Unidade Comercial']

# Preenchimento da coluna N C M

df['N C M'] = np.where((df['Tipo'] == 'entrada') & (~df['Chave Acesso NFe'].str.slice(6,20).isin(cnpjs)),
                               df['ncm'], df['Código NCM'])

tabela_2['N C M'] = np.where(df['N C M'].notnull(),
                               df['N C M'], df['Código NCM'])

# Preenchimento da coluna ALIQUOTA

tabela_2['ALIQUOTA'] = df['icms']
tabela_2['ALIQUOTA'] = tabela_2['ALIQUOTA'].astype(str).str.replace(r'\.0$','', regex=True)

# Preenchimento da coluna CEST

tabela_2['CEST'] = df['cest_y']                        

# Preenchimento da coluna Entr_PCAT

cfops = [1102, 1202, 1403, 1409, 1411, 
         1949, 2101, 2102, 2202, 2209, 
         2401, 2403, 2405, 2409, 
         2411, 2414, 2949, 5101, 5102, 
         5117, 5118, 
         5120, 5152, 5201, 5202, 
         5210, 5401, 5403, 5405, 
         5409, 5410, 5411, 5413, 5551,
         5556, 5910, 5911, 5922, 5923, 
         5927, 
         5929, 5949, 6101, 6102, 6103, 
         6108, 6117, 6119, 6152, 6202, 
         6401, 6403, 6404, 6409, 
         6411, 6414, 6556, 6910, 
         6911, 6922, 6923]

tabela_2['Entr_PCAT'] = np.where((tabela_2['CFOP'].astype(int).isin(cfops)) & (tabela_2['CEST'].notnull()),
                                 1,
                                 0)


# Preenchimento das colunas CNPJ EMITENTE e CNPJ DESTINATARIO

tabela_2['CNPJ EMITENTE'] = df['Número CNPJ Emitente']
tabela_2['CNPJ DESTINATARIO'] = df['Número CNPJ Destinatário']

# Preenchimento da coluna IND_OPER

tabela_2['IND_OPER'] = df['IND_OPER']

# Preenchimento da coluna VALOR
tabela_2['VALOR'] = df['Valor Produto ou Serviço']

# Definição de mais colunas

tabela_2['Valor Base Cálculo ICMS ST Retido Operação Anterior'] = df['Valor Base Cálculo ICMS ST Retido Operação Anterior']
tabela_2['Valor Base Cálculo ICMS Substituição Tributária'] = df['Valor Base Cálculo ICMS Substituição Tributária']
tabela_2['Valor Complementar'] = df['bc_complementar_total_item_original']
tabela_2['vBCST'] = df['vBCST']
tabela_2['Valor ICMS Operação'] = df['Valor ICMS Operação']
tabela_2['Valor ICMS Substituição Tributária'] = df['Valor ICMS Substituição Tributária']
tabela_2['Valor ICMS ST Retido'] = df['Valor ICMS ST Retido']
tabela_2['Valor ICMS Substituto'] = df['Valor ICMS Substituto']
tabela_2['QTD_CAT'] = tabela_2['QTD_CAT'].replace('nan', np.nan)
tabela_2['CST'] = df['CST ICMS']
tabela_2['FONTE'] = df['Tipo']

# Preenchimento da coluna ICMS_TOT 
tabela_2['Valor ICMS Operação'] = np.where((tabela_2['FONTE'] == 'entrada') & (~tabela_2['CHV_DOC'].str.slice(6,20).isin(cnpjs)),
                                     np.where(tabela_2['Valor Base Cálculo ICMS ST Retido Operação Anterior'] <= tabela_2['vBCST'],
                                              tabela_2['Valor ICMS Operação'],
                                              tabela_2['Valor ICMS Substituto']),
                                              tabela_2['Valor ICMS Operação'])

tabela_2['Valor ICMS Operação'] = tabela_2['Valor ICMS Operação'].fillna(0)
tabela_2['Valor ICMS Substituição Tributária'] = tabela_2['Valor ICMS Substituição Tributária'].fillna(0)
tabela_2['Valor ICMS ST Retido'] = tabela_2['Valor ICMS ST Retido'].fillna(0)
tabela_2['Valor ICMS Substituto'] = tabela_2['Valor ICMS Substituto'].fillna(0)
tabela_2['Valor Base Cálculo ICMS ST Retido Operação Anterior'] = tabela_2['Valor Base Cálculo ICMS ST Retido Operação Anterior'].fillna(0)
tabela_2['Valor Complementar'] = tabela_2['Valor Complementar'].fillna(0)
tabela_2['vBCST'] = tabela_2['vBCST'].fillna(0)
tabela_2['ALIQUOTA'] = tabela_2['ALIQUOTA'].fillna(0)

condicao_entrada = (tabela_2['FONTE'] == 'entrada') & (~tabela_2['CHV_DOC'].str.slice(6, 20).isin(cnpjs))

condicao_base_icms = tabela_2['Valor Base Cálculo ICMS ST Retido Operação Anterior'] <= tabela_2['vBCST']
condicao_zero_valores = (
    (tabela_2['Valor Complementar'] == 0) &
    (tabela_2['Valor Base Cálculo ICMS ST Retido Operação Anterior'] == 0) &
    (tabela_2['Valor ICMS Operação'] == 0) &
    (tabela_2['Valor ICMS Substituição Tributária'] == 0)
)

# Ajustando a lógica para garantir que a soma dos valores ICMS retido e substituto aconteça quando os outros valores forem 0
tabela_2['ICMS_TOT'] = np.where(
    condicao_entrada,
    np.where(
        condicao_base_icms,
        np.where(
            condicao_zero_valores,
            tabela_2['Valor ICMS ST Retido'] + tabela_2['Valor ICMS Substituto'],  # Garantir que essa soma seja feita
            np.maximum(
                tabela_2['Valor ICMS Operação'] + tabela_2['Valor ICMS Substituição Tributária'],
                (tabela_2['Valor Base Cálculo ICMS ST Retido Operação Anterior'] + tabela_2['Valor Complementar']) * tabela_2['ALIQUOTA'].astype(float) / 100
            )
        ),
        np.where(
            tabela_2['vBCST'] != 0,
            np.maximum(
                tabela_2['Valor ICMS Operação'] + tabela_2['Valor ICMS Substituição Tributária'],
                (tabela_2['vBCST'] + tabela_2['Valor Complementar']) * tabela_2['ALIQUOTA'].astype(float) / 100
            ),
            tabela_2['Valor ICMS ST Retido'] + tabela_2['Valor ICMS Substituto']  # Garantir que a soma de ICMS ST Retido e Substituto aconteça aqui também
        )
    ),
    np.nan  # Valor a ser atribuído quando a condição principal não for atendida
)

tabela_2['ICMS_TOT'] = np.where(tabela_2['CFOP'].isin([1102, 2102]), np.nan, tabela_2['ICMS_TOT'])

tabela_2['Valor ICMS Operação'] = np.where(tabela_2['Valor ICMS Operação'] == 0,
                                           tabela_2['Valor ICMS Substituto'],
                                           tabela_2['Valor ICMS Operação'])

tabela_2 = tabela_2[tabela_2['Entr_PCAT'] == 1]

query = 'select * from cfop'
df_2 = pd.read_sql_query(query, connection)

tabela_2['CFOP'] = tabela_2['CFOP'].astype(int)
df_2['cfop'] = df_2['cfop'].astype(int)

tabela_2 = tabela_2.merge(df_2, left_on='CFOP', right_on='cfop')
tabela_2['DATA'] = pd.to_datetime(tabela_2['DATA'], format='%Y-%m-%d')
# df_new = df_new.sort_values(by=['COD_ITEM', 'DATA', 'IND_OPER'])
tabela_2['ALIQUOTA'] = tabela_2['ALIQUOTA'].astype(str).replace('nan', np.nan)

# Preenchimento da coluna VL_CONFR
tabela_2['CFOP'] = tabela_2['CFOP'].astype(int)
tabela_2['IND_OPER'] = tabela_2['IND_OPER'].astype(int)
tabela_2['sinal'] = tabela_2['sinal'].astype(int)

mask = (tabela_2['IND_OPER'] == 1) & (tabela_2['sinal'] == 1) & \
       (~tabela_2['CFOP'].isin([5409, 5927])) 
# Calculate the values using NumPy operations
values = np.where(mask, tabela_2['VALOR'] * (tabela_2['ALIQUOTA'].fillna(0).astype(float) / 100), np.nan)

# Assign the calculated values to the 'vl_confr' column
        
tabela_2['VL_CONFR'] = values

tabela_2['SUB_TIPO'] = tabela_2['sinal']
tabela_2['VL_CONFR_0'] = tabela_2['VL_CONFR']

tabela_2['ALIQUOTA'] = tabela_2['ALIQUOTA'].astype(str).replace('nan', np.nan)

# Preenchimento da coluna COD_LEGAL
tabela_2['CFOP'] = tabela_2['CFOP'].astype(int)
tabela_2['IND_OPER'] = tabela_2['IND_OPER'].astype(int)
tabela_2['SUB_TIPO'] = tabela_2['SUB_TIPO'].astype(int)

conditions = [
    tabela_2['CFOP'] == 5927,
    tabela_2['CFOP'].isin([6102, 6404, 6108, 6117, 6152, 6409]),
    tabela_2['CFOP'] == 5409,
    (tabela_2['IND_OPER'] == 0) & (tabela_2['SUB_TIPO'] == 1),
    (tabela_2['IND_OPER'] == 1) & (tabela_2['SUB_TIPO'] == -1),
    tabela_2['ALIQUOTA'].isnull()
]

choices = [2, 4, 0, np.nan, np.nan, 0]

# If none of the conditions are met, the default choice is 1
default_choice = 1

# Use numpy.select to set values for 'COD_LEGAL'
tabela_2['COD_LEGAL'] = np.select(conditions, choices, default=default_choice)

# Substituição dos valores nulos de aliquota por 0

tabela_2['ALIQUOTA'] = tabela_2['ALIQUOTA'].astype(str).replace('nan', np.nan).fillna(0).astype(float)

# Criação de coluna para checagem de erro em fator de conversão

tabela_2 = tabela_2[(tabela_2['QTD_NOTA'] != 'nan') & (tabela_2['QTD_NOTA'].astype(float) > 0)]
tabela_2['QTD_NOTA'] = tabela_2['QTD_NOTA'].astype(float)
tabela_2['QTD_CAT'] = tabela_2['QTD_CAT'].astype(float)
tabela_2['CHECAGEM'] = (tabela_2['QTD_CAT']/tabela_2['QTD_NOTA']).round(3)

# Geração de tabela para checagem de erro em fator de conversão

pivot_table = tabela_2[(tabela_2['IND_OPER'] == 0) & (~tabela_2['CHV_DOC'].str.slice(6,20).isin(cnpjs)) & (~tabela_2['CFOP'].astype(float).isin([1202, 2202, 1411,2411]))].pivot_table(index=['COD_ITEM', 'UNIDADE'], values='CHECAGEM', aggfunc='nunique').reset_index()

# Checagem de erro em fator de conversão

cod_items_with_multiple_values = pivot_table[pivot_table['CHECAGEM'] > 1]['COD_ITEM']
if cod_items_with_multiple_values.shape[0] > 0:
    print('Erro encontrado: Fator de conversão errado, favor verificar')
    cods = pivot_table[pivot_table['CHECAGEM'] > 1]['COD_ITEM'].values
    salvar_dataframe_no_s3(tabela_2[(tabela_2['COD_ITEM'].isin(cods)) & (tabela_2['IND_OPER'] == 0)], bucket_name=bucket_name,
                           s3_key=f'Cat42/{nome_empresa.title()}/cods_a_verificar{nome_empresa}_{cnpj}.xlsx', file_type='xlsx')
    sys.exit()

tabela_2 = tabela_2[(tabela_2['DATA'] >= '2020-01-01') & (tabela_2['DATA'] <= '2020-12-31')]
tabela_2_filt = tabela_2[['CHV_DOC', 'DATA', 'CFOP', 'NUM_ITEM', 'COD_ITEM',
                    'IND_OPER', 'SUB_TIPO', 'QTD_CAT', 'QTD_EFD', 'ICMS_TOT','VL_CONFR_0', 'COD_LEGAL',
                    'ALIQUOTA', 'VALOR', 'Valor Base Cálculo ICMS ST Retido Operação Anterior',
                    'Valor Complementar', 'Valor ICMS Substituição Tributária', 'Valor ICMS Operação',
                    'Valor ICMS ST Retido', 'Valor ICMS Substituto', 'CST',
                    'DESCRICAO', 'CODIGO_BARRA', 'UNIDADE', 'N C M','CEST', 'CNPJ EMITENTE', 'CNPJ DESTINATARIO',
                    'Valor Base Cálculo ICMS Substituição Tributária', 'vBCST', 'FONTE']]

if tabela_2[tabela_2['ALIQUOTA'].isnull()].shape[0] > 0:
    print('Erro encontrado. Existem Alíquotas vazias, favor verificar')
    print('Os seguintes códigos não possuem Aliquota:')
    print(list(tabela_2[tabela_2['ALIQUOTA'].isnull()]['COD_ITEM'].unique()))
    print(tabela_2[tabela_2[['CHV_DOC','COD_ITEM', 'DESCRICAO', 'FONTE']]])
    sys.exit()

tabela_2_filt['COMBINACAO_CHV_NUM'] = tabela_2_filt['CHV_DOC'] + '-' + tabela_2_filt['NUM_ITEM'].astype(str)
# Criando uma coluna temporária para a combinação de CHAVE_ORIGINAL e COD_DO_ITEM em efd
efd['COMBINACAO_CHAVE_COD'] = efd['chave_nfe'] + '-' + efd['codigo_do_item'].astype(str)

# Cenário
condicao_cenario = (
    (tabela_2_filt['IND_OPER'] == 0) &
    (~tabela_2_filt['COMBINACAO_CHV_NUM'].isin(efd['COMBINACAO_CHAVE_COD']))
)

# Verificando se há alguma linha que atende ao cenário
tabela_2_final = tabela_2_filt[~condicao_cenario]
# Checagem de duplicação de par chave-item


tabela_2_final['CHAVE_ITEM'] = tabela_2_final['CHV_DOC'].astype(str) + '-' + tabela_2_final['NUM_ITEM'].astype(str)

duplicate_mask = tabela_2_final['CHAVE_ITEM'].duplicated(keep=False)
duplicate_df = tabela_2_final[duplicate_mask]
if duplicate_df.shape[0] > 0:
    print('Erro encontrado: combinação Chave-Item duplicada. Por favor verificar')
    sys.exit()

tabela_2_final['CHAVE_ITEM'] = tabela_2_final['CHV_DOC'].astype(str) + '-' + tabela_2_final['NUM_ITEM'].astype(str)

duplicate_mask = tabela_2_final['CHAVE_ITEM'].duplicated(keep=False)
duplicate_df = tabela_2_final[duplicate_mask]
if duplicate_df.shape[0] > 0:
    print('Erro encontrado: combinação Chave-Item duplicada. Por favor verificar')
    sys.exit()

tabela_2_final['ICMS_TOT'] = np.where(tabela_2_final['CFOP'].isin([1102, 2102]), np.nan, tabela_2_final['ICMS_TOT'])

# Salvamento do arquivo da tabela 2

salvar_dataframe_no_s3(tabela_2_final,
                       bucket_name,
                       s3_key=f'Cat42/{nome_empresa.title()}/Tabela 2/tabela_2_{nome_empresa.title()}_{cnpj}.xlsx', file_type='xlsx')
