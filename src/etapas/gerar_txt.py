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
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), 
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                  region_name=os.getenv('AWS_DEFAULT_REGION')
                  )
# Importação das planilhas necessárias para
# gerar os documentos da 000, 0150, 0200, 1050 e 1100

planilha = ler_arquivo_para_dataframe(bucket_name=bucket_name, s3_key=f'Cat42/{nome_empresa.title()}/Ficha 3/ficha_3_{nome_empresa.title()}_{cnpj}.xlsx',
                                  file_type='xlsx')
planilha = planilha.reset_index()
planilha['DATA'] = pd.to_datetime(planilha['DATA'], format='%Y-%m-%d')

tabela_2 = ler_arquivo_para_dataframe(bucket_name, f'Cat42/{nome_empresa.title()}/Tabela 2/tabela_2_{nome_empresa.title()}_{cnpj}.xlsx', file_type='xlsx')
tabela_2 = tabela_2.sort_values(by=['COD_ITEM', 'DATA', 'IND_OPER', 'SUB_TIPO'], 
                           ascending=[True, True, True, True])

planilha = planilha.merge(tabela_2[['CHV_DOC', 'NUM_ITEM', 'DESCRICAO', 'CODIGO_BARRA', 'UNIDADE', 'N C M', 'CEST',
                'CNPJ DESTINATARIO']],
            on=['CHV_DOC', 'NUM_ITEM'], how='left')

planilha['COD_ITEM'] = planilha['COD_ITEM'].astype(str)
planilha['DESCRICAO'] = planilha['DESCRICAO'].str.replace('"', "'")
# planilha['N C M'] = pd.to_numeric(planilha['N C M'])
planilha['N C M'] = planilha['N C M'].astype(str).str.replace(r'\.0$', '', regex=True)

planilha['CEST'] = pd.to_numeric(planilha['CEST'])
planilha['CEST'] = planilha['CEST'].astype('Int64').astype(str).replace('<NA>', np.nan)
planilha['CEST'] = planilha['CEST'].astype(str).str.replace(r'\.0$', '', regex=True)

planilha['CNPJ EMITENTE'] = pd.to_numeric(planilha['CNPJ EMITENTE'])
planilha['CNPJ EMITENTE'] = planilha['CNPJ EMITENTE'].astype('Int64').astype(str).replace('<NA>', np.nan)

planilha['QTD_CAT'] = planilha['QTD_CAT'].apply(lambda x: f"{x:.3f}".replace('.', ',') if isinstance(x, (int, float)) else x) 

planilha['COD_LEGAL_PCAT'] = pd.to_numeric(planilha['COD_LEGAL_PCAT'])
planilha['COD_LEGAL_PCAT'] = planilha['COD_LEGAL_PCAT'].astype('Int64').astype(str).replace('<NA>', np.nan)

planilha['CFOP'] = pd.to_numeric(planilha['CFOP'])
planilha['CFOP'] = planilha['CFOP'].astype('Int64').astype(str).replace('<NA>', np.nan)

planilha['UNIDADE'] = planilha['UNIDADE'].str.strip()

def format_number(value):
    return '{:,.2f}'.format(value).replace(',', '.')

planilha = planilha.drop_duplicates()

planilha['CODIGO_BARRA'] = np.where(pd.notnull(planilha['CODIGO_BARRA']), planilha['CODIGO_BARRA'].astype(str).str.replace(r'\.0$', '', regex=True),
                                   np.nan)

mes_ano = []
for date in planilha['DATA']:
    mes_ano.append(str(date)[5:7] + str(date)[0:4])

planilha['Data'] = mes_ano

planilha.replace('nan', np.nan, inplace=True)

planilha['CEST'] = planilha['CEST'].replace('0', pd.NA)

planilha['CEST'] = planilha['CEST'].fillna('').astype(str)

# Second, add leading '0' to values with length equal to 6
planilha.loc[planilha['CEST'].str.len() == 6, 'CEST'] = '0' + planilha['CEST']

planilha['CEST'] = planilha['CEST'].replace('', np.nan)

planilha['N C M'] = planilha['N C M'].apply(lambda x: str(x).zfill(8) if len(str(x)) < 8 else str(x))

planilha['CODIGO_BARRA'] = planilha['CODIGO_BARRA'].replace('SEM GTIN      ', np.nan)

# Geração do dataset base para geração da 1100

cols_1100 = ['CHV_DOC', 'DATA', 'NUM_ITEM', 'IND_OPER', 'COD_ITEM', 'CFOP','QTD_CAT', 'ICMS_TOT_PCAT', 'VLR_CONFR_PCAT', 'COD_LEGAL_PCAT', 'FONTE']

df_1100 = planilha.copy()
df_1100 = df_1100[cols_1100]
df_1100['Data'] = df_1100['DATA']
df_1100['DATA'] = df_1100['DATA'].astype(str)
df_1100['DATA'] = [datetime.strptime(x, '%Y-%m-%d').strftime('%d%m%Y') for x in df_1100['DATA']]
#df_1100.rename(coluns={'Número Item': 'NUM_ITEM'}, inplace=True)


df_1100['QTD_FIN'] = df_1100['QTD_CAT']
df_1100['COD_REG'] = ['1100']*df_1100.shape[0]
df_1100 = df_1100[['COD_REG','CHV_DOC','Data','DATA','NUM_ITEM','IND_OPER','COD_ITEM','CFOP','QTD_FIN',
                   'ICMS_TOT_PCAT','VLR_CONFR_PCAT','COD_LEGAL_PCAT', 'FONTE']]

df_1100['Data'] = df_1100['Data'].astype(str)
df_1100['Data'] = [datetime.strptime(x, '%Y-%m-%d').strftime('%m%Y') for x in df_1100['Data']]

df_1100['QTD_FIN'] = df_1100['QTD_FIN'].replace('nan', np.nan)

# Leitura do arquivo da 1050

arquivo_1050 = ler_arquivo_para_dataframe(bucket_name, f'Cat42/{nome_empresa.title()}/1050/1050_{nome_empresa.title()}.xlsx', file_type='xlsx')

mes_ano = []
for date in arquivo_1050['REF']:
    mes_ano.append(str(date)[5:7] + str(date)[0:4])
    
arquivo_1050['DATA'] = [datetime.strptime(x, '%Y%m').strftime('%m%Y') for x in arquivo_1050['REF'].astype(str)]

# Definição de uma função para exportar os
# dataframes de cada documentação em um
# arquivo .txt com a formatação desejada

def dataframe_to_txt_with_crlf(data, bucket_name, path):
    # Criar uma instância do cliente S3
    s3_client = boto3.client('s3')
    
    # Criar uma string no formato que seria gravada no arquivo local
    output = StringIO()
    for index, row in data.iterrows():
        row_str = '|'.join(map(lambda x: '' if pd.isna(x) else str(x), row)) + '\r\n'
        output.write(row_str)
    
    # Voltar o ponteiro para o início do StringIO antes de fazer o upload
    output.seek(0)
    
    # Fazer o upload do conteúdo para o S3
    s3_client.put_object(Body=output.getvalue(), Bucket=bucket_name, Key=path)

def format_number(value):
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')  # Set your desired locale
    formatted_value = locale.format('%.2f', value, grouping=True)
    formatted_value = str(formatted_value).replace('.', '')
    return formatted_value

query = 'SELECT * FROM "0150" '
df_0150 = pd.read_sql_query(query, connection)
df_0150 = df_0150[['empresa', 'registro', 'codigo_participante', 'razao_social', 'codigo_pais', 
                   'cnpj', 'cpf', 'codigo_municipio', 'ie', 'data']]

df_0150['registro'] = '0150'
df_0150 = df_0150[df_0150['empresa'] == f'{cnpj}']
if df_0150.shape[0] == 0:
        print(f'CNPJ {cnpj} não possui 0150. Favor atualizar arquivo')
        sys.exit()
df_0150['data'] = df_0150['data'].str.slice(0, 10).astype(str)
df_0150['data'] = [datetime.strptime(x, '%d/%m/%Y').strftime('%m%Y') for x in df_0150['data']]
df_0150['cnpj'] = df_0150['cnpj'].replace('', np.nan)
df_0150['ie'] = df_0150['ie'].replace('', np.nan)
df_0150['ie'] = df_0150['ie'].str.lstrip('\n')
df_0150['codigo_participante'] = df_0150['codigo_participante'].str.replace(r'^(FOR|CLI)', '', regex=True)

df_0150 = df_0150[['registro', 'codigo_participante', 'razao_social', 'codigo_pais',
                    'cnpj', 'cpf', 'ie', 'codigo_municipio', 'data']]

df_0150 = df_0150[df_0150['cnpj'].notnull()]
df_0150 = df_0150.drop_duplicates()

def fill_missing_ie(group):
    non_null_ie = group.dropna(subset='ie')['ie']
    if not non_null_ie.empty:
        group['ie'] = non_null_ie.iloc[0]
    elif group['ie'].isnull().all():
        group['ie'] = np.nan
    return group

df_0150 = df_0150.groupby('cnpj', group_keys=False).apply(fill_missing_ie)

df_0150_final = pd.DataFrame()

# Get the unique 'PERIODO' values
unique_periodos = df_0150['data'].unique()

# Iterate through unique 'PERIODO' values
for data_periodo in unique_periodos:
    # Filter the DataFrame for the current 'PERIODO' value
    filtered_df = df_0150[df_0150['data'] == data_periodo]
    
    # Drop duplicates in the 'CNPJ' column for the current 'PERIODO'
    filtered_df = filtered_df.drop_duplicates(subset='cnpj', keep='first')
    
    # Append the filtered results to the final DataFrame
    df_0150_final = pd.concat([df_0150_final, filtered_df])

# Reset the index of the final DataFrame
df_0150_final.reset_index(drop=True, inplace=True)
# df_0150_final = df_0150_final[df_0150_final['registro'] == '0150']

df_0150_final = df_0150_final.drop_duplicates()

df_cnpj_repetido = pd.DataFrame(columns=df_0150_final.columns)

dates = list(df_0150_final['data'].unique())
for date in dates:
    for cnpj_ in df_0150_final[df_0150_final['data'] == date]['cnpj'].unique():
        if len(df_0150_final.loc[(df_0150_final['data'] == date) & (df_0150_final['cnpj'] == cnpj_)]) > 1:
            df = df_0150_final.loc[(df_0150_final['data'] == date) & (df_0150_final['cnpj'] == cnpj_)]
            df_cnpj_repetido = pd.concat([df_cnpj_repetido, df])
            
if df_cnpj_repetido.shape[0] > 0:
    print('Existe repetição de CNPJ. Favor verificar')
    print(f'O CNPJ repetido é: {list(df_cnpj_repetido["cnpj"].unique())}')
    print(f'A data em que ocorreu a repetição é {list(df_cnpj_repetido["data"].unique())}')
    sys.exit()

df_ie_repetido = pd.DataFrame(columns=df_0150_final.columns)


dates = list(df_0150_final['data'].unique())
for date in dates:
    for ie in df_0150_final[df_0150_final['data'] == date]['ie'].unique():
        if len(df_0150_final.loc[(df_0150_final['data'] == date) & (df_0150_final['ie'] == ie)]) > 1:
            df = df_0150_final.loc[(df_0150_final['data'] == date) & (df_0150_final['ie'] == ie)]
            df_ie_repetido = pd.concat([df_ie_repetido, df])
    
if df_ie_repetido.shape[0] > 0:
    print('Existe repetição de Inscrição Estadual. Favor verificar')
    print(f'A IE repetida é: {list(df_ie_repetido["ie"].unique())}')
    print(f'A data em que ocorreu a repetição é {list(df_ie_repetido["data"].unique())}')
    sys.exit()

# Remove duplicatas
df_0150_final = df_0150_final.drop_duplicates(subset=('codigo_participante', 'data'), keep='first')

# Verifica duplicatas diretamente no DataFrame
duplicados = df_0150_final[df_0150_final.duplicated(subset=['codigo_participante', 'data'], keep=False)]

if not duplicados.empty:
    mensagem = 'ATENÇÃO. EXISTE CODIGO DE PARTICIPANTE REPETIDO. FAVOR CHECAR'
    print(mensagem)
    for _, row in duplicados.iterrows():
        print(f"O código repetido é igual a {row['codigo_participante']}")
        print(f"A data da repetição é igual a {row['data']}")
    sys.exit()

# Iterate through each file in the folder
query = 'SELECT * FROM "0000" '
df_0000 = pd.read_sql_query(query, connection)
df_0000['data'] = df_0000['data'].astype(str).str.slice(0, 10)
df_0000['data'] = [datetime.strptime(x, '%d/%m/%Y').strftime('%m%Y') for x in df_0000['data']]
df_0000['COD_VER'] = ['01'] * df_0000.shape[0]
df_0000['COD_FIN'] = ['00'] * df_0000.shape[0]
df_0000 = df_0000[['empresa','registro', 'data', 'razao_social', 'cnpj', 'ie',
        'codigo_municipio', 'COD_VER', 'COD_FIN']]

df_0150_final['codigo_municipio'] = pd.to_numeric(df_0150_final['codigo_municipio'])

df_0150_final_new = df_0150_final[df_0150_final['ie'].notnull()]

df_0150_final_new['cnpj'] = df_0150_final_new['cnpj'].apply(lambda x: str(x).zfill(14) if len(str(x)) < 14 else str(x))

# Função para verificar o dígito verificador de um CNPJ
def verifica_digito_verificador(cnpj):
    cnpj = [int(d) for d in str(cnpj) if d.isdigit()]  # Converte o CNPJ em uma lista de números inteiros
    if len(cnpj) != 14:
        return False  # CNPJ deve ter 14 dígitos

    # Calcula o primeiro dígito verificador
    soma = 0
    peso = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    for i in range(12):
        soma += cnpj[i] * peso[i]
    resto = soma % 11
    if resto < 2:
        digito_verificador1 = 0
    else:
        digito_verificador1 = 11 - resto

    # Calcula o segundo dígito verificador
    soma = 0
    peso.insert(0, 6)
    for i in range(13):
        soma += cnpj[i] * peso[i]
    resto = soma % 11
    if resto < 2:
        digito_verificador2 = 0
    else:
        digito_verificador2 = 11 - resto

    # Verifica se os dígitos verificadores são iguais aos dígitos originais
    return cnpj[-2] == digito_verificador1 and cnpj[-1] == digito_verificador2

# Exemplo de DataFrame
df = df_0150_final_new.copy()

# Aplica a função verifica_digito_verificador à coluna 'CNPJ' do DataFrame
df['Dígito_Verificador_Válido'] = df['cnpj'].apply(verifica_digito_verificador)

if len(df['Dígito_Verificador_Válido'].unique()) > 1:
    print('Existem CNPJs com dígito verificador inválido. Favor checar.')
    print(f'O CNPJ com dígito verificador inválido é {list(df["cnpj"].unique())}')
    sys.exit()

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

# Geração dos TXTs da CAT

planilha = planilha[planilha['Data'] != '102019']
for date in planilha['Data'].unique():
#for date in ['112019']:
    # Geração da 0000
    df_0000_ = df_0000[df_0000['data'] == date][df_0000['cnpj'] == f'{cnpj}']
    df_0000_['ie'] = df_0000_['ie'].str.replace('-', '')
    df_0000_ = df_0000_.drop('empresa', axis=1)
    if df_0000_.shape[0] != 1:
        print(f'Erro no 0000 do CNPJ {cnpj} para a data {date}. Favor verificar')
        sys.exit()
        
    # Geração da 0150

    # df_0150 has originally 8 columns
    df_0150_ = df_0150_final_new[df_0150_final_new['data'] == date]
    if df_0150_.shape[0] == 0:
        print(f'CNPJ {cnpj} não possui 0150 para a data {date}. Favor atualizar arquivo')
        sys.exit()
    df_0150_ = df_0150_[df_0150_['cnpj'].notnull()]
    df_0150_['ie'] = df_0150_['ie'].str.replace('-', '')
    df_0150_ = df_0150_[['registro', 'codigo_participante', 'razao_social', 'codigo_pais', 'cnpj', 'cpf',
                'ie', 'codigo_municipio']]
    
    # Geração da 0200
    df_0200 = planilha[planilha['Data'] == date][['COD_ITEM','DESCRICAO','CODIGO_BARRA','UNIDADE','N C M','ALIQUOTA','CEST']].drop_duplicates()
    df_0200['COD_REG'] = ['0200']*(df_0200.shape[0])
    df_0200 = df_0200[['COD_REG', 'COD_ITEM','DESCRICAO','CODIGO_BARRA','UNIDADE','N C M','ALIQUOTA','CEST']]
    df_0200['ALIQUOTA'] = df_0200['ALIQUOTA'].astype(float).astype(int)
    df_0200['CODIGO_BARRA'] = df_0200['CODIGO_BARRA'].astype(str)
    df_0200['CODIGO_BARRA'] = df_0200['CODIGO_BARRA'].replace('nan', np.nan)
    df_0200.drop_duplicates(subset='COD_ITEM', keep='first', inplace=True)
    for cod in list(df_0200['COD_ITEM'].unique()):
        if df_0200[df_0200['COD_ITEM'] == cod].shape[0] > 1:
            mensagem = 'ATENÇÃO. EXISTE CODIGO DE PRODUTO DUPLICADO NO 0200.'
            print(mensagem)
            print(f'O código duplicado é {cod}')
            sys.exit()
    # Geração da 1050
    unique_cod_items = planilha[planilha['Data'] == date]['COD_ITEM'].unique()
    arquivo_1050['COD_ITEM'] = arquivo_1050['COD_ITEM'].astype(str)
    filtered = arquivo_1050[arquivo_1050['DATA'] == date][arquivo_1050['COD_ITEM'].isin(unique_cod_items)]
    filtered

    df_1050 = filtered[['COD_ITEM','QTD_INI', 'ICMS_INI', 'SALDO_FINAL_MES_QTD', 'SALDO_FINAL_MES_ICMS']]
    
    df_1050['COD_REG'] = ['1050']*(df_1050.shape[0])
    df_1050['QTD_INI'] = df_1050['QTD_INI'].astype(float).astype(int).astype(str) + ',000'
    df_1050['ICMS_INI'] = df_1050['ICMS_INI'].astype(float).map(format_number)
    df_1050['SALDO_FINAL_MES_QTD'] = df_1050['SALDO_FINAL_MES_QTD'].astype(float).astype(int).astype(str) + ',000'
    df_1050['SALDO_FINAL_MES_ICMS'] = df_1050['SALDO_FINAL_MES_ICMS'].astype(float).map(format_number)
    
    df_1050 = df_1050[['COD_REG', 'COD_ITEM', 'QTD_INI', 'ICMS_INI', 'SALDO_FINAL_MES_QTD', 'SALDO_FINAL_MES_ICMS']]
    df_1050.rename(columns={'COD_REG': 'REG'}, inplace=True)
    
    # Geração da 1100
    df_1100_ = df_1100[df_1100['Data'] == date]
    if df_1100_[df_1100_['VLR_CONFR_PCAT'] < 0.01].shape[0] > 0:
        mensagem = 'ATNEÇÂO. EXISTE VALOR DE CONFRONTO IGUAL A 0.'
        print(mensagem)
        sys.exit()
    df_1100_['ICMS_TOT_PCAT'] = df_1100_['ICMS_TOT_PCAT'].astype(float).map(format_number)
    df_1100_['VLR_CONFR_PCAT'] = df_1100_['VLR_CONFR_PCAT'].astype(float).map(format_number)
    df_1100_['VLR_CONFR_PCAT'] = df_1100_['VLR_CONFR_PCAT'].astype(str).replace('nan', np.nan)
    df_1100_['ICMS_TOT_PCAT'] = df_1100_['ICMS_TOT_PCAT'].astype(str).replace('nan', np.nan)
    df_1100_ = df_1100_[['COD_REG','CHV_DOC','DATA','NUM_ITEM','IND_OPER','COD_ITEM','CFOP','QTD_FIN','ICMS_TOT_PCAT','VLR_CONFR_PCAT','COD_LEGAL_PCAT', 'FONTE']]

            
    cnpj_loja = df_0000[df_0000['empresa'] == f'{cnpj}']['cnpj'].values[0]
    df_1100_['CHV_DOC'] = np.where((df_1100_['CHV_DOC'].str.slice(6,20).isin(cnpjs)) | (df_1100_['IND_OPER'] == 1),
                           df_1100_['CHV_DOC'].str.slice(0,6) + cnpj_loja + df_1100_['CHV_DOC'].str.slice(20,44),
                           df_1100_['CHV_DOC'])
    
    df_1100_['DV_NFe'] = df_1100_['CHV_DOC'].apply(recalcular_digito_verificador)
    df_1100_['CHV_DOC'] = df_1100_['CHV_DOC'].str.slice(0,-1) + df_1100_['DV_NFe'].astype(str)
    
    df_1100_['CNPJ'] = df_1100_['CHV_DOC'].str.slice(6,20)
    for cnpj_ in df_1100_['CNPJ'].unique():
        if df_0150_[df_0150_['cnpj'] == cnpj_].shape[0] == 0:
            mensagem = 'ATENÇÃO. EXISTE PARTICIPANTE NO 1100 QUE NÃO ESTÁ NO 0150.'
            print(mensagem)
            print(f'O participante faltante no 0150 tem CNPJ {cnpj_}, para o mês {date}')
            sys.exit()
    
    df_1100_ = df_1100_[['COD_REG','CHV_DOC','DATA','NUM_ITEM','IND_OPER','COD_ITEM','CFOP','QTD_FIN','ICMS_TOT_PCAT','VLR_CONFR_PCAT','COD_LEGAL_PCAT']]
    
    # Geração do .txt da 0000
    dataframe_to_txt_with_crlf(data=df_0000_,
                               bucket_name=bucket_name, 
                               path=f'Cat42/{nome_empresa.title()}/Txts/Teste_CNPJ_{cnpj}/0000_{date}_{cnpj}.txt')
    print(f'Gerado o arquivo 0000 e salvo em Cat42/{nome_empresa.title()}/Txts/Teste_CNPJ_{cnpj}/0000_{date}_{cnpj}.txt')
    
    # Geração do .txt da 0150
    dataframe_to_txt_with_crlf(data=df_0150_,
                               bucket_name=bucket_name,
                                path=f'Cat42/{nome_empresa.title()}/Txts/Teste_CNPJ_{cnpj}/0150_{date}_{cnpj}.txt')
    print(f'Gerado o arquivo 0150 e salvo em Cat42/{nome_empresa.title()}/Txts/Teste_CNPJ_{cnpj}/0150_{date}_{cnpj}.txt')

    # Geração do .txt da 0200
    dataframe_to_txt_with_crlf(data=df_0200,
                               bucket_name=bucket_name,
                               path=f'Cat42/{nome_empresa.title()}/Txts/Teste_CNPJ_{cnpj}/0200_{date}_{cnpj}.txt')
    print(f'Gerado o arquivo 0200 e salvo em Cat42/{nome_empresa.title()}/Txts/Teste_CNPJ_{cnpj}/0200_{date}_{cnpj}.txt')

    # Geração do .txt da 1050
    dataframe_to_txt_with_crlf(data=df_1050,
                               bucket_name=bucket_name, 
                               path=f'Cat42/{nome_empresa.title()}/Txts/Teste_CNPJ_{cnpj}/1050_{date}_{cnpj}.txt')
    print(f'Gerado o arquivo 1050 e salvo em Cat42/{nome_empresa.title()}/Txts/Teste_CNPJ_{cnpj}/1050_{date}_{cnpj}.txt')

    # Geração do .txt da 1100
    dataframe_to_txt_with_crlf(data=df_1100_,
                               bucket_name=bucket_name, 
                               path=f'Cat42/{nome_empresa.title()}/Txts/Teste_CNPJ_{cnpj}/1100_{date}_{cnpj}.txt')
    print(f'Gerado o arquivo 1100 e salvo em Cat42/{nome_empresa.title()}/Txts/Teste_CNPJ_{cnpj}/1100_{date}_{cnpj}.txt')

    # Geração de um arquivo .txt único,
    # contendo todas as informações das documentações
    # da 0000, 0150, 0200, 1050 e 1100
    # dentro do formato correto


    # Listar arquivos no bucket S3 com o prefixo correspondente
    prefix = f"Cat42/{nome_empresa.title()}/Txts/Teste_CNPJ_{cnpj}/"
    files = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Agrupar os arquivos por data
    files_by_date = {}

    for file in files.get('Contents', []):
        file_key = file['Key']
        
        # Filtrar os arquivos com o padrão de data e CNPJ
        if f"{date}_{cnpj}" in file_key and file_key.endswith('.txt'):
            # Separar o arquivo pela data (ex: 032020)
            file_date = f"{file_key.split('_')[3]}"  # Obtém a parte de data do nome do arquivo
            
            # Adiciona o arquivo ao grupo de data
            if file_date not in files_by_date:
                files_by_date[file_date] = []
            files_by_date[file_date].append(file_key)

    # Processar os arquivos agrupados e fazer upload para o S3
    for file_date, file_list in files_by_date.items():
        merged_content = b""
        
        # Juntar os arquivos de uma mesma data
        for file_key in file_list:
            # Baixar o conteúdo do arquivo S3
            file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
            merged_content += file_obj['Body'].read()

        # Criar o caminho do arquivo de saída, incluindo a data no nome
        s3_path = f'Cat42/{nome_empresa.title()}/Txts/cat_{nome_empresa.title()}_{cnpj}_{file_date}.txt'
        
        # Fazer o upload do arquivo concatenado para o S3
        s3.put_object(Body=merged_content, Bucket=bucket_name, Key=s3_path)
        
        print(f'Arquivo da CAT salvo em {s3_path}')