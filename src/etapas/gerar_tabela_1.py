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
from tqdm import tqdm
import zipfile
import re
import os
# from sqlalchemy import create_engine

pd.set_option("display.max_columns", None)

nome_empresa = input("Digite o nome da empresa: ")

if nome_empresa.lower() == 'tateti':
    cnpj = "65369985000504"
    cnpj_produtos = "65369985000334"
    cnpjs = [cnpj]

if nome_empresa.lower() == 'ladakh':
    cnpj = "07318052000150"
    cnpj_produtos = "07318052000150"
    cnpjs = [cnpj]

if nome_empresa.lower() == 'sonda':
    cnpj = "01937635001316"
    cnpj_produtos = "01937635001316"
    cnpjs = [cnpj]

if nome_empresa.lower() == 'mensa':
    cnpj = "10290457000301"
    cnpj_produtos = "10290457000484"
    cnpjs = [cnpj]

if nome_empresa.lower() == 'casa mimosa':
    cnpj = "62978978000180"
    cnpj_produtos = "62978978000180"
    cnpjs = [cnpj]

if nome_empresa.lower() == 'tobras':
    cnpj = "05759383001090"
    cnpj_produtos = None
    cnpjs = [cnpj]

if nome_empresa.lower() == 'morikawa':
    cnpj = "05886844000286"

# engine = create_connection(
#     f"postgresql+psycopg2://cat:5pM2h0MBQu9JHkxHud2A@177.11.49.194:3361/4btaxtech"
# )

connection = psycopg2.connect(
    user='cat',
    password='5pM2h0MBQu9JHkxHud2A',
    host='177.11.49.194',
    port="3361",
    database='4btaxtech'
)

# Variáveis para acesso ao s3
bucket_name = '4btech'


print("✅ Cliente S3 autenticado com sucesso!")


def formatar_colunas(tabela, tipo):
    """ 
    Função para formatar as colunas dos arquivos para uma nomenclatura padrão
    """
    if tipo == 'entrada_opprp':
        # Formatação dos nomes das colunas
        tabela.rename(columns={'Número CNPJ Destinatário (char)': 'Número CNPJ Destinatário',
                               'Documento Fiscal.Número CNPJ Emitente (char)': 'Número CNPJ Emitente',
                               'Valor ICMS': 'Valor ICMS Operação',
                               'Código CFOP (04 Posições)': 'CFOP',
                               'Código CEST': 'CEST'}, inplace=True
                      )

        # Formatação dos dados da colunas de data
        tabela = tabela[['Chave Acesso NFe', 'Data Emissão', 'Número Item',
                         'Código Produto ou Serviço', 'CFOP',
                         'Quantidade Comercial', 'Valor Produto ou Serviço',
                         'Valor ICMS Operação', 'Valor ICMS Substituição Tributária',
                         'Valor Base Cálculo ICMS Substituição Tributária',
                         'Valor Base Cálculo ICMS ST Retido Operação Anterior',
                         'Valor ICMS ST Retido Operação Anterior',
                         'Descrição Produto', 'Código GTIN', 'Unidade Comercial', 'Código NCM',
                         'CEST', 'Número CNPJ Emitente', 'Número CNPJ Destinatário',
                         ]]

        tabela['Data Emissão'] = pd.to_datetime(
            tabela['Data Emissão'], format='%d/%m/%Y')
        tabela['Tipo'] = 'entrada'
        tabela['Fonte'] = 'entrada_opprp'

        tabela = tabela[['Chave Acesso NFe', 'Data Emissão', 'Número Item',
                         'Código Produto ou Serviço', 'CFOP',
                         'Valor ICMS Operação', 'Quantidade Comercial',
                         'Valor Produto ou Serviço', 'Descrição Produto', 'Código GTIN',
                         'Unidade Comercial', 'CEST',
                         'Valor ICMS Substituição Tributária',
                         'Valor Base Cálculo ICMS Substituição Tributária',
                         'Valor ICMS ST Retido Operação Anterior',
                         'Valor Base Cálculo ICMS ST Retido Operação Anterior', 'Código NCM',
                         'Número CNPJ Emitente', 'Número CNPJ Destinatário', 'Tipo', 'Fonte']].dropna(subset='Chave Acesso NFe')

        tabela = tabela.loc[:, ~tabela.columns.duplicated()]

    elif tipo == 'entrada_sp':
        # Formatação dos nomes das colunas
        tabela.rename(columns={'Número CNPJ Destinatário (char)': 'Número CNPJ Destinatário',
                               'Valor ICMS': 'Valor ICMS Operação',
                               'Código CFOP (04 Posições)': 'CFOP',
                               'Código CEST': 'CEST'}, inplace=True
                      )

        # Formatação dos dados da colunas de data
        tabela = tabela[['Chave Acesso NFe', 'Data Emissão', 'Número Item',
                         'Código Produto ou Serviço', 'CFOP',
                         'Quantidade Comercial', 'Valor Produto ou Serviço',
                         'Valor ICMS Operação', 'Valor ICMS Substituição Tributária',
                         'Valor Base Cálculo ICMS Substituição Tributária',
                         'Valor Base Cálculo ICMS ST Retido Operação Anterior',
                         'Valor ICMS ST Retido Operação Anterior',
                         'Descrição Produto', 'Código GTIN', 'Unidade Comercial', 'Código NCM',
                         'CEST', 'Número CNPJ Emitente', 'Número CNPJ Destinatário',
                         ]]
        tabela['Data Emissão'] = pd.to_datetime(
            tabela['Data Emissão'], format='%d/%m/%Y')
        tabela['Tipo'] = 'entrada'
        tabela['Fonte'] = 'entrada_sp'

        tabela = tabela[['Chave Acesso NFe', 'Data Emissão', 'Número Item',
                         'Código Produto ou Serviço', 'CFOP',
                         'Valor ICMS Operação', 'Quantidade Comercial',
                         'Valor Produto ou Serviço', 'Descrição Produto', 'Código GTIN',
                         'Unidade Comercial', 'CEST',
                         'Valor ICMS Substituição Tributária',
                         'Valor Base Cálculo ICMS Substituição Tributária',
                         'Valor ICMS ST Retido Operação Anterior',
                         'Valor Base Cálculo ICMS ST Retido Operação Anterior', 'Código NCM',
                         'Número CNPJ Emitente', 'Número CNPJ Destinatário', 'Tipo', 'Fonte']].dropna(subset='Chave Acesso NFe')

        tabela = tabela.loc[:, ~tabela.columns.duplicated()]

    elif tipo == 'entrada_outras':
        # Formatação dos nomes das colunas
        tabela.rename(columns={'Quantidade Tributável': 'Quantidade Comercial',
                               'Código CEST (char)': 'CEST',
                               'Valor ICMS': 'Valor ICMS Operação',
                               'Número CNPJ Emitente (char)': 'Número CNPJ Emitente',
                               'Número CNPJ Destinatário (char)': 'Número CNPJ Destinatário',
                               'Código CFOP (04 Posições)': 'CFOP',
                               'Código GTIN (char)': 'Código GTIN'}, inplace=True
                      )

        # Formatação dos dados da colunas de data
        tabela = tabela[['Chave Acesso NFe', 'Data Emissão', 'Número Item',
                         'Código Produto ou Serviço', 'CFOP',
                         'Quantidade Comercial', 'Valor Produto ou Serviço',
                         'Valor ICMS Operação', 'Valor ICMS Substituição Tributária',
                         'Valor Base Cálculo ICMS Substituição Tributária',
                         'Valor Base Cálculo ICMS ST Retido Operação Anterior',
                         'Valor ICMS ST Retido Operação Anterior',
                         'Descrição Produto', 'Código GTIN', 'Unidade Comercial', 'Código NCM',
                         'CEST', 'Número CNPJ Emitente', 'Número CNPJ Destinatário',
                         ]]
        tabela['Data Emissão'] = pd.to_datetime(
            tabela['Data Emissão'], format='%d/%m/%Y')
        tabela['Tipo'] = 'entrada'
        tabela['Fonte'] = 'entrada_outras'

        tabela = tabela[['Chave Acesso NFe', 'Data Emissão', 'Número Item',
                         'Código Produto ou Serviço', 'CFOP',
                         'Valor ICMS Operação', 'Quantidade Comercial',
                         'Valor Produto ou Serviço', 'Descrição Produto', 'Código GTIN',
                         'Unidade Comercial', 'CEST',
                         'Valor ICMS Substituição Tributária',
                         'Valor Base Cálculo ICMS Substituição Tributária',
                         'Valor Base Cálculo ICMS ST Retido Operação Anterior', 'Código NCM',
                         'Número CNPJ Emitente', 'Número CNPJ Destinatário', 'Tipo', 'Fonte']].dropna(subset='Chave Acesso NFe')

        tabela = tabela.loc[:, ~tabela.columns.duplicated()]

    elif tipo == 'dfe':
        # Formatação dos nomes das colunas
        tabela.rename(columns={'Chave CF-e': 'Chave Acesso NFe',
                               'Código Produto': 'Código Produto ou Serviço',
                               'Valor Icms': 'Valor ICMS Operação',
                               'Código CFOP 04 Posições': 'CFOP',
                               'Valor Item': 'Valor Produto ou Serviço',
                               'Código EAN': 'Código GTIN',
                               'Código CEST': 'CEST'}, inplace=True
                      )

        # Formatação dos dados da colunas de data
        tabela = tabela[['Chave Acesso NFe', 'Data Emissão', 'Número Item',
                         'Código Produto ou Serviço', 'CFOP',
                         'Quantidade Comercial', 'Valor Produto ou Serviço', 'Valor ICMS Operação',
                         'Descrição Produto', 'Código GTIN',
                         'Unidade Comercial', 'CEST']]
        tabela['Data Emissão'] = pd.to_datetime(
            tabela['Data Emissão'], format='%m/%d/%Y')

        # Definição de colunas necessárias que não tem valor
        tabela['Valor ICMS Substituição Tributária'] = np.nan
        tabela['Valor Base Cálculo ICMS Substituição Tributária'] = np.nan
        tabela['Valor Base Cálculo ICMS ST Retido Operação Anterior'] = np.nan
        tabela['Código NCM'] = np.nan
        tabela['Número CNPJ Emitente'] = np.nan
        tabela['Número CNPJ Destinatário'] = np.nan
        tabela['Tipo'] = 'saida'
        tabela['Valor ICMS ST Retido Operação Anterior'] = np.nan
        tabela['Fonte'] = 'dfe'

        tabela = tabela[['Chave Acesso NFe', 'Data Emissão', 'Número Item',
                         'Código Produto ou Serviço', 'CFOP',
                         'Valor ICMS Operação', 'Quantidade Comercial',
                         'Valor Produto ou Serviço', 'Descrição Produto', 'Código GTIN',
                         'Unidade Comercial', 'CEST',
                         'Valor ICMS Substituição Tributária',
                         'Valor Base Cálculo ICMS Substituição Tributária',
                         'Valor ICMS ST Retido Operação Anterior',
                         'Valor Base Cálculo ICMS ST Retido Operação Anterior', 'Código NCM',
                         'Número CNPJ Emitente', 'Número CNPJ Destinatário', 'Tipo', 'Fonte']].dropna(subset='Chave Acesso NFe')

        tabela = tabela.loc[:, ~tabela.columns.duplicated()]

    elif tipo == 'saida_nfe':
        # Formatação dos nomes das colunas
        tabela.rename(columns={'Número CNPJ Emitente (char)': 'Número CNPJ Emitente',
                               'Valor ICMS': 'Valor ICMS Operação',
                               'Número CNPJ Destinatário (char)': 'Número CNPJ Destinatário',
                               'Código CFOP (04 Posições)': 'CFOP',
                               'Código CEST': 'CEST'}, inplace=True
                      )

        # Formatação dos dados da colunas de data
        tabela = tabela[['Chave Acesso NFe', 'Data Emissão', 'Número Item',
                         'Código Produto ou Serviço', 'CFOP',
                         'Quantidade Comercial', 'Valor Produto ou Serviço',
                         'Valor ICMS Operação', 'Descrição Produto', 'Código GTIN',
                         'Unidade Comercial', 'Código NCM', 'CEST',
                         'Número CNPJ Emitente', 'Número CNPJ Destinatário']]
        tabela['Data Emissão'] = pd.to_datetime(
            tabela['Data Emissão'], format='%d/%m/%Y')

        # Definição de colunas necessárias
        tabela['Valor ICMS Substituição Tributária'] = np.nan
        tabela['Valor Base Cálculo ICMS Substituição Tributária'] = np.nan
        tabela['Valor Base Cálculo ICMS ST Retido Operação Anterior'] = np.nan
        tabela['Tipo'] = 'saida'
        tabela['Valor ICMS ST Retido Operação Anterior'] = np.nan
        tabela['Fonte'] = 'saida_nfe'

        tabela = tabela[['Chave Acesso NFe', 'Data Emissão', 'Número Item',
                         'Código Produto ou Serviço', 'CFOP',
                         'Valor ICMS Operação', 'Quantidade Comercial',
                         'Valor Produto ou Serviço', 'Descrição Produto', 'Código GTIN',
                         'Unidade Comercial', 'CEST',
                         'Valor ICMS Substituição Tributária',
                         'Valor Base Cálculo ICMS Substituição Tributária',
                         'Valor ICMS ST Retido Operação Anterior',
                         'Valor Base Cálculo ICMS ST Retido Operação Anterior', 'Código NCM',
                         'Número CNPJ Emitente', 'Número CNPJ Destinatário', 'Tipo', 'Fonte']].dropna(subset='Chave Acesso NFe')

        tabela = tabela.loc[:, ~tabela.columns.duplicated()]

    return tabela


def agrupar_tabelas(entrada_opprp, entrada_sp, entrada_outras, dfe, saida_nfe):
    dfs = [entrada_opprp, entrada_sp, entrada_outras, dfe, saida_nfe]
    dfs_final = []

    for df in dfs:
        if not df.shape[0] == 0:
            dfs_final.append(df)

    df = pd.concat(dfs_final)

    return df


def ler_arquivo_para_dataframe(bucket_name, s3_key, file_type='csv', sep=None, header=0):
    """
    Lê um arquivo do S3 e carrega em um DataFrame do Pandas.
    """
    print(f"🔍 Bucket: {bucket_name}")
    print(f"🔍 S3 Key: '{s3_key}'")
    try:
        # Criar o client boto3 aqui, após o .env já ter sido carregado
        s3 = boto3.client('s3',
                          aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                          aws_secret_access_key=os.getenv(
                              'AWS_SECRET_ACCESS_KEY'),
                          region_name=os.getenv('AWS_DEFAULT_REGION')
                          )

        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        if file_type == 'csv':
            chunk_size = 100_000

            # Isso cria um "iterador", não um DataFrame completo. A memória ainda não foi usada.
            csv_iterator = pd.read_csv(BytesIO(response['Body'].read()),
                                       dtype=str,
                                       header=header,
                                       sep=sep,
                                       encoding='utf-8',
                                       chunksize=chunk_size,
                                       engine='python')  # Ajuste sep e encoding se necessário

            lista_de_resultados = []

            print("Iniciando o processamento em chunks...")
            # Agora, iteramos sobre o arquivo, pedaço por pedaço
            for chunk in tqdm(csv_iterator, desc="Lendo chunks do S3"):

                # Guarde apenas o resultado que te interessa
                lista_de_resultados.append(chunk)

            # Ao final, você pode consolidar os resultados
            df = pd.concat(lista_de_resultados)
            # df = pd.read_csv(
            #     BytesIO(response['Body'].read()), dtype=str, header=0, sep=sep, encoding='utf-8')
        elif file_type == 'xlsx':
            df = pd.read_excel(BytesIO(response['Body'].read()))
        else:
            raise ValueError(
                "Tipo de arquivo não suportado. Use 'csv' ou 'xlsx'.")
        print(f"✅ Arquivo '{s3_key}' lido com sucesso!")
        return df
    except Exception as e:
        raise ValueError(f"❌ Erro ao ler arquivo {s3_key} do S3: {e}")


def salvar_dataframe_no_s3(df, bucket_name, s3_key, file_type='csv'):
    """
    Salva um DataFrame do Pandas no S3.
    """
    try:
        # Criar o client aqui também
        s3 = boto3.client('s3',
                          aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                          aws_secret_access_key=os.getenv(
                              'AWS_SECRET_ACCESS_KEY'),
                          region_name=os.getenv('AWS_DEFAULT_REGION')
                          )

        buffer = BytesIO()
        if file_type == 'csv':
            df.to_csv(buffer, index=False, encoding='utf-8')
        elif file_type == 'xlsx':
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False)
        else:
            raise ValueError(
                "Tipo de arquivo não suportado. Use 'csv' ou 'xlsx'.")

        buffer.seek(0)
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())
        print(f"✅ Arquivo salvo com sucesso em '{s3_key}'.")
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo no S3: {e}")


def listar_arquivos_s3(bucket, prefix, extensao):
    """Lista arquivos em um prefixo do S3 que terminam com uma extensão específica."""
    arquivos = []
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
        for obj in page.get('Contents', []):
            if obj['Key'].lower().endswith(extensao):
                arquivos.append(obj['Key'])
    return arquivos


def processar_zip_s3(bucket, zip_key, cnpj):
    """
    Baixa um arquivo .zip do S3, navega em suas subpastas ('nfes' e 'dfes_e_efd170'),
    extrai, formata e agrupa os DataFrames por CNPJ.
    Retorna um dicionário onde cada chave é um CNPJ e o valor contém seus DFs.
    """
    print(f"⚙️  Processando arquivo ZIP: '{zip_key}'...")
    try:
        s3_response = s3.get_object(Bucket=bucket, Key=zip_key)
        zip_content_in_memory = BytesIO(s3_response['Body'].read())

        # Dicionário principal para agrupar dados por CNPJ
        # Estrutura: { 'cnpj1': {'dfe': [], 'entrada_outras': None, ...}, 'cnpj2': ... }
        cnpjs_data = {}

        with zipfile.ZipFile(zip_content_in_memory, 'r') as zip_archive:
            for file_path in tqdm(zip_archive.namelist(), desc="Lendo arquivos no ZIP"):

                if file_path.startswith('__MACOSX/') or file_path.endswith('/'):
                    continue

                # Extrai o CNPJ de 14 dígitos do caminho completo do arquivo
                match = re.search(r'(\d{14})', file_path)
                if not match:
                    # print(f"\n- Aviso: CNPJ não encontrado em '{file_path}'. Pulando.")
                    continue

                cnpj = match.group(1)

                # Se for a primeira vez que vemos este CNPJ, inicializa sua estrutura
                if cnpj not in cnpjs_data:
                    cnpjs_data[cnpj] = {
                        "dfe": [], "entrada_outras": None, "entrada_opprp": None,
                        "entrada_sp": None, "saida_nfe": None
                    }

                base_filename = os.path.basename(file_path)

                try:
                    with zip_archive.open(file_path) as inner_file:
                        df_temp = pd.read_csv(
                            inner_file, dtype=str, sep=';', encoding='utf-8', engine='python')

                        # Verifica em qual pasta o arquivo está e aplica a formatação
                        if 'nfes/' in file_path:
                            if base_filename.startswith('nfe_saidas_CNPJ-'):
                                cnpjs_data[cnpj]["saida_nfe"] = formatar_colunas(
                                    df_temp, tipo='saida_nfe')
                            elif base_filename.startswith('nfe_entrada_outras-ufs_CNPJ-'):
                                cnpjs_data[cnpj]["entrada_outras"] = formatar_colunas(
                                    df_temp, tipo='entrada_outras')
                            elif base_filename.startswith('nfe_entrada_oper_proprias_CNPJ-'):
                                cnpjs_data[cnpj]["entrada_opprp"] = formatar_colunas(
                                    df_temp, tipo='entrada_opprp')
                            elif base_filename.startswith('nfe_entrada_sp_CNPJ-'):
                                cnpjs_data[cnpj]["entrada_sp"] = formatar_colunas(
                                    df_temp, tipo='entrada_sp')

                        elif 'dfes_e_efd170/' in file_path and base_filename.startswith('dfe_'):
                            df_formatado = formatar_colunas(
                                df_temp, tipo='dfe')
                            cnpjs_data[cnpj]["dfe"].append(df_formatado)

                except Exception as e:
                    print(
                        f"\n⚠️ Erro ao processar o arquivo interno '{file_path}': {e}")

        # Após ler todos os arquivos, consolida as listas de DFs (no caso, só 'dfe')
        print("\nConsolidando DataFrames de DFE por CNPJ...")
        for cnpj, data in cnpjs_data.items():
            if data["dfe"]:
                data["dfe"] = pd.concat(data["dfe"], ignore_index=True)
            else:
                # Garante que seja um DF vazio se nenhum arquivo DFE for encontrado
                data["dfe"] = pd.DataFrame()

        print(
            f"✅ Arquivo ZIP '{zip_key}' processado. Encontrados dados de {len(cnpjs_data)} CNPJ(s).")
        return cnpjs_data

    except Exception as e:
        print(f"❌ Erro CRÍTICO ao processar o arquivo ZIP '{zip_key}': {e}")
        return None


cnpjs = ["05886844000367", "05886844000448", "05886844000600",
         "05886844000871", "05886844001096", "05886844001681",
         "05886844001843", "05886844001924", ]

for cnpj in cnpjs:
    print(f"\n{'='*20} INICIANDO PROCESSAMENTO PARA O CNPJ: {cnpj} {'='*20}")

    # Caminho base no S3 onde os arquivos estão localizados
    prefixo_s3 = f"Ressarcimento/{nome_empresa.title()}/Notas Fiscais/{cnpj}"

    # 1. Procura por um arquivo .zip no diretório
    arquivos_zip = listar_arquivos_s3(bucket_name, prefixo_s3, extensao='.zip')

    if not arquivos_zip:
        print(
            f"❌ Nenhum arquivo .zip encontrado para o CNPJ {cnpj} no caminho '{prefixo_s3}'. Pulando...")
        continue

    # Pega o primeiro arquivo zip encontrado
    zip_file_key = arquivos_zip[0]

    # 2. Processa o arquivo ZIP para obter todos os DataFrames formatados
    dfs_processados = processar_zip_s3(bucket_name, zip_file_key)

    if dfs_processados:
        # 3. Agrupa as tabelas extraídas do ZIP
        df_final = agrupar_tabelas(
            dfs_processados["entrada_opprp"],
            dfs_processados["entrada_sp"],
            dfs_processados["entrada_outras"],
            dfs_processados["dfe"],
            dfs_processados["saida_nfe"]
        )

        # 4. Salva o resultado final no S3
        if not df_final.empty:
            caminho_saida = f"Ressarcimento/{nome_empresa.title()}/Tabela 1/tabela_1_{cnpj}.csv"
            salvar_dataframe_no_s3(df_final, bucket_name, caminho_saida)
        else:
            print(
                f"⚠️ Nenhum dado foi processado para o CNPJ {cnpj}, arquivo final não foi gerado.")

print("\n🚀 Processamento concluído para todos os CNPJs!")
