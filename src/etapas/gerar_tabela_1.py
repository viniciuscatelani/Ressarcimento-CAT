import xml.etree.ElementTree as ET
import pandas as pd
import os
import io
import openpyxl
import boto3
import logging
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
import re

# Carregando variáveis de ambiente
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
print(f"Carregando .env de: {dotenv_path}")
load_dotenv(dotenv_path, override=True)

# Configurar o cliente S3
s3 = boto3.client('s3',
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                  region_name=os.getenv('AWS_DEFAULT_REGION'))

# Nome do bucket
bucket_name = '4btaxtech'

# Listar arquivos no S3
def listar_arquivos_s3(bucket, prefix):
    arquivos = []
    continuation_token = None

    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        arquivos.extend([obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.xml')])

        if not response.get('IsTruncated'):
            break

        continuation_token = response.get('NextContinuationToken')

    return arquivos

# Ler conteúdo de um arquivo no S3
def ler_arquivo_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')

# Salvar conteúdo no S3
def salvar_arquivo_s3(bucket, key, conteudo):
    s3.put_object(Bucket=bucket, Key=key, Body=conteudo.getvalue())

# Corrigir cabeçalho do XML
def corrigir_cabecalho_xml(conteudo_xml):
    return conteudo_xml.replace('<?xml version="1.0" encoding="UTF-8"?>', '', 1)

# Salvar arquivos problemáticos no S3
def salvar_arquivo_problema_s3(bucket, key, conteudo):
    pasta_problemas = 'Cat42/Tateti/XMLs_Problemas'
    chave_problematica = os.path.join(pasta_problemas, os.path.basename(key))
    s3.put_object(Bucket=bucket, Key=chave_problematica, Body=conteudo.encode('utf-8'))
    print(f"Arquivo problemático salvo em: {chave_problematica}")

def mover_arquivo_s3(bucket, arquivo_origem, pasta_destino):
    """
    Move um arquivo no S3 para uma nova pasta.
    """
    novo_caminho = os.path.join(pasta_destino, os.path.basename(arquivo_origem))
    s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': arquivo_origem}, Key=novo_caminho)

# Extração do XML 
def xml_to_dict(element):
    result = {}
    tag = element.tag.split('}')[-1]
    result[tag] = {}
    for key, value in element.attrib.items():
        result[tag][key] = value
    if element.text and element.text.strip():
        result[tag]['valor'] = element.text.strip()
    for sub_element in element:
        sub_result = xml_to_dict(sub_element)
        if sub_result:
            for sub_tag, sub_data in sub_result.items():
                if sub_tag not in result[tag]:
                    result[tag][sub_tag] = sub_data
                elif isinstance(result[tag][sub_tag], list):
                    result[tag][sub_tag].append(sub_data)
                else:
                    result[tag][sub_tag] = [result[tag][sub_tag], sub_data]
    if not result[tag]:
        del result[tag]
        result = element.text.strip() if element.text and element.text.strip() else None
    return result if result != {} else None

def extrair_dados_xml(filepath):

    """

    Analisa um arquivo XML e retorna um dicionário com os dados e os namespaces.

    """

    try:

        tree = ET.parse(filepath)

        root = tree.getroot()

        namespaces = {'nfe': 'http://www.portalfiscal.inf.br/nfe', 'nfce': 'http://www.portalfiscal.inf.br/nfce'}

        return xml_to_dict(root), namespaces

    except ET.ParseError as e:

        logging.error(f"Erro ao analisar o arquivo XML: {filepath} - {e}")

        return None, None

# Funções de extração e busca mantidas
def buscar_valores_icms_entrada(elemento, tags_desejadas):
    valores = {}
    if isinstance(elemento, dict):
        for chave, valor in elemento.items():
            if re.match(r"ICMS\d+", chave):
                valores.update(buscar_valores_icms_entrada(valor, tags_desejadas))
            elif chave in tags_desejadas and isinstance(valor, dict) and 'valor' in valor:
                valores[chave] = valor['valor']
            elif isinstance(valor, (dict, list)):
                if isinstance(valor, dict):
                    for subvalor in valor.values():
                        valores.update(buscar_valores_icms_entrada(subvalor, tags_desejadas))
                else:
                    for subvalor in valor:
                        valores.update(buscar_valores_icms_entrada(subvalor, tags_desejadas))
    elif isinstance(elemento, list):
        for item in elemento:
            valores.update(buscar_valores_icms_entrada(item, tags_desejadas))
    return valores

def buscar_valores_icms_saida(elemento, tags_desejadas):
    valores = {}
    if isinstance(elemento, dict):
        for chave, valor in elemento.items():
            if chave in tags_desejadas and isinstance(valor, dict) and 'valor' in valor:
                valores[chave] = valor['valor']
            elif isinstance(valor, (dict, list)):
                valores.update(buscar_valores_icms_saida(valor, tags_desejadas))
    elif isinstance(elemento, list):
        for item in elemento:
            valores.update(buscar_valores_icms_saida(item, tags_desejadas))
    return valores

# Funções de extração mantidas
def extrair_campos_entrada(dados_xml, namespaces, filepath=None):
    try:
        if 'nfeProc' in dados_xml:
            infNFe = dados_xml.get('nfeProc', {}).get('NFe', {}).get('infNFe', {})
            protNFe = dados_xml.get('nfeProc', {}).get('protNFe', {}).get('infProt', {})
        elif 'NFe' in dados_xml:
            infNFe = dados_xml.get('NFe', {}).get('infNFe', {})
            protNFe = {}
        else:
            return None

        emit = infNFe.get('emit', {})
        dest = infNFe.get('dest', {})

        itens = infNFe.get('det')
        if isinstance(itens, dict):
            itens = [itens]
        elif itens is None:
            return None

        itens_data = []
        for item in itens:
            prod = item.get('prod', {})
            imposto = item.get('imposto', {})

            tags_icms = {'vICMS', 'vICMSST', 'vBCST', 'vBCSTRet', 'vICMSSubstituto', 'vICMSSTRet'}
            valores_icms = buscar_valores_icms_entrada(imposto, tags_icms)

            item_data = {
                "Número Item": item.get('nItem'),
                "Código Produto ou Serviço": prod.get('cProd', {}).get('valor'),
                "CFOP": prod.get('CFOP', {}).get('valor'),
                "Quantidade Comercial": prod.get('qCom', {}).get('valor'),
                "Valor Produto ou Serviço": prod.get('vProd', {}).get('valor'),
                "Descrição Produto": prod.get('xProd', {}).get('valor'),
                "Unidade Comercial": prod.get('uCom', {}).get('valor'),
                "Código GTIN": prod.get('cEAN', {}).get('valor'),
                "Código NCM": prod.get('NCM', {}).get('valor'),
                "CEST": prod.get('CEST', {}).get('valor'),
                'Valor ICMS Operação': valores_icms.get('vICMS'),
                'Valor ICMS Substituição Tributária': valores_icms.get('vICMSST'),
                'Valor Base Cálculo ICMS Substituição Tributária': valores_icms.get('vBCST'),
                'Valor Base de Cálculo ICMS ST Retido operação Anterior': valores_icms.get('vBCSTRet'),
                'Valor ICMS Substituto': valores_icms.get('vICMSSubstituto'),
                'Valor ICMS ST Retido': valores_icms.get('vICMSSTRet')
            }
            itens_data.append(item_data)

        return {
            "Chave Acesso NFe": protNFe.get('chNFe', {}).get('valor'),
            "Data Emissão": infNFe.get('ide', {}).get('dhEmi', {}).get('valor'),
            "Número CNPJ Emitente": emit.get('CNPJ', {}).get('valor'),
            "Número CNPJ Destinatário": dest.get('CNPJ', {}).get('valor'),
            'itens': itens_data
        }

    except Exception as e:
        logging.error(f"Erro ao processar XML: {filepath} - {e}")
        return None

def extrair_campos_saida(dados_xml, namespaces):
    try:
        infCFe = dados_xml['CFe']['infCFe']
        emit = infCFe['emit']
        dest = infCFe.get('dest', {})
        itens = infCFe['det']
        if isinstance(itens, dict):
            itens = [itens]

        dados_cfe = []
        for item in itens:
            prod = item['prod']
            imposto = item.get('imposto', {})
            tags_icms_st = {'vICMSST', 'vBCST', 'vBCSTRet'}
            valores_icms_st = buscar_valores_icms_saida(imposto, tags_icms_st)

            dados_cfe.append({
                'Chave Acesso NFe': infCFe['Id'].replace("CFe", ""),
                'Data Emissão': infCFe['ide']['dEmi']['valor'],
                'Número Item': int(item['nItem']),
                'Código Produto ou Serviço': prod['cProd']['valor'],
                'CFOP': prod['CFOP']['valor'],
                'Quantidade Comercial': prod['qCom']['valor'],
                'Valor Produto ou Serviço': prod['vProd']['valor'],
                'Descrição Produto': prod['xProd']['valor']
            })
        return dados_cfe

    except Exception as e:
        logging.error(f"Erro ao processar saída: {e}")
        return None

# Extrair dados de um único arquivo XML
def processar_arquivo(arquivo):
    try:
        print(f'Processando arquivo {arquivo}')
        conteudo_xml = ler_arquivo_s3(bucket_name, arquivo)
        conteudo_corrigido = corrigir_cabecalho_xml(conteudo_xml)
        xml_file_like = io.StringIO(conteudo_corrigido)
        dados_xml, namespaces = extrair_dados_xml(xml_file_like)

        if not dados_xml:
            return []

        if ('nfeProc' in dados_xml) or ('NFe' in dados_xml):
            tipo = 'entrada'
            dados_nota = extrair_campos_entrada(dados_xml, namespaces)
        elif ('envCFe' in dados_xml) or ('CFe' in dados_xml):
            tipo = 'saida'
            dados_nota = extrair_campos_saida(dados_xml, namespaces)
        else:
            salvar_arquivo_problema_s3(bucket_name, arquivo, conteudo_corrigido)
            return []

        resultado = []
        if isinstance(dados_nota, list):
            for item in dados_nota:
                item['Tipo'] = tipo
                resultado.append(item)
        elif isinstance(dados_nota, dict):
            for item in dados_nota['itens']:
                item['Tipo'] = tipo
                item.update({k: dados_nota[k] for k in dados_nota if k != 'itens'})
                resultado.append(item)

        mover_arquivo_s3(bucket_name, arquivo, 'Cat42/Tateti/XMLs_Processados/')
        print(f'Arquivo {arquivo} processado e salvo em Cat42/Tateti/XMLs_Processados!')
        return resultado
    except Exception as e:
        logging.error(f"Erro ao processar arquivo {arquivo}: {e}")
        return []

# Função principal com paralelismo
def processar_xmls(bucket, pasta_origem, pasta_destino):
    arquivos_xml = listar_arquivos_s3(bucket, pasta_origem)
    print(f'Iniciando o processamento de {len(arquivos_xml)} arquivos!')

    resultados = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for resultado in executor.map(processar_arquivo, arquivos_xml):
            resultados.extend(resultado)

    if resultados:
        df = pd.DataFrame(resultados)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        buffer.seek(0)
        salvar_arquivo_s3(bucket, f"{pasta_destino}/tabela_1.xlsx", buffer)
        print(f"Arquivo Excel salvo com sucesso em {pasta_destino}/tabela_1.xlsx")
    else:
        print("Nenhum dado foi processado. Nenhum arquivo Excel será gerado.")

# Configurações do S3
pasta_origem = 'Cat42/Tateti/XMLs'
pasta_destino = 'Cat42/Tateti/Tabela 1'

# Processar XMLs
processar_xmls(bucket_name, pasta_origem, pasta_destino)