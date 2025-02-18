import boto3
from botocore.exceptions import NoCredentialsError
import pandas as pd
from io import BytesIO
import os

# Variáveis para acesso ao s3
bucket_name = '4btaxtech'

s3 = boto3.client('s3', 
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), 
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                  region_name=os.getenv('AWS_DEFAULT_REGION')
                  )

def ler_arquivo_para_dataframe(bucket_name, s3_key, file_type='csv'):
    """
    Lê um arquivo do S3 e carrega em um DataFrame do Pandas.
    
    :param bucket_name: Nome do bucket.
    :param s3_key: Caminho do arquivo no S3.
    :param file_type: Tipo do arquivo ('csv' ou 'xlsx').
    :return: DataFrame do Pandas.
    """
    try:
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        if file_type == 'csv':
            df = pd.read_csv(BytesIO(response['Body'].read()), dtype=str, header=0, sep=';')
        elif file_type == 'xlsx':
            df = pd.read_excel(BytesIO(response['Body'].read()))
        else:
            raise ValueError("Tipo de arquivo não suportado. Use 'csv' ou 'xlsx'.")
        print(f"Arquivo '{s3_key}' lido com sucesso!")
        return df
    except Exception as e:
        print(f"Erro ao ler arquivo do S3: {e}")
        return None

def salvar_dataframe_no_s3(df, bucket_name, s3_key, file_type='csv'):
    """
    Salva um DataFrame do Pandas no S3.
    
    :param df: DataFrame a ser salvo.
    :param bucket_name: Nome do bucket.
    :param s3_key: Caminho para salvar o arquivo no S3.
    :param file_type: Tipo do arquivo ('csv' ou 'xlsx').
    """
    try:
        buffer = BytesIO()
        if file_type == 'csv':
            df.to_csv(buffer, index=False, encoding='utf-8')
        elif file_type == 'xlsx':
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False)
        else:
            raise ValueError("Tipo de arquivo não suportado. Use 'csv' ou 'xlsx'.")
        
        buffer.seek(0)  # Resetar o ponteiro do buffer
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())
        print(f"Arquivo salvo com sucesso em '{s3_key}'.")
    except Exception as e:
        print(f"Erro ao salvar arquivo no S3: {e}")