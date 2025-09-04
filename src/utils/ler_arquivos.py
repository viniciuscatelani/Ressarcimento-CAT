import boto3
from botocore.exceptions import NoCredentialsError
import pandas as pd
from io import BytesIO
import os

# No início do módulo
bucket_name = '4btaxtech'


def ler_arquivo_para_dataframe(bucket_name, s3_key, file_type='csv', sep=None):
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
            df = pd.read_csv(
                BytesIO(response['Body'].read()), dtype=str, header=0, sep=sep, encoding='utf-8')
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
