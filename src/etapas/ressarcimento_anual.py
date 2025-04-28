import pandas as pd
import boto3
import warnings
from dotenv import load_dotenv
import os

from src.utils.ler_arquivos import ler_arquivo_para_dataframe, salvar_dataframe_no_s3

warnings.filterwarnings('ignore')
# Carregando variáveis de ambiente
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../', '.env'))
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

if nome_empresa.lower() == 'ladakh':
    cnpj = "07318052000150"
    
if nome_empresa.lower() == 'sonda':
    cnpj = "01937635001316"   

if nome_empresa.lower() == 'mensa':
    cnpj = "10290457000301"

if nome_empresa.lower() == 'casa mimosa':
    cnpj = "62978978000301"
   

try:
    ficha_3 = ler_arquivo_para_dataframe(bucket_name, f'Cat42/{nome_empresa.title()}/Ficha 3/ficha_3_{nome_empresa.title()}_{cnpj}.xlsx', file_type='xlsx')

except:
    ficha_3_p1 = ler_arquivo_para_dataframe(bucket_name, f'Cat42/{nome_empresa.title()}/Ficha 3/ficha_3_{nome_empresa.title()}_{cnpj}_p1.xlsx', file_type='xlsx')
    ficha_3_p2 = ler_arquivo_para_dataframe(bucket_name, f'Cat42/{nome_empresa.title()}/Ficha 3/ficha_3_{nome_empresa.title()}_{cnpj}_p2.xlsx', file_type='xlsx')
    ficha_3_p3 = ler_arquivo_para_dataframe(bucket_name, f'Cat42/{nome_empresa.title()}/Ficha 3/ficha_3_{nome_empresa.title()}_{cnpj}_p3.xlsx', file_type='xlsx')

    ficha_3 = pd.concat([ficha_3_p1, ficha_3_p2, ficha_3_p3])

ficha_3['DATA'] = pd.to_datetime(ficha_3['DATA'], format='%Y-%m-%d')
ficha_3['ANO'] = ficha_3['DATA'].dt.year

ressarc_por_ano = ficha_3.groupby('ANO')['VLR_RESSARCIMENTO'].sum()

salvar_dataframe_no_s3(ressarc_por_ano, bucket_name=bucket_name,s3_key=f'Cat42/{nome_empresa.title()}/Ficha 3/ressarcimento_por_ano_{nome_empresa.title()}_{cnpj}.xlsx',
                       file_type='xlsx')
