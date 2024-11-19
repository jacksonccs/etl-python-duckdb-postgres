import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

#  carregar as variaveis do arquivo .env
load_dotenv()

# download all files
def baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local):
    # create pasta
    os.makedirs(diretorio_local, exist_ok=True)
    # download all files
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)

# list all files in pasta
def listar_aquivos_csv(diretorio):
    arquivos_csv = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        if arquivo.endswith(".csv"):
            caminho_completo = os.path.join(diretorio, arquivo)
            arquivos_csv.append(caminho_completo)
    return arquivos_csv

# Ler um arquivo csv e retornar um dataframe duckdb
def ler_csv(caminho_do_arquivo):
    dataframe_duckdb = duckdb.read_csv(caminho_do_arquivo)
    ##print(dataframe_duckdb)
    ##print(type(dataframe_duckdb))
    return dataframe_duckdb

# Transformation 
def transformar(df: DuckDBPyRelation) -> DataFrame:
    df_transformado = duckdb.sql("select data_venda,valor,quantidade,cliente_id,categoria,(valor * quantidade) as total_vendas from df").df()
    ##print(df_transformado)
    return df_transformado

# Salva para converter o duckdb em pandas e salvar o dataframe no banco
def salvar_no_postgres(df_duckdb, tabela):
    DATABASE_URL = os.getenv("DATABASE_URL") # 
    engine = create_engine(DATABASE_URL)
    df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)

if __name__ == "__main__":
    url_pasta = 'https://drive.google.com/drive/folders/19flL9P8UV9aSu4iQtM6Ymv-77VtFcECP'
    diretorio_local = './pasta_gdown'
    
    ##baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local)
    lista_arquivos = listar_aquivos_csv(diretorio_local)

    for caminho_do_arquivo in lista_arquivos:
        duck_db_df = ler_csv(caminho_do_arquivo)
        pandas_df_transformado = transformar(duck_db_df)
        salvar_no_postgres(pandas_df_transformado, "vendas_calculado")