import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

from datetime import datetime

#  carregar as variaveis do arquivo .env
load_dotenv()

def conectar_banco():
    return duckdb.connect(database='duckdb.db', read_only=False)

def inicializar_tabela(con):
    con.execute("""create table if not exists historico_arquivos 
                (   nome_arquivo          varchar, 
                    horario_processamento timestamp  )
            """)
    
def registrar_arquivo(con, nome_arquivo):
    con.execute("""insert into historico_arquivos (nome_arquivo, horario_processamento)
                   values (?, ?)
                """, (nome_arquivo, datetime.now()))

def arquivos_processados(con):
    return set(row[0] for row in con.execute("select nome_arquivo from historico_arquivos").fetchall())

# download all files
def baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local):
    # create pasta
    os.makedirs(diretorio_local, exist_ok=True)
    # download all files
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)

# list all files in pasta
def listar_arquivos_csv(diretorio):
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
    
    # baixar_pasta_google_drive(url_pasta, diretorio_local)
    arquivos_csv = listar_arquivos_csv(diretorio_local)
    con = conectar_banco()
    inicializar_tabela(con)
    processados = arquivos_processados(con)
    for caminho_do_arquivo in arquivos_csv:
        nome_arquivo = os.path.basename(caminho_do_arquivo)
        if nome_arquivo not in processados:
            df = ler_csv(caminho_do_arquivo)
            df_transformado = transformar(df)
            salvar_no_postgres(df_transformado, "vendas_calculado")
            registrar_arquivo(con, nome_arquivo)
            print(f"Arquivo {nome_arquivo} processado e salvo.")
        else:
            print(f"Arquivo {nome_arquivo} já foi processado anteriormente.")