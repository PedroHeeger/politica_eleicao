# CONEXÃO SQL ALCHEMY
# Conexão criada para inserir dados no banco MySQL com o SQLAlchemy

from sqlalchemy import (create_engine, MetaData, Column, Table, Integer, String, DateTime)
from dotenv import load_dotenv
import pymysql
import urllib.parse
import os

# Carregando as variáveis de ambiente
load_dotenv()

# Buscando cada variável
user = os.environ['MYSQL_USER_ROOT']
passw = os.environ['MYSQL_ROOT_PASSWORD']
host = os.environ['MYSQL_HOST']
port = os.environ['MYSQL_PORT']
db = os.environ['MYSQL_DATABASE']

# Formatando as variáveis
user = urllib.parse.quote_plus(user)
passw = urllib.parse.quote_plus(passw)
host = urllib.parse.quote_plus(host)
port = urllib.parse.quote_plus(port)
db = urllib.parse.quote(db)


# Criando a URL conexão com o SQLAlchemy Inicial (Sem Banco)
engine_inicial = create_engine(f"mysql+pymysql://{user}:{passw}@{host}:{port}", echo=True, encoding='UTF-8', pool_recycle=3600)

# Criando a conexão (Sem Banco)
conection_inicial = engine_inicial.connect()



# Criando a URL conexão com o SQLAlchemy (Com Banco)
engine = create_engine(f"mysql+pymysql://{user}:{passw}@{host}:{port}/{db}", echo=True, encoding='UTF-8', pool_recycle=3600)

# Criando a conexão (Com Banco)
conection = engine.connect()