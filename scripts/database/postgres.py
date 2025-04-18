import psycopg2
import os
from psycopg2 import sql
from dotenv import load_dotenv

class PostgresConnection:
    """
    Classe para conexão e interação com banco de dados PostgreSQL.
    """
    
    def __init__(self, user: str, host: str, password: str, database: str , port: str = '5432'):
        """ 
        Pega as variaveis necessarias para conexao com o banco do arquivo .env
        """

        
        self.user = user
        self.host = host
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        self.cursor = None

    def connect_pg(self):
        """"
        Estabele conexão com o banco de dados.
        
        Returns:
            String com versao do banco se conectado com sucesso ou erro caso falhe
        """

        try:
            self.connection = psycopg2.connect(
                dbname=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )

            self.cursor = self.connection.cursor()

            print("Conexão com o banco estabelecida com sucesso")
            self.cursor.execute("SELECT version();")
            version = self.cursor.fetchone()
            print(f"Versão do PostgreSQL: {version}")

        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {e}")


if __name__ == "__main__":
    load_dotenv()
    pg = PostgresConnection(
        user=os.getenv("POSTGRES_USER"),
        host=os.getenv("POSTGRES_HOST"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB")
    )
    pg.connect_pg()

