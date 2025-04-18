import psycopg2
import os
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

class PostgresConnection:
    """
    Classe para conexão e interação com banco de dados PostgreSQL.
    """
    
    def __init__(
            self, 
            user: str = os.getenv("POSTGRES_USER"), 
            host: str = os.getenv("POSTGRES_HOST"), 
            password: str = os.getenv("POSTGRES_PASSWORD"), 
            database: str = os.getenv("POSTGRES_DB"), 
            port: str = os.getenv("POSTGRES_PORT")
            ):
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

    def close(self):
        """
        Fecha cursor e conexão.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Conexão encerrada.")

    def create_schema(self, cursor, schema_name: str):
        """
        Cria um schema no banco de dados, se ainda não existir.

        Args:
            schema_name (str): nome do schema que será criado.
        """

        query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(schema_name)
        )

        try:
            cursor.execute(query)
            self.connection.commit()
            print(f"Schema '{schema_name}' criado (ou já existia).")

        except Exception as e:
            print(f"Erro ao criar o schema: {e}")
            self.connection.rollback()



    def create_table(self, cursor, schema: str, name: str, columns: dict):
        """
        Cria uma tabela no PostgreSQL dinamicamente.

        Args:
            cursor: cursor ativo da conexão com o banco.
            name (str): nome da tabela a ser criada.
            fields (dict): dicionário com nome da coluna como chave e tipo como valor.
                            Ex: {"id": "SERIAL PRIMARY KEY", "nome": "VARCHAR(100)", "idade": "INTEGER"}
        """

        # Normalmente, você deve expressar o modelo da sua consulta como uma SQLinstância 
        # com {} marcadores de posição no estilo - e usar format() para mesclar as partes 
        # variáveis ​​neles, todas as quais devem ser Composablesubclasses
        # exemplo: sql.SQL("select {columns} from {table}").format(columns=sql.identifier('col_name'))

        # Transforma os valores do dicionario em objetos sql
        formatted_columns = [
            sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(col_type))
            for name, col_type in columns.items()
        ]

        query = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
            sql.Identifier(schema),
            sql.Identifier(name),
            sql.SQL(', ').join(formatted_columns)
        )

        try:
            cursor.execute(query)
            print(f"Tabela {name} criada com sucesso")
            self.connection.commit()
            
        except Exception as e:
            print(f"Erro ao criar a tabela: {e}")
            self.connection.rollback()


if __name__ == "__main__":

    load_dotenv()

    conn = PostgresConnection()

    conn.connect_pg()

    columns = {
        "id": "SERIAL PRIMARY KEY",
        "dados": "JSONB",
    }

    conn.create_table(conn.cursor, "warehouse", "json_test", columns)
    conn.connection.commit()
    conn.cursor.close()
    conn.connection.close()


