import os
import sys
import requests
from dotenv import load_dotenv
from psycopg2.extras import Json

load_dotenv()
root_dir = os.getenv('ROOT_DIR')
sys.path.append(root_dir)

from scripts.database.postgres import PostgresConnection

def extract_api(date: str):
    '''
    Extrai dados no formato json referente a transações pix da api do bacen.
    
    date: data base para extraaco dos dados
    '''
    
    url = f"https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata/EstatisticasTransacoesPix(Database=@Database)?@Database='{date}'&$format=json&$select=AnoMes,PAG_PFPJ,REC_PFPJ,PAG_REGIAO,REC_REGIAO,PAG_IDADE,REC_IDADE,NATUREZA,VALOR,QUANTIDADE"
    r = requests.get(url=url)

    if r.status_code == 200:
        data = r.json()
        print(f"Dados extraídos com sucesso: {r.status_code}")

        registros = data.get("value", [])

        conn = PostgresConnection()
        conn.connect_pg()

        if conn.connection is None:
            print("Falha ao estalecer conexão com o banco")
            return

        DB_SCHEMA = "warehouse"
        TABLE_NAME = "estatisticas_pix"
        COLUMNS = {"id": "SERIAL PRIMARY KEY",
                   "dados": "JSONB"
                   }

        conn.create_schema(conn.cursor, DB_SCHEMA)
        conn.create_table(conn.cursor, DB_SCHEMA, TABLE_NAME, COLUMNS)
        conn.connection.commit()

        # insere os dados no campo (dados) através do placeholder (%s)
        # (%s) placeholder para o valor que será passadoo em seguida
        # json(registros) converte o dict python para o formato json compativel com o postgres
        with conn.connection.cursor() as cursor:
            for data in registros:
                cursor.execute(
                    "INSERT INTO warehouse.estatisticas_pix (dados) VALUES (%s)",
                    [Json(data)]
                )
        conn.connection.commit()
        print(f"{len(registros)} registros inseridos com sucesso no banco.")



    else:
        print(f"Erro ao buscar os dados: {r.status_code}")

    cursor.close()

if __name__ == "__main__":
    DATE_API = '202412'
    extract_api(date=DATE_API)