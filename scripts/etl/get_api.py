import sys
import requests
import pandas as pd

sys.path.append("/home/mateus/workflow/pipeline_project")

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

        df = pd.DataFrame(registros)

        conn = PostgresConnection()
        conn.connect_pg()
        cursor = conn.pg_cursor
        engine = conn.create_engine()

        try:
            if conn.connection is None:
                raise ConnectionError("Falha ao estalecer conexão com o banco")
        except ConnectionError as e:
            print(e)


        DB_SCHEMA = "warehouse"
        TABLE_NAME = "estatisticas_pix"
        COLUMNS = {
                    "id": "SERIAL PRIMARY KEY",
                    "AnoMes": "integer",
                    "PAG_PFPJ": "text",
                    "REC_PFPJ": "text",
                    "PAG_REGIAO": "text",
                    "REC_REGIAO": "text",
                    "PAG_IDADE": "text",
                    "REC_IDADE": "text",
                    "NATUREZA": "text",
                    "VALOR": "double precision",
                    "QUANTIDADE": "integer"
                   }

        conn.create_schema(cursor, DB_SCHEMA)
        conn.create_table(cursor, DB_SCHEMA, TABLE_NAME, COLUMNS)
        conn.connection.commit()

        df.to_sql(name=TABLE_NAME,  con=engine, schema=DB_SCHEMA, if_exists="append", index=False)

        print(f"{df.shape} registros inseridos com sucesso no banco.")

    else:
        print(f"Erro ao buscar os dados: {r.status_code}")

    conn.close()

if __name__ == "__main__":
    DATE_API = '202412'
    extract_api(date=DATE_API)