import sys
import pandas as pd

sys.path.append("/opt/airflow")

from scripts.database.postgres import PostgresConnection

pg = PostgresConnection()
conn = pg.connect_pg()

query = """
    SELECT
        dados->>'VALOR' AS VALOR,
        (dados->>'AnoMes')::numeric AS ANOMES,
        dados->>'NATUREZA' AS NATUREZA,
        dados->'PAG_PFPJ' AS PAG_PFPJ,
        dados->'PAG_IDADE' AS PAG_IDADE,
        dados->'REC_IDADE' AS REC_IDADE,
        dados->'PAG_REGIAO' as PAG_REGIAO,
        dados->'QUANTIDADE' as QUANTIDADE,
        dados->'REC_REGIAO' as REC_REGIAO
    FROM warehouse.estatisticas_pix
    LIMIT 100;
"""

df = pd.read_sql_query(query, conn)

print(df.head())