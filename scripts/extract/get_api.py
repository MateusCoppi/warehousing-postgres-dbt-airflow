import os
import json
import requests

DATA_DIR = '../../data'
DATE_API = '202412'

def extract_api(data_dir: str, date: str):
    '''
    Extrai dados no formato json referente a transações pix da api do bacen 
    data_dir: diretorio onde serao salvos os dados
    date: data base para extraaco dos dados
    '''
    
    url = f"https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata/EstatisticasTransacoesPix(Database=@Database)?@Database='{date}'&$format=json&$select=AnoMes,PAG_PFPJ,REC_PFPJ,PAG_REGIAO,REC_REGIAO,PAG_IDADE,REC_IDADE,NATUREZA,VALOR,QUANTIDADE"
    r = requests.get(url=url)

    if r.status_code == 200:
        data = r.json()
        print(f"Dados extraídos com sucesso: {r.status_code}")
    else:
        print(f"Erro: {r.status_code}")

    with open (f'{data_dir}/dados_{date}.json', 'w') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    if os.path.isfile(f"{data_dir}/dados_{date}.json"):
        print("Dados extraídos com sucesso")
    else:
        print("Dados não encontrados")


if __name__ == "__main__":
    extract_api(data_dir=DATA_DIR, date=DATE_API)