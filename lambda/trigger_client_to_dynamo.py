import boto3, csv
import urllib.parse
from decimal import Decimal

s3 = boto3.client("s3")

TABLES = {
    "1": "sensor-corrente",
    "2": "sensor-tensao",
    "3": "sensor-temperatura",
    "4": "sensor-vibracao",
    "5": "sensor-pressao",
    "6": "sensor-frequencia"
}

dynamo = boto3.resource("dynamodb")

def lambda_handler(event, context):
    try:
        # Extrai informações do evento S3
        source_bucket = event["Records"][0]["s3"]["bucket"]["name"]
        source_key = urllib.parse.unquote_plus(
            event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
        )

        # Verifica se é um arquivo CSV
        if not source_key.endswith(".csv"):
            print(f"Ignorando arquivo não-CSV: {source_key}")
            return {"status": "ignorado", "arquivo": source_key}

        # Lê o arquivo CSV do bucket
        obj = s3.get_object(Bucket=source_bucket, Key=source_key)
        csv_content = obj["Body"].read().decode("utf-8").splitlines()
        reader = csv.DictReader(csv_content)

        registros = 0
        for row in reader:
            fk_sensor = str(row["fk_sensor"])

            # Verifica se o sensor tem tabela associada
            if fk_sensor not in TABLES:
                print(f"fk_sensor {fk_sensor} não tem tabela associada. Ignorado.")
                continue

            tabela_destino = TABLES[fk_sensor]
            tabela = dynamo.Table(tabela_destino)

            # Monta o item 
            item = {
                "data_captura": row["data_captura"],
                "valor": Decimal(str(row["valor"]))
            }

            tabela.put_item(Item=item)
            registros += 1

        print(f"Processado {registros} registros do arquivo {source_key}")
        return {
            "status": "sucesso",
            "arquivo_processado": source_key,
            "registros_inseridos": registros,
        }

    except Exception as e:
        print(f"Erro ao processar arquivo {source_key}: {e}")
        raise e
