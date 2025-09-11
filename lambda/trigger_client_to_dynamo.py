import boto3
import csv
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

        print(f"Processando arquivo: {source_key} (bucket: {source_bucket})")


        # Verifica se é um arquivo CSV
        if not source_key.endswith(".csv"):
            print(f"Ignorando arquivo não-CSV: {source_key}")
            return {"status": "ignorado", "arquivo": source_key}

        # Lê o arquivo CSV do bucket
        obj = s3.get_object(Bucket=source_bucket, Key=source_key)
        csv_content = obj["Body"].read().decode("utf-8-sig").splitlines()  # <- utf-8-sig remove BOM
        reader = csv.DictReader(csv_content)

        print(f"Colunas do CSV: {reader.fieldnames}")

        registros = 0
        for row in reader:
            # Ignora linhas vazias
            if not row:
                continue

            # Pega o fk_sensor de forma segura
            fk_sensor = row.get("fk_sensor")
            if not fk_sensor:
                print(f"Linha ignorada, fk_sensor ausente: {row}")
                continue

            fk_sensor = str(fk_sensor).strip()  # remove espaços

            # Verifica se o sensor tem tabela associada
            if fk_sensor not in TABLES:
                print(f"fk_sensor {fk_sensor} não tem tabela associada. Ignorado.")
                continue

            tabela_destino = TABLES[fk_sensor]
            tabela = dynamo.Table(tabela_destino)

            # Pega valor e data de forma segura
            valor = row.get("valor")
            data_captura = row.get("data_captura")

            if valor is None or data_captura is None:
                print(f"Linha ignorada, dados incompletos: {row}")
                continue

            # Monta o item para o DynamoDB
            item = {
                "data_captura": data_captura.strip(),
                "valor": Decimal(str(valor).strip())
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
