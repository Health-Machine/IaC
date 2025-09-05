import boto3, csv, uuid
import urllib.parse
from decimal import Decimal

TABLE_NAME = "sensores-hm"  
dynamo = boto3.resource("dynamodb").Table(TABLE_NAME)
s3 = boto3.client("s3")

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

        # Lê o arquivo CSV do bucket client
        obj = s3.get_object(Bucket=source_bucket, Key=source_key)
        csv_content = obj["Body"].read().decode("utf-8").splitlines()
        reader = csv.DictReader(csv_content)

        registros = 0
        for row in reader:
            item = {
                "fk_sensor": Decimal(str(row["fk_sensor"])),
                "valor": Decimal(str(row["valor"])),
                "data_captura": row["data_captura"],
            }
            dynamo.put_item(Item=item)
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
