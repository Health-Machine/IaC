import boto3
import json
import urllib.parse
import csv
import io
from datetime import datetime

s3 = boto3.client('s3')
TRUSTED_BUCKET = 'trusted-bucket-891377383993'
CLIENT_BUCKET = 'client-bucket-891377383993'

def lambda_handler(event, context):
    for record in event['Records']:
        source_bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        print(f"Processando arquivo: s3://{source_bucket}/{key}")

        try:
            raw_to_trusted(source_bucket, key)
            trusted_to_client(key)
        except Exception as e:
            print(f"function=lambda_handler_error file={key} message={e}")


def raw_to_trusted(source_bucket, key):
    try:
        # Lê o JSON do bucket de origem
        response = s3.get_object(Bucket=source_bucket, Key=key)
        raw_bytes = response['Body'].read()

        # Tenta decodificar o conteúdo
        try:
            content = raw_bytes.decode('utf-8')
        except UnicodeDecodeError:
            content = raw_bytes.decode('latin1')

        # Converte o conteúdo para JSON
        data = json.loads(content)
        if isinstance(data, dict):
            data = [data]

        valid_rows = []
        for row in data:
            if not row:
                continue

            # Captura campos
            sensor_1 = row.get("sensor_1")
            sensor_2 = row.get("sensor_2")
            sensor_3 = row.get("sensor_3")
            sensor_4 = row.get("sensor_4")
            sensor_5 = row.get("sensor_5")
            sensor_6 = row.get("sensor_6")
            data_captura = row.get("data_captura")

            # Validação básica
            if data_captura:
                try:
                    data_dt = datetime.strptime(data_captura, "%Y-%m-%d %H:%M:%S")
                    data_str = data_dt.strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    print(f"Formato de data inválido em {data_captura}, arquivo: {key}")
                    continue

                valid_rows.append({
                    "sensor_1": sensor_1,
                    "sensor_2": sensor_2,
                    "sensor_3": sensor_3,
                    "sensor_4": sensor_4,
                    "sensor_5": sensor_5,
                    "sensor_6": sensor_6,
                    "data_captura": data_str
                })

        if not valid_rows:
            print(f"Nenhum dado válido encontrado no arquivo {key}.")
            return

        # Cria o CSV em memória
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=[
            "sensor_1",
            "sensor_2",
            "sensor_3",
            "sensor_4",
            "sensor_5",
            "sensor_6",
            "data_captura"
        ])
        writer.writeheader()
        writer.writerows(valid_rows)
        csv_data = output.getvalue().encode('utf-8')
        output.close()

        # Define o nome do arquivo no trusted-bucket
        csv_key = key.replace('.json', '.csv')

        # Salva o CSV no bucket confiável
        s3.put_object(
            Bucket=TRUSTED_BUCKET,
            Key=csv_key,
            Body=csv_data,
            ContentType='text/csv'
        )

        print(f"CSV salvo no bucket '{TRUSTED_BUCKET}' como '{csv_key}'")

    except Exception as e:
        print(f"function=raw_to_trusted_error file={key} message={e}")

def trusted_to_client(key):

