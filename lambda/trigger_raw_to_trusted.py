import boto3
import json
import urllib.parse
import csv
import io
from datetime import datetime

s3 = boto3.client('s3')
TRUSTED_BUCKET = 'trusted-bucket-891377383993'

def lambda_handler(event, context):
    for record in event['Records']:
        source_bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        print(f"Processando arquivo: s3://{source_bucket}/{key}")

        try:
            # Lê o JSON do bucket de origem
            response = s3.get_object(Bucket=source_bucket, Key=key)
            raw_bytes = response['Body'].read()

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
                fk = row.get("fk_sensor")
                valor = row.get("valor")
                data_captura = row.get("data_captura")
                if fk is not None and valor is not None and data_captura and valor != 0:
                    try:
                        data_dt = datetime.strptime(data_captura, "%Y-%m-%d %H:%M:%S")
                        data_str = data_dt.strftime("%Y-%m-%d %H:%M")
                    except ValueError:
                        print(f"Formato de data inválido em {data_captura}, arquivo: {key}")
                        continue

                    valid_rows.append({
                        "fk_sensor": fk,
                        "valor": valor,
                        "data_captura": data_str
                    })

            if not valid_rows:
                print(f"Nenhum dado válido encontrado no arquivo {key}.")
                continue

            # Cria o CSV em memória
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=["fk_sensor", "valor", "data_captura"])
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
            print(f"Erro ao processar o arquivo {key} do bucket {source_bucket}: {e}")
