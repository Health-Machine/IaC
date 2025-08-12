import boto3
import json
import urllib.parse
import csv
import io
from datetime import datetime

s3 = boto3.client('s3')
TRUSTED_BUCKET = 'trusted-bucket-381492149341'

def lambda_handler(event, context):
    # Buckets
    source_bucket = event['Records'][0]['s3']['bucket']['name']

    # Nome do arquivo recebido
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        # Lê o JSON do bucket de origem
        response = s3.get_object(Bucket=source_bucket, Key=key)
        raw_bytes = response['Body'].read()
        
        # Tenta decodificar com UTF-8, se falhar tenta Latin-1
        try:
            content = raw_bytes.decode('utf-8')
            print("Arquivo decodificado com UTF-8")
        except UnicodeDecodeError:
            content = raw_bytes.decode('latin1')
            print("Arquivo decodificado com Latin-1")

        # Converte o conteúdo para JSON
        data = json.loads(content)

        # Garante que o JSON está em formato de lista
        if isinstance(data, dict):
            data = [data]
        
        # Validação e filtragem dos dados
        valid_rows = []
        for row in data:
            if not row:
                continue
            fk = row.get("fk_sensor")
            valor = row.get("valor")
            data_captura = row.get("data_captura")
            if fk and valor and data_captura and valor != 0:
                data = datetime.strptime(data_captura, "%d/%m/%Y %H:%M")
                data = data.strftime("%Y-%m-%d %H:%M")

                valid_rows.append({
                    "fk_sensor": fk,
                    "valor": valor,
                    "data_captura": data
                })

        # Se não houver linhas válidas, encerra
        if not valid_rows:
            print("Nenhum dado válido encontrado.")
            return {"status": "sem dados válidos"}

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
        return {
            "status": "sucesso",
            "arquivo_csv": f"s3://{TRUSTED_BUCKET}/{csv_key}"
        }

    except Exception as e:
        print(e)
        print(f"Erro ao processar o arquivo {key} do bucket {source_bucket}")
        raise e
