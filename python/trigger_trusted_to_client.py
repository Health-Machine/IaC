import boto3
import urllib.parse

s3 = boto3.client('s3')
CLIENT_BUCKET = 'client-bucket-381492149341'

def lambda_handler(event, context):
    try:
        # Extrai informações do evento S3
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        source_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        
        # Verifica se é um arquivo CSV
        if not source_key.endswith('.csv'):
            print(f"Ignorando arquivo não-CSV: {source_key}")
            return {"status": "ignorado", "arquivo": source_key}

        # Copia o arquivo para o bucket de destino
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        s3.copy_object(
            Bucket=CLIENT_BUCKET,
            Key=source_key,
            CopySource=copy_source
        )

        print(f"Arquivo {source_key} copiado de {source_bucket} para {CLIENT_BUCKET}")
        return {
            "status": "sucesso",
            "arquivo_copiado": source_key
        }

    except Exception as e:
        print(f"Erro ao copiar o arquivo: {e}")
        raise e
