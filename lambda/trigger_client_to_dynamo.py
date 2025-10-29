import boto3
import csv
import urllib.parse
from decimal import Decimal

s3 = boto3.client("s3")

TABLES = {
    "1": "sensor-corrente",     # <- Seu sensor especial
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
        csv_content = obj["Body"].read().decode("utf-8-sig").splitlines() # <- utf-8-sig remove BOM
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

            fk_sensor = str(fk_sensor).strip() # remove espaços

            # Verifica se o sensor tem tabela associada
            if fk_sensor not in TABLES:
                print(f"fk_sensor {fk_sensor} não tem tabela associada. Ignorado.")
                continue

            tabela_destino = TABLES[fk_sensor]
            tabela = dynamo.Table(tabela_destino)

            # Pega valor e data de forma segura (CRUCIAL PARA AMBOS OS CASOS)
            valor = row.get("valor")
            data_captura = row.get("data_captura")

            if valor is None or data_captura is None:
                print(f"Linha ignorada, dados incompletos: {row}")
                continue

            # --- [ INÍCIO DA LÓGICA MODIFICADA ] ---
            # Aqui começa a divisão do comportamento

            item = {} # Inicializa o item do DynamoDB

            if fk_sensor == "1":
                # MODO SENSOR 1 (Corrente): Pega TODAS as colunas do CSV
                # print(f"MODO SENSOR 1: Processando linha completa para {data_captura}")
                
                for key, value in row.items():
                    # Pula valores nulos ou vazios
                    if value is None or value == "":
                        continue

                    key_clean = key.strip()
                    value_clean = str(value).strip()

                    # Tenta converter para Decimal, se falhar, salva como string.
                    # Exceto para chaves que sabemos que SÃO strings.
                    if key_clean in ["data_captura", "fk_sensor", "estado_operacional"]:
                        item[key_clean] = value_clean
                    elif key_clean == 'alerta_sobrecarga':
                        item[key_clean] = (value_clean.lower() == 'true') # Salva como Booleano
                    else:
                        try:
                            # Converte todos os outros campos (valor, mtbf, etc.) para Decimal
                            item[key_clean] = Decimal(value_clean)
                        except Exception:
                            # Se falhar (ex: uma string inesperada), salva como string
                            item[key_clean] = value_clean

            else:
                # MODO OUTROS SENSORES (2-6): Comportamento antigo e preservado
                # Salva APENAS 'data_captura' e 'valor'
                
                item = {
                    "data_captura": data_captura.strip(),
                    "valor": Decimal(str(valor).strip())
                }
            
            # --- [ FIM DA LÓGICA MODIFICADA ] ---

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