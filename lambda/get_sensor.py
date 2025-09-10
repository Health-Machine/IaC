import json
import boto3
from decimal import Decimal
from datetime import datetime

dynamodb = boto3.resource('dynamodb')

# Mapeamento de sensor_id para tabela e nome legível
SENSOR_TABLES = {
    "1": ("sensor-corrente", "corrente"),
    "2": ("sensor-frequencia", "frequencia"),
    "3": ("sensor-pressao", "pressao"),
    "4": ("sensor-temperatura", "temperatura"),
    "5": ("sensor-tensao", "tensao"),
    "6": ("sensor-vibracao", "vibracao"),
    # IDs para retorno de TODOS os dados (grafana timeseries)
    "11": ("sensor-corrente", "corrente"),
    "22": ("sensor-frequencia", "frequencia"),
    "33": ("sensor-pressao", "pressao"),
    "44": ("sensor-temperatura", "temperatura"),
    "55": ("sensor-tensao", "tensao"),
    "66": ("sensor-vibracao", "vibracao")
}

# Converte Decimal -> float
def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    try:
        sensor_id = event.get('pathParameters', {}).get('sensor_id')
        
        if not sensor_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Parâmetro sensor_id é obrigatório"})
            }

        table_info = SENSOR_TABLES.get(sensor_id)
        if not table_info:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": f"Sensor_id {sensor_id} inválido"})
            }

        table_name, metric_name = table_info
        table = dynamodb.Table(table_name)

        # Busca todos os registros
        response = table.scan()
        itens = response.get("Items", [])

        if not itens:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "Nenhum dado encontrado para esse sensor"})
            }

        # Caso IDs 11,22,33... → retorna todos os registros p/ gráfico
        if sensor_id in ["11", "22", "33", "44", "55", "66"]:
            datapoints = []
            for item in itens:
                try:
                    valor = float(item["valor"])
                    timestamp = int(datetime.strptime(item["data_captura"], "%Y-%m-%d %H:%M").timestamp() * 1000)
                    datapoints.append([valor, timestamp])
                except Exception as e:
                    print(f"Erro ao converter item {item}: {e}")

            # Ordena pelo tempo
            datapoints.sort(key=lambda x: x[1])

            grafana_response = [
                {
                    "target": metric_name,
                    "datapoints": datapoints
                }
            ]

            return {
                "statusCode": 200,
                "body": json.dumps(grafana_response, default=decimal_default)
            }

        # Caso IDs 1–6 → retorna só o último registro
        else:
            ultimo_registro = max(itens, key=lambda x: x["data_captura"])

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "sensor_id": sensor_id,
                    "table": table_name,
                    "data_time": ultimo_registro
                }, default=decimal_default)
            }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
