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

        # Lógica de Modo Grafana (agora para 1, 11, 22, 33...)
        if sensor_id in ["11", "22", "33", "44", "55", "66"]:
            datapoints = []
            for item in itens:
                try:
                    # --- INÍCIO DA CORREÇÃO DE DATA ---
                    # Esta parte agora trata datas com e sem segundos
                    valor = float(item["valor"])
                    data_captura_str = item["data_captura"]
                    
                    try:
                        # 1. Tenta converter o formato COM segundos (ex: "2025-10-29 16:45:00")
                        dt = datetime.strptime(data_captura_str, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        # 2. Se falhar, tenta converter o formato SEM segundos (ex: "2025-10-29 16:46")
                        dt = datetime.strptime(data_captura_str, "%Y-%m-%d %H:%M")
                    
                    timestamp = int(dt.timestamp() * 1000)
  
                    # Verifica se estamos tratando os sensores especiais (1 ou 11)
                    if sensor_id in ["1", "11"]:
                        # Cria a lista de datapoint com os dois valores base
                        datapoint_list = [valor, timestamp]
                        
                        # Adiciona as colunas extras do CSV
                        datapoint_list.append(item.get("alerta_sobrecarga", None))
                        datapoint_list.append(item.get("carga_media_trabalho_amps", None))
                        datapoint_list.append(item.get("confiabilidade_perc_oee", None))
                        datapoint_list.append(item.get("estado_operacional", None))
                        datapoint_list.append(item.get("fk_sensor", None))
                        datapoint_list.append(item.get("mtbf_minutos", None))
                        datapoint_list.append(item.get("mttr_minutos", None))
                        datapoint_list.append(item.get("perc_tempo_desligada", None))
                        datapoint_list.append(item.get("perc_tempo_em_carga", None))
                        datapoint_list.append(item.get("perc_tempo_ociosa", None))
                        datapoint_list.append(item.get("total_eventos_sobrecarga", None))
                        
                        datapoints.append(datapoint_list)
                        
                    else:
                        # Sensores 22, 33, 44, etc. continuam como antes
                        datapoints.append([valor, timestamp])
                 
                        
                except Exception as e:
                    # Se um item falhar na conversão (mesmo com a data corrigida),
                    # ele será logado e pulado, evitando que a Lambda quebre.
                    print(f"Erro ao processar item {item}: {e}")

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

        # Lógica de "Último Registro" (agora só para 2, 3, 4, 5, 6)
        else:
            ultimo_registro = max(itens, key=lambda x: x["data_captura"])

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "sensor_id": sensor_id,
                    "table": table_name,
                    "ultimo_registro": ultimo_registro
                }, default=decimal_default)
            }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }