import json
import boto3
from decimal import Decimal
from datetime import datetime, timedelta
import statistics

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

# --- MELHORIA DO SEU AMIGO (VAMOS MANTER) ---
# Função limpa para tratar datas com e sem segundos
def parse_dt(data_str):
    if not data_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(data_str, fmt)
        except Exception:
            continue
    return None
# ---------------------------------------------

def lambda_handler(event, context):
    try:
        sensor_id = event.get('pathParameters', {}).get('sensor_id')
        if not sensor_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Parâmetro sensor_id é obrigatório"})
            }

        # --- NOVAS MÉTRICAS DO SEU AMIGO (MANTER) ---
        # Função auxiliar para ler vibração
        def get_vibration_data():
            table = dynamodb.Table("sensor-vibracao")
            response = table.scan()
            itens = response.get("Items", [])
            itens = [x for x in itens if parse_dt(x.get("data_captura"))]
            return sorted(itens, key=lambda x: parse_dt(x["data_captura"]))

        # 666: detectar último restart
        if sensor_id == "666":
            itens = get_vibration_data()
            if not itens:
                return {"statusCode": 404, "body": json.dumps({"error": "Nenhum dado encontrado"})}

            ultimo_restart, maior_gap = None, 0
            for i in range(1, len(itens)):
                dt_atual = parse_dt(itens[i]["data_captura"])
                dt_anterior = parse_dt(itens[i - 1]["data_captura"])
                if dt_atual and dt_anterior:
                    diff = (dt_atual - dt_anterior).total_seconds() / 60
                    if diff > 20:
                        ultimo_restart = dt_atual
                        maior_gap = diff

            if ultimo_restart:
                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "ultimo_restart": ultimo_restart.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "gap_minutos": round(maior_gap, 2)
                    })
                }
            return {
                "statusCode": 200,
                "body": json.dumps({"mensagem": "Nenhum salto de tempo > 20 min"})
            }

        # 6666: tempo total em operação/parada
        elif sensor_id == "6666":
            itens = get_vibration_data()
            if not itens:
                return {"statusCode": 404, "body": json.dumps({"error": "Sem dados"})}

            total_operando = 0
            total_parado = 0
            for i in range(1, len(itens)):
                atual = float(itens[i]["valor"])
                anterior = float(itens[i - 1]["valor"])
                dt_atual = parse_dt(itens[i]["data_captura"])
                dt_anterior = parse_dt(itens[i - 1]["data_captura"])
                if dt_atual and dt_anterior:
                    diff_min = (dt_atual - dt_anterior).total_seconds() / 60
                    if anterior != 0 or atual != 0:
                        total_operando += diff_min
                    else:
                        total_parado += diff_min

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "tempo_operando_min": round(total_operando, 2),
                    "tempo_parado_min": round(total_parado, 2)
                })
            }

        # 66666: quantidade de ciclos concluídos
        elif sensor_id == "66666":
            itens = get_vibration_data()
            if not itens:
                return {"statusCode": 404, "body": json.dumps({"error": "Sem dados"})}

            ciclos = 0
            ativo = False
            for item in itens:
                valor = float(item["valor"])
                if valor > 0 and not ativo:
                    ativo = True
                elif valor == 0 and ativo:
                    ativo = False
                    ciclos += 1

            return {"statusCode": 200, "body": json.dumps({"ciclos_concluidos": ciclos})}

        # 666666: média e pico de vibração do dia
        elif sensor_id == "666666":
            itens = get_vibration_data()
            hoje = datetime.now().date()
            valores_hoje = [
                float(x["valor"])
                for x in itens
                if parse_dt(x.get("data_captura")) and parse_dt(x.get("data_captura")).date() == hoje
            ]

            if not valores_hoje:
                return {"statusCode": 404, "body": json.dumps({"error": "Sem dados de hoje"})}

            media = sum(valores_hoje) / len(valores_hoje)
            pico = max(valores_hoje)
            return {
                "statusCode": 200,
                "body": json.dumps({"media_vibracao": round(media, 3), "pico_vibracao": pico})
            }

        # 6666666: desvio padrão da vibração
        elif sensor_id == "6666666":
            itens = get_vibration_data()
            valores = [float(x["valor"]) for x in itens]
            if len(valores) < 2:
                return {"statusCode": 200, "body": json.dumps({"desvio_padrao": 0})}

            desvio = statistics.stdev(valores)
            return {
                "statusCode": 200,
                "body": json.dumps({"desvio_padrao": round(desvio, 4)})
            }

        # 66666666: última hora de pico
        elif sensor_id == "66666666":
            itens = get_vibration_data()
            if not itens:
                return {"statusCode": 404, "body": json.dumps({"error": "Sem dados"})}

            max_item = max(itens, key=lambda x: float(x["valor"]))
            dt = parse_dt(max_item["data_captura"])
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "pico_vibracao": float(max_item["valor"]),
                    "hora_pico": dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None
                })
            }
        # --- FIM DAS NOVAS MÉTRICAS ---


        # --- NOSSA LÓGICA ANTIGA (AGORA COMO "CASO GERAL") ---
        table_info = SENSOR_TABLES.get(sensor_id)
        if not table_info:
            return {"statusCode": 400, "body": json.dumps({"error": "Sensor_id inválido"})}

        table_name, metric_name = table_info
        table = dynamodb.Table(table_name)
        response = table.scan()
        itens = response.get("Items", [])

        if not itens:
            return {"statusCode": 404, "body": json.dumps({"error": "Nenhum dado encontrado"})}

        # --- LÓGICA DO GRAFANA (NOSSA) ---
        if sensor_id in ["1", "11", "22", "33", "44", "55", "66"]:
            datapoints = []
            for item in itens:
                try:
                    valor = float(item["valor"])
                    
                    # Usando a função parse_dt melhorada
                    dt = parse_dt(item.get("data_captura"))
                    if not dt:
                        continue # Pula item se a data for inválida
                        
                    timestamp = int(dt.timestamp() * 1000)

                    # --- NOSSA LÓGICA "CIRÚRGICA" (RESTAURADA) ---
                    if sensor_id in ["1", "11"]:
                        datapoint_list = [valor, timestamp]
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
                        # Sensores 22, 33, etc.
                        datapoints.append([valor, timestamp])
                        
                except Exception as e:
                    print(f"Erro ao processar item {item}: {e}")

            datapoints.sort(key=lambda x: x[1])
            grafana_response = [{"target": metric_name, "datapoints": datapoints}]
            return {
                "statusCode": 200,
                "body": json.dumps(grafana_response, default=decimal_default)
            }

        # --- LÓGICA DE "ÚLTIMO REGISTRO" (NOSSA) ---
        else:
            ultimo_registro = max(itens, key=lambda x: parse_dt(x.get("data_captura")) or datetime.min)
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "sensor_id": sensor_id,
                    "table": table_name,
                    "ultimo_registro": ultimo_registro
                }, default=decimal_default)
            }

    except Exception as e:
        print(f"[ERRO] {e}") # Log de erro melhorado (do seu amigo)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}