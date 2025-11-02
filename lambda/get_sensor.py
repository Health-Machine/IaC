import json
import boto3
from decimal import Decimal
from datetime import datetime
import statistics

dynamodb = boto3.resource("dynamodb")

# Mapeamento das tabelas
SENSOR_TABLES = {
    "1": ("sensor-corrente", "corrente"),
    "2": ("sensor-frequencia", "frequencia"),
    "3": ("sensor-pressao", "pressao"),
    "4": ("sensor-temperatura", "temperatura"),
    "5": ("sensor-tensao", "tensao"),
    "6": ("sensor-vibracao", "vibracao"),
    # Timeseries completas
    "11": ("sensor-corrente", "corrente"),
    "22": ("sensor-frequencia", "frequencia"),
    "33": ("sensor-pressao", "pressao"),
    "44": ("sensor-temperatura", "temperatura"),
    "55": ("sensor-tensao", "tensao"),
    "66": ("sensor-vibracao", "vibracao"),
}

# Campos completos do sensor 11
FULL_DP_FIELDS = [
    "valor",
    "data_captura",
    "alerta_sobrecarga",
    "carga_media_trabalho_amps",
    "confiabilidade_perc_oee",
    "estado_operacional",
    "fk_sensor",
    "mtbf_minutos",
    "mttr_minutos",
    "perc_tempo_desligada",
    "perc_tempo_em_carga",
    "perc_tempo_ociosa",
    "total_eventos_sobrecarga"
]

# Endpoints de métricas derivadas
METRIC_ENDPOINTS = {
    "12": ("confiabilidade_perc_oee", "OEE"),
    "13": ("mtbf_minutos", "MTBF"),
    "14": ("mttr_minutos", "MTTR"),
    "15": ("perc_tempo_ociosa", "OCIOSA_PERCENT"),
    "16": ("perc_tempo_desligada", "PARADA_PERCENT"),
    "17": ("total_eventos_sobrecarga", "EVENTOS_SOBRECARGA"),
    "18": ("estado_operacional", "ESTADO_OPERACIONAL"),
}

# Mapeamento do estado para numérico
ESTADO_MAP = {"Em Carga": 1.0, "Ociosa": 0.5, "Parada": 0.0}


def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def try_parse_datetime_str(s):
    """Converte string em datetime"""
    if not isinstance(s, str):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    try:
        ival = int(s)
        if ival > 10**12:
            return datetime.fromtimestamp(ival / 1000.0)
    except Exception:
        pass
    return None


def to_epoch_ms(raw):
    """Garante timestamp em milissegundos"""
    if raw is None:
        return None
    if isinstance(raw, Decimal):
        raw = float(raw)
    if isinstance(raw, (int, float)):
        if raw > 10**12:
            return int(raw)
        if raw > 10**9:
            return int(raw * 1000)
        return int(raw * 1000)
    if isinstance(raw, str):
        if raw.isdigit():
            ival = int(raw)
            if ival > 10**12:
                return ival
            if ival > 10**9:
                return int(ival * 1000)
        dt = try_parse_datetime_str(raw)
        if dt:
            return int(dt.timestamp() * 1000)
    return None


def build_full_datapoint(item):
    """Cria lista completa (para sensor 11)"""
    dp = []
    for f in FULL_DP_FIELDS:
        if f == "data_captura":
            dp.append(to_epoch_ms(item.get(f)))
        else:
            v = item.get(f)
            if isinstance(v, Decimal):
                v = float(v)
            dp.append(v)
    return dp


def lambda_handler(event, context):
    try:
        sensor_id = event.get("pathParameters", {}).get("sensor_id")
        if not sensor_id:
            return {"statusCode": 400, "body": json.dumps({"error": "Parâmetro sensor_id é obrigatório"})}

        # === Função auxiliar ===
        def get_vibration_data():
            table = dynamodb.Table("sensor-vibracao")
            response = table.scan()
            itens = response.get("Items", [])
            itens = [x for x in itens if to_epoch_ms(x.get("data_captura")) is not None]
            return sorted(itens, key=lambda x: to_epoch_ms(x["data_captura"]))

        # === Endpoints especiais ===
        if sensor_id == "666":
            itens = get_vibration_data()
            if not itens:
                return {"statusCode": 404, "body": json.dumps({"error": "Nenhum dado encontrado"})}
            ultimo_restart, maior_gap = None, 0
            for i in range(1, len(itens)):
                dt_atual = to_epoch_ms(itens[i]["data_captura"])
                dt_anterior = to_epoch_ms(itens[i - 1]["data_captura"])
                if dt_atual and dt_anterior:
                    diff = (dt_atual - dt_anterior) / 1000.0 / 60.0
                    if diff > 20:
                        ultimo_restart = dt_atual
                        maior_gap = diff
            if ultimo_restart:
                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "ultimo_restart": datetime.utcfromtimestamp(ultimo_restart / 1000).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "gap_minutos": round(maior_gap, 2)
                    })
                }
            return {"statusCode": 200, "body": json.dumps({"mensagem": "Nenhum salto > 20 min"})}

        # === Endpoints derivados (12–18) ===
        if sensor_id in METRIC_ENDPOINTS:
            campo, alias = METRIC_ENDPOINTS[sensor_id]
            table_name, _ = SENSOR_TABLES["11"]
            table = dynamodb.Table(table_name)
            response = table.scan()
            itens = response.get("Items", [])
            datapoints = []
            for item in itens:
                ts = to_epoch_ms(item.get("data_captura"))
                if ts is None:
                    continue
                if campo == "estado_operacional":
                    raw = item.get(campo)
                    if raw is None:
                        continue
                    val = ESTADO_MAP.get(str(raw), None)
                    if val is None:
                        continue
                else:
                    val = item.get(campo)
                    if val is None:
                        continue
                    val = float(val)
                datapoints.append([val, ts])
            datapoints.sort(key=lambda x: x[1])
            return {
                "statusCode": 200,
                "body": json.dumps([{"target": alias, "datapoints": datapoints}], default=decimal_default)
            }

        # === Endpoint 19: corrente apenas em carga ===
        if sensor_id == "19":
            table_name, _ = SENSOR_TABLES["11"]
            table = dynamodb.Table(table_name)
            response = table.scan()
            itens = response.get("Items", [])
            datapoints = []
            for item in itens:
                estado = str(item.get("estado_operacional", "")).strip()
                if estado != "Em Carga":
                    continue
                valor = item.get("valor")
                ts = to_epoch_ms(item.get("data_captura"))
                if valor is None or ts is None:
                    continue
                datapoints.append([float(valor), ts])
            datapoints.sort(key=lambda x: x[1])
            return {
                "statusCode": 200,
                "body": json.dumps([{"target": "corrente_em_carga", "datapoints": datapoints}], default=decimal_default)
            }

        # === Sensor 11: completo ===
        if sensor_id == "11":
            table_name, metric_name = SENSOR_TABLES["11"]
            table = dynamodb.Table(table_name)
            response = table.scan()
            itens = response.get("Items", [])
            datapoints = []
            for item in itens:
                if (
                    item.get("confiabilidade_perc_oee") is None
                    or item.get("mtbf_minutos") is None
                    or item.get("mttr_minutos") is None
                ):
                    continue
                dp = build_full_datapoint(item)
                if dp[1] is None:
                    continue
                datapoints.append(dp)
            datapoints.sort(key=lambda x: x[1])
            return {
                "statusCode": 200,
                "body": json.dumps([{"target": metric_name, "datapoints": datapoints}], default=decimal_default)
            }

        # === Sensores simples (1–6): último registro ===
        if sensor_id in ["1", "2", "3", "4", "5", "6"]:
            table_name, metric_name = SENSOR_TABLES[sensor_id]
            table = dynamodb.Table(table_name)
            response = table.scan()
            itens = response.get("Items", [])
            ultima = max(itens, key=lambda x: (to_epoch_ms(x.get("data_captura")) or 0))
            val = float(ultima.get("valor", 0))
            ts = to_epoch_ms(ultima.get("data_captura"))
            return {
                "statusCode": 200,
                "body": json.dumps({"target": metric_name, "datapoints": [[val, ts]]}, default=decimal_default)
            }

        # === Sensores timeseries básicos (22–66) ===
        if sensor_id in ["22", "33", "44", "55", "66"]:
            table_name, metric_name = SENSOR_TABLES[sensor_id]
            table = dynamodb.Table(table_name)
            response = table.scan()
            itens = response.get("Items", [])
            datapoints = []
            for item in itens:
                valor = item.get("valor")
                ts = to_epoch_ms(item.get("data_captura"))
                if valor is None or ts is None:
                    continue
                datapoints.append([float(valor), ts])
            datapoints.sort(key=lambda x: x[1])
            return {
                "statusCode": 200,
                "body": json.dumps(
                    [{"target": metric_name, "datapoints": datapoints}],
                    default=decimal_default
                )
            }

        return {"statusCode": 400, "body": json.dumps({"error": "Sensor_id inválido"})}

    except Exception as e:
        print(f"[ERRO LAMBDA] {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
