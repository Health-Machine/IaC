import json
import boto3
from decimal import Decimal
from datetime import datetime
import statistics

dynamodb = boto3.resource("dynamodb")

# Tabelas / nome legível
SENSOR_TABLES = {
    "1": ("sensor-corrente", "corrente"),
    "2": ("sensor-frequencia", "frequencia"),
    "3": ("sensor-pressao", "pressao"),
    "4": ("sensor-temperatura", "temperatura"),
    "5": ("sensor-tensao", "tensao"),
    "6": ("sensor-vibracao", "vibracao"),
    # ids para timeseries "completo" (mantém o comportamento antigo do 11)
    "11": ("sensor-corrente", "corrente"),
    "22": ("sensor-frequencia", "frequencia"),
    "33": ("sensor-pressao", "pressao"),
    "44": ("sensor-temperatura", "temperatura"),
    "55": ("sensor-tensao", "tensao"),
    "66": ("sensor-vibracao", "vibracao"),
}

# Campos que compõem o datapoint "completo" (ordem usada no JSON grande)
FULL_DP_FIELDS = [
    "valor",                     # index 0
    "data_captura",              # index 1 (timestamp)
    "alerta_sobrecarga",        # index 2
    "carga_media_trabalho_amps",# index 3
    "confiabilidade_perc_oee",  # index 4
    "estado_operacional",       # index 5
    "fk_sensor",                # index 6
    "mtbf_minutos",             # index 7
    "mttr_minutos",             # index 8
    "perc_tempo_desligada",     # index 9
    "perc_tempo_em_carga",      # index 10
    "perc_tempo_ociosa",        # index a
    "total_eventos_sobrecarga"  # index 12
]

# Mapeamento de novos endpoints métricos (id -> campo do item a retornar)
# 11 fica para o FULL (completo). A partir de 12 retornamos métricas separadas.
METRIC_ENDPOINTS = {
    "12": ("confiabilidade_perc_oee", "OEE"),    # index 4
    "13": ("mtbf_minutos", "MTBF"),              # index 7
    "14": ("mttr_minutos", "MTTR"),              # index 8
    "15": ("perc_tempo_ociosa", "OCIOSA_PERCENT"),   # index 11
    "16": ("perc_tempo_desligada", "PARADA_PERCENT"),# index 9
    "17": ("total_eventos_sobrecarga", "EVENTOS_SOBRECARGA"), # index 12
    "18": ("estado_operacional", "ESTADO_OPERACIONAL"), # será mapeado para numérico
    # você pode adicionar mais aqui (19, 20...) se quiser separar outras colunas
}

# Mapeamento simples de estado_operacional -> número (se quiser alterar, ajuste aqui)
ESTADO_MAP = {
    "Em Carga": 1.0,
    "Parada": 0.0,
    "Ociosa": 0.5
}

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def try_parse_datetime_str(s):
    """Tenta converter string em datetime usando alguns formatos conhecidos."""
    if not isinstance(s, str):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    # tenta interpretar como inteiro string (epoch ms)
    try:
        ival = int(s)
        # se valor > 10**12 provavelmente é ms, converter para dt
        if ival > 10**12:
            return datetime.fromtimestamp(ival / 1000.0)
    except Exception:
        pass
    return None

def to_epoch_ms(raw):
    """Retorna epoch ms inteiro de uma entrada que pode ser:
       - int/float epoch ms
       - string timestamp (yyyy-mm-dd HH:MM[:SS]) ou string com epoch ms
    """
    if raw is None:
        return None
    # se for Decimal (Dynamo) converte
    if isinstance(raw, Decimal):
        raw = float(raw)
    # número -> assume epoch ms se for grande
    if isinstance(raw, (int, float)):
        # se número em segundos (<= 10^12?) -> detect minimal cutoff
        if raw > 10**12:
            return int(raw)
        # se for segundos (ex: 1690000000) -> converte para ms
        if raw > 10**9:
            return int(raw * 1000)
        # caso improvável, tenta converter assumindo segundos
        return int(raw * 1000)
    # string -> tenta parse
    if isinstance(raw, str):
        # se a string é número
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
    """Constroi a lista completa na ordem definida em FULL_DP_FIELDS.
       Se algum campo estiver ausente, coloca None (mas chamamos o filtro a seguir para pular linhas incompletas quando necessário).
    """
    dp = []
    for f in FULL_DP_FIELDS:
        if f == "data_captura":
            dp.append(to_epoch_ms(item.get(f)))
        else:
            v = item.get(f)
            # Decimal -> float
            if isinstance(v, Decimal):
                v = float(v)
            dp.append(v)
    return dp

def lambda_handler(event, context):
    try:
        sensor_id = event.get("pathParameters", {}).get("sensor_id")
        if not sensor_id:
            return {"statusCode": 400, "body": json.dumps({"error": "Parâmetro sensor_id é obrigatório"})}

        # --- Funções especiais herdadas do código do seu amigo (vibração analytics) ---
        def get_vibration_data():
            table = dynamodb.Table("sensor-vibracao")
            response = table.scan()
            itens = response.get("Items", [])
            # filtra itens sem data válida
            itens = [x for x in itens if to_epoch_ms(x.get("data_captura")) is not None]
            return sorted(itens, key=lambda x: to_epoch_ms(x["data_captura"]))

        # 666, 6666, 66666, 666666, 6666666, 66666666 -> mantém como estava
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
                return {"statusCode": 200, "body": json.dumps({"ultimo_restart": datetime.utcfromtimestamp(ultimo_restart/1000).strftime("%Y-%m-%dT%H:%M:%SZ"), "gap_minutos": round(maior_gap, 2)})}
            return {"statusCode": 200, "body": json.dumps({"mensagem": "Nenhum salto de tempo > 20 min"})}

        if sensor_id == "6666":
            itens = get_vibration_data()
            if not itens:
                return {"statusCode": 404, "body": json.dumps({"error": "Sem dados"})}
            total_operando = 0.0
            total_parado = 0.0
            for i in range(1, len(itens)):
                atual = float(itens[i].get("valor", 0) or 0)
                anterior = float(itens[i - 1].get("valor", 0) or 0)
                dt_atual = to_epoch_ms(itens[i]["data_captura"])
                dt_anterior = to_epoch_ms(itens[i - 1]["data_captura"])
                if dt_atual and dt_anterior:
                    diff_min = (dt_atual - dt_anterior) / 1000.0 / 60.0
                    if anterior != 0 or atual != 0:
                        total_operando += diff_min
                    else:
                        total_parado += diff_min
            return {"statusCode": 200, "body": json.dumps({"tempo_operando_min": round(total_operando, 2), "tempo_parado_min": round(total_parado, 2)})}

        if sensor_id == "66666":
            itens = get_vibration_data()
            if not itens:
                return {"statusCode": 404, "body": json.dumps({"error": "Sem dados"})}
            ciclos = 0
            ativo = False
            for item in itens:
                valor = float(item.get("valor", 0) or 0)
                if valor > 0 and not ativo:
                    ativo = True
                elif valor == 0 and ativo:
                    ativo = False
                    ciclos += 1
            return {"statusCode": 200, "body": json.dumps({"ciclos_concluidos": ciclos})}

        if sensor_id == "666666":
            itens = get_vibration_data()
            hoje = datetime.now().date()
            valores_hoje = [float(x.get("valor", 0)) for x in itens if datetime.utcfromtimestamp(to_epoch_ms(x["data_captura"])/1000).date() == hoje]
            if not valores_hoje:
                return {"statusCode": 404, "body": json.dumps({"error": "Sem dados de hoje"})}
            media = sum(valores_hoje) / len(valores_hoje)
            pico = max(valores_hoje)
            return {"statusCode": 200, "body": json.dumps({"media_vibracao": round(media, 3), "pico_vibracao": pico})}

        if sensor_id == "6666666":
            itens = get_vibration_data()
            valores = [float(x.get("valor", 0)) for x in itens]
            if len(valores) < 2:
                return {"statusCode": 200, "body": json.dumps({"desvio_padrao": 0})}
            desvio = statistics.stdev(valores)
            return {"statusCode": 200, "body": json.dumps({"desvio_padrao": round(desvio, 4)})}

        if sensor_id == "66666666":
            itens = get_vibration_data()
            if not itens:
                return {"statusCode": 404, "body": json.dumps({"error": "Sem dados"})}
            max_item = max(itens, key=lambda x: float(x.get("valor", 0) or 0))
            dt = to_epoch_ms(max_item["data_captura"])
            return {"statusCode": 200, "body": json.dumps({"pico_vibracao": float(max_item.get("valor", 0)), "hora_pico": datetime.utcfromtimestamp(dt/1000).strftime("%Y-%m-%dT%H:%M:%SZ")})}

        # --- Caso geral: sensores mapeados ---
        table_info = SENSOR_TABLES.get(sensor_id)
        if not table_info and sensor_id not in METRIC_ENDPOINTS and sensor_id != "11":
            return {"statusCode": 400, "body": json.dumps({"error": f"Sensor_id {sensor_id} inválido"})}

        # Se for um endpoint métrico separado (12,13,...)
        if sensor_id in METRIC_ENDPOINTS:
            campo, alias = METRIC_ENDPOINTS[sensor_id]
            # Para endpoints métricos, precisamos saber de qual tabela pegar os itens:
            # se for por convenção começamos pela tabela equivalente do "grupo" (ex.: 12/13... serão derivados da mesma tabela do 11)
            # assumimos que a origem é a mesma do 11 para cada tipo (ex.: 12 deriva do sensor-corrente)
            # mapa simples: pega table do "11" correspondendo ao grupo (ex.: 12 -> sensor-corrente)
            # vamos derivar a tabela a partir de table_info de "11"
            base_table_info = SENSOR_TABLES.get("11")
            if base_table_info is None:
                return {"statusCode": 500, "body": json.dumps({"error": "Configuração base ausente"})}
            table_name, metric_name = base_table_info
            table = dynamodb.Table(table_name)
            response = table.scan()
            itens = response.get("Items", [])
            if not itens:
                return {"statusCode": 404, "body": json.dumps({"error": "Nenhum dado encontrado para esse sensor"})}

            datapoints = []
            for item in itens:
                try:
                    ts = to_epoch_ms(item.get("data_captura"))
                    if ts is None:
                        continue
                    # estado_operacional especial -> mapeia para número
                    if campo == "estado_operacional":
                        raw = item.get(campo)
                        if raw is None:
                            continue
                        # se for numérico já usa, se string tenta mapear
                        if isinstance(raw, (int, float, Decimal)):
                            val = float(raw)
                        else:
                            val = ESTADO_MAP.get(str(raw), None)
                            if val is None:
                                # pula se não consegue mapear para número
                                continue
                    else:
                        raw = item.get(campo)
                        if raw is None:
                            continue
                        val = float(raw) if isinstance(raw, (int, float, Decimal, str)) and str(raw) != "" else None
                        if val is None:
                            continue
                    datapoints.append([val, ts])
                except Exception as e:
                    print(f"Erro item métrica {item}: {e}")
                    continue
            datapoints.sort(key=lambda x: x[1])
            return {"statusCode": 200, "body": json.dumps([{"target": alias, "datapoints": datapoints}], default=decimal_default)}

        # Se for o endpoint "11" -> retorna o JSON grande (datapoints completos)
        if sensor_id == "11":
            table_name, metric_name = SENSOR_TABLES["11"]
            table = dynamodb.Table(table_name)
            response = table.scan()
            itens = response.get("Items", [])
            if not itens:
                return {"statusCode": 404, "body": json.dumps({"error": "Nenhum dado encontrado para esse sensor"})}

            datapoints = []
            for item in itens:
                try:
                    # Pula registros incompletos (mesma lógica que você mencionou antes)
                    # garante que os campos críticos existam para compor o array completo
                    if (
                        item.get("confiabilidade_perc_oee") is None
                        or item.get("mtbf_minutos") is None
                        or item.get("mttr_minutos") is None
                    ):
                        # pula linha incompleta
                        continue

                    dp = build_full_datapoint(item)
                    # se timestamp inválido pula
                    if dp[1] is None:
                        continue
                    datapoints.append(dp)
                except Exception as e:
                    print(f"Erro ao montar full datapoint para item {item}: {e}")
                    continue

            datapoints.sort(key=lambda x: x[1])
            grafana_response = [{"target": metric_name, "datapoints": datapoints}]
            return {"statusCode": 200, "body": json.dumps(grafana_response, default=decimal_default)}

        # Caso: sensores simples (1,2,3,4,5,6) -> retorna último registro (mesmo comportamento antigo)
        if sensor_id in ["1", "2", "3", "4", "5", "6"]:
            table_name, metric_name = SENSOR_TABLES[sensor_id]
            table = dynamodb.Table(table_name)
            response = table.scan()
            itens = response.get("Items", [])
            if not itens:
                return {"statusCode": 404, "body": json.dumps({"error": "Nenhum dado encontrado para esse sensor"})}
            # pega último registro por campo data_captura (usando epoch ms)
            ultima = max(itens, key=lambda x: (to_epoch_ms(x.get("data_captura")) or 0))
            val = float(ultima.get("valor", 0) or 0)
            ts = to_epoch_ms(ultima.get("data_captura"))
            if ts is None:
                # se não tiver ts, tenta retornar somente valor
                return {"statusCode": 200, "body": json.dumps({"target": metric_name, "datapoints": [[val, None]]}, default=decimal_default)}
            return {"statusCode": 200, "body": json.dumps({"target": metric_name, "datapoints": [[val, ts]]}, default=decimal_default)}

        # fallback (não esperado)
        return {"statusCode": 400, "body": json.dumps({"error": "Rota não tratada"})}

    except Exception as e:
        print(f"[ERRO LAMBDA] {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
