import json
import boto3
from decimal import Decimal
from datetime import datetime
import statistics

dynamodb = boto3.resource('dynamodb')

SENSOR_TABLES = {
    "1": ("sensor-corrente", "corrente"),
    "2": ("sensor-frequencia", "frequencia"),
    "3": ("sensor-pressao", "pressao"),
    "4": ("sensor-temperatura", "temperatura"),
    "5": ("sensor-tensao", "tensao"),
    "6": ("sensor-vibracao", "vibracao"),
    "11": ("sensor-corrente", "corrente"),
    "22": ("sensor-frequencia", "frequencia"),
    "33": ("sensor-pressao", "pressao"),
    "44": ("sensor-temperatura", "temperatura"),
    "55": ("sensor-tensao", "tensao"),
    "66": ("sensor-vibracao", "vibracao")
}

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def parse_dt_any(v):
    """
    Aceita:
     - int/float/Decimal (assume ms se > 1e12, senão assume seconds e multiplica por 1000)
     - string numérica (mesma regra)
     - string 'YYYY-MM-DD HH:MM:SS' ou 'YYYY-MM-DD HH:MM'
    Retorna datetime ou None.
    """
    if v is None:
        return None
    # números (Decimal, int, float)
    if isinstance(v, (int, float, Decimal)):
        try:
            iv = int(v)
            # se já está em ms (>= 1e12 ~ ano 2001) -> ms
            if iv > 10**12:
                return datetime.fromtimestamp(iv / 1000.0)
            # senão, considera segundos
            return datetime.fromtimestamp(iv)
        except Exception:
            return None
    # strings
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            try:
                iv = int(s)
                if iv > 10**12:
                    return datetime.fromtimestamp(iv / 1000.0)
                return datetime.fromtimestamp(iv)
            except Exception:
                return None
        # tenta formatos conhecidos
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
    return None

def to_ms_from_any(v):
    dt = parse_dt_any(v)
    if dt:
        return int(dt.timestamp() * 1000)
    # se for número e parse_dt_any não retornou (caso raro), tenta converter direto:
    if isinstance(v, (int, float, Decimal)):
        iv = int(v)
        if iv > 10**12:
            return iv
        return iv * 1000
    if isinstance(v, str) and v.isdigit():
        iv = int(v)
        if iv > 10**12:
            return iv
        return iv * 1000
    return None

def lambda_handler(event, context):
    try:
        sensor_id = event.get('pathParameters', {}).get('sensor_id')
        if not sensor_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Parâmetro sensor_id é obrigatório"})
            }

        # --- Função auxiliar para ler vibração (usada pelos 666* especiais) ---
        def get_vibration_data():
            table = dynamodb.Table("sensor-vibracao")
            response = table.scan()
            itens = response.get("Items", [])
            # filtra apenas itens com data_captura parseável
            itens = [x for x in itens if parse_dt_any(x.get("data_captura"))]
            return sorted(itens, key=lambda x: to_ms_from_any(x["data_captura"]))

        # --- Blocos 666* (mantidos do código do seu amigo) ---
        if sensor_id == "666":
            itens = get_vibration_data()
            if not itens:
                return {"statusCode": 404, "body": json.dumps({"error": "Nenhum dado encontrado"})}

            ultimo_restart, maior_gap = None, 0
            for i in range(1, len(itens)):
                dt_atual = parse_dt_any(itens[i]["data_captura"])
                dt_anterior = parse_dt_any(itens[i - 1]["data_captura"])
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

        elif sensor_id == "6666":
            itens = get_vibration_data()
            if not itens:
                return {"statusCode": 404, "body": json.dumps({"error": "Sem dados"})}

            total_operando = 0
            total_parado = 0
            for i in range(1, len(itens)):
                atual = float(itens[i].get("valor", 0) or 0)
                anterior = float(itens[i - 1].get("valor", 0) or 0)
                dt_atual = parse_dt_any(itens[i]["data_captura"])
                dt_anterior = parse_dt_any(itens[i - 1]["data_captura"])
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

        elif sensor_id == "66666":
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

        elif sensor_id == "666666":
            itens = get_vibration_data()
            hoje = datetime.now().date()
            valores_hoje = [
                float(x.get("valor", 0) or 0)
                for x in itens
                if parse_dt_any(x.get("data_captura")) and parse_dt_any(x.get("data_captura")).date() == hoje
            ]

            if not valores_hoje:
                return {"statusCode": 404, "body": json.dumps({"error": "Sem dados de hoje"})}

            media = sum(valores_hoje) / len(valores_hoje)
            pico = max(valores_hoje)
            return {
                "statusCode": 200,
                "body": json.dumps({"media_vibracao": round(media, 3), "pico_vibracao": pico})
            }

        elif sensor_id == "6666666":
            itens = get_vibration_data()
            valores = [float(x.get("valor", 0) or 0) for x in itens]
            if len(valores) < 2:
                return {"statusCode": 200, "body": json.dumps({"desvio_padrao": 0})}

            desvio = statistics.stdev(valores)
            return {
                "statusCode": 200,
                "body": json.dumps({"desvio_padrao": round(desvio, 4)})
            }

        elif sensor_id == "66666666":
            itens = get_vibration_data()
            if not itens:
                return {"statusCode": 404, "body": json.dumps({"error": "Sem dados"})}

            max_item = max(itens, key=lambda x: float(x.get("valor", 0) or 0))
            dt = parse_dt_any(max_item.get("data_captura"))
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "pico_vibracao": float(max_item.get("valor", 0) or 0),
                    "hora_pico": dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None
                })
            }

        # --- CASO GERAL ---
        table_info = SENSOR_TABLES.get(sensor_id)
        if not table_info:
            return {"statusCode": 400, "body": json.dumps({"error": "Sensor_id inválido"})}

        table_name, metric_name = table_info
        table = dynamodb.Table(table_name)
        response = table.scan()
        itens = response.get("Items", [])

        if not itens:
            return {"statusCode": 404, "body": json.dumps({"error": "Nenhum dado encontrado"})}

        # --- Para os grafana_ids que devolvem série temporal completa (11,22,...) ---
        if sensor_id in ["11", "22", "33", "44", "55", "66"]:
            datapoints = []
            for item in itens:
                try:
                    # valor (campo 'valor')
                    raw_val = item.get("valor")
                    if raw_val is None:
                        # pula itens sem valor
                        continue
                    valor = float(raw_val)

                    # timestamp (data_captura) -> milissegundos
                    ts_ms = to_ms_from_any(item.get("data_captura"))
                    if ts_ms is None:
                        # pula itens sem timestamp válido
                        continue

                    # --- Para o caso específico do sensor 11: manter colunas extras (mesma ordem do seu JSON)
                    if sensor_id == "11":
                        # Filtragem que você mencionou: evita linhas incompletas (trava dos nulls)
                        if (
                            item.get("confiabilidade_perc_oee") is None
                            or item.get("mtbf_minutos") is None
                            or item.get("mttr_minutos") is None
                        ):
                            # pula este registro (era o que evitava uma série com muitos nulls)
                            continue

                        # montar a linha na ordem esperada (preserve as posições para os JSONPath)
                        row = [
                            valor,
                            ts_ms,
                            item.get("alerta_sobrecarga", None),
                            item.get("carga_media_trabalho_amps", None),
                            item.get("confiabilidade_perc_oee", None),
                            item.get("estado_operacional", None),
                            item.get("fk_sensor", None),
                            item.get("mtbf_minutos", None),
                            item.get("mttr_minutos", None),
                            item.get("perc_tempo_desligada", None),
                            item.get("perc_tempo_em_carga", None),
                            item.get("perc_tempo_ociosa", None),
                            item.get("total_eventos_sobrecarga", None),
                        ]
                        # garantir que valor e timestamp existem (dupla checagem)
                        if row[0] is None or row[1] is None:
                            continue
                        datapoints.append(row)
                    else:
                        # outros sensores 22/33/... -> formato simples [valor, timestamp]
                        datapoints.append([valor, ts_ms])

                except Exception as e:
                    print(f"Erro ao processar item {item}: {e}")
                    continue

            # ordena por timestamp
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

        # --- Para sensores 1..6: apenas o último registro (comportamento original) ---
        # usa max por data_captura (comparando com parse_dt_any para evitar strings estranhas)
        def key_dt(item):
            dt = parse_dt_any(item.get("data_captura"))
            if dt:
                return dt
            # fallback: tenta converter numeric, senão string
            return datetime.min

        ultimo = max(itens, key=key_dt)
        valor = float(ultimo.get("valor", 0) or 0)
        ts_ms = to_ms_from_any(ultimo.get("data_captura"))
        # se timestamp for None, tenta ignorar e deixar sem timestamp — mas mantemos ms quando possível
        datapoint = [[valor, ts_ms]] if ts_ms is not None else [[valor, None]]

        return {
            "statusCode": 200,
            "body": json.dumps({
                "target": metric_name,
                "datapoints": datapoint
            }, default=decimal_default)
        }

    except Exception as e:
        print(f"[ERRO] {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
