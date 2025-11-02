import boto3
import csv
import urllib.parse
from decimal import Decimal
import io
import json
import os
import traceback

# --- Dependências que exigem Lambda Layer (ou runtime com libs) ---
import pandas as pd
import numpy as np
# -----------------------------------------------------------------

s3 = boto3.client("s3")
dynamo = boto3.resource("dynamodb")

# Mapeamento fk_sensor -> tabela do DynamoDB
TABLES = {
    "1": "sensor-corrente",
    "2": "sensor-tensao",
    "3": "sensor-temperatura",
    "4": "sensor-vibracao",
    "5": "sensor-pressao",
    "6": "sensor-frequencia",
}

# -----------------------------
# Configs de análise (Pandas)
# -----------------------------
COLUNA_VALOR = "valor"
COLUNA_TEMPO = "data_captura"

LIMITE_CORRENTE_DESLIGADA = 0.5
LIMITE_CORRENTE_TRABALHO = 10.0
LIMITE_CORRENTE_SOBRECARGA = 50.0


def definir_estado_operacional(corrente: float) -> str:
    """Classifica a corrente em três estados."""
    if corrente < LIMITE_CORRENTE_DESLIGADA:
        return "Desligada"
    elif corrente >= LIMITE_CORRENTE_TRABALHO:
        return "Em Carga"
    return "Ociosa"


def calcular_indicadores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Análise completa (Pandas) para o sensor 1.
    Retorna o mesmo DataFrame acrescido das métricas calculadas por linha.
    """
    print("Iniciando análise completa (Pandas)...")
    df = df.copy()

    # Garantir colunas mínimas
    for col in (COLUNA_TEMPO, COLUNA_VALOR, "fk_sensor"):
        if col not in df.columns:
            raise ValueError(f"CSV sem coluna obrigatória: {col}")

    # 1) Tempo
    df[COLUNA_TEMPO] = pd.to_datetime(df[COLUNA_TEMPO], errors="coerce")
    df = df.dropna(subset=[COLUNA_TEMPO])
    df = df.sort_values(by=COLUNA_TEMPO).reset_index(drop=True)

    # 2) Duração da linha (em segundos)
    df["duracao_segundos_linha"] = df[COLUNA_TEMPO].diff().dt.total_seconds()
    if df["duracao_segundos_linha"].isna().all():
        # fallback caso só exista 1 linha
        df["duracao_segundos_linha"] = 60.0
    else:
        mean_gap = df["duracao_segundos_linha"].dropna().mean()
        df["duracao_segundos_linha"] = df["duracao_segundos_linha"].fillna(mean_gap if pd.notna(mean_gap) else 60.0)

    # 3) Estados e alertas por linha
    df[COLUNA_VALOR] = pd.to_numeric(df[COLUNA_VALOR], errors="coerce").fillna(0.0)
    df["estado_operacional"] = df[COLUNA_VALOR].apply(definir_estado_operacional)
    df["alerta_sobrecarga"] = df[COLUNA_VALOR] > LIMITE_CORRENTE_SOBRECARGA

    # 4) Agregações para OEE/Confiabilidade
    tempo_total = df["duracao_segundos_linha"].sum()
    tempo_por_estado = df.groupby("estado_operacional")["duracao_segundos_linha"].sum()

    perc_em_carga = (tempo_por_estado.get("Em Carga", 0.0) / tempo_total * 100) if tempo_total > 0 else 0.0
    perc_ociosa = (tempo_por_estado.get("Ociosa", 0.0) / tempo_total * 100) if tempo_total > 0 else 0.0
    perc_desligada = (tempo_por_estado.get("Desligada", 0.0) / tempo_total * 100) if tempo_total > 0 else 0.0

    # MTBF/MTTR: alternância de "UP" (Em Carga) vs "DOWN" (outros)
    df["estado_mtbf"] = np.where(df["estado_operacional"] == "Em Carga", "UP", "DOWN")
    df["mudou_estado_mtbf"] = df["estado_mtbf"].shift().ne(df["estado_mtbf"])
    df.loc[df.index.min(), "mudou_estado_mtbf"] = True
    df["group_id"] = df["mudou_estado_mtbf"].cumsum()

    duracao_por_evento = df.groupby("group_id").agg(
        estado=("estado_mtbf", "first"),
        duracao_total_segundos=("duracao_segundos_linha", "sum"),
    )

    uptime_seg = duracao_por_evento.loc[duracao_por_evento["estado"] == "UP", "duracao_total_segundos"]
    downtime_seg = duracao_por_evento.loc[duracao_por_evento["estado"] == "DOWN", "duracao_total_segundos"]

    mtbf_min = (uptime_seg.mean() / 60.0) if len(uptime_seg) else 0.0
    mttr_min = (downtime_seg.mean() / 60.0) if len(downtime_seg) else 0.0

    total_uptime = uptime_seg.sum()
    total_downtime = downtime_seg.sum()
    confiab_perc = (total_uptime / (total_uptime + total_downtime) * 100) if (total_uptime + total_downtime) > 0 else 100.0

    # Preditiva: carga média quando em carga
    carga_media_trabalho_amps = df.loc[df["estado_operacional"] == "Em Carga", COLUNA_VALOR].mean()
    if pd.isna(carga_media_trabalho_amps):
        carga_media_trabalho_amps = 0.0

    total_eventos_sobrecarga = int(df["alerta_sobrecarga"].sum())

    # 5) Gravar métricas por linha (constantes ao período analisado)
    df["mtbf_minutos"] = float(mtbf_min)
    df["mttr_minutos"] = float(mttr_min)
    df["confiabilidade_perc_oee"] = float(confiab_perc)
    df["perc_tempo_em_carga"] = float(perc_em_carga)
    df["perc_tempo_ociosa"] = float(perc_ociosa)
    df["perc_tempo_desligada"] = float(perc_desligada)
    df["carga_media_trabalho_amps"] = float(carga_media_trabalho_amps)
    df["total_eventos_sobrecarga"] = int(total_eventos_sobrecarga)

    # 6) Limpeza de colunas temporárias
    df = df.drop(columns=["duracao_segundos_linha", "estado_mtbf", "mudou_estado_mtbf", "group_id"])

    # 7) Segurança: sem NaN para subir ao Dynamo
    df = df.fillna(0)

    print("Análise completa (Pandas) concluída.")
    return df


def lambda_handler(event, context):
    """
    Evento esperado: S3:ObjectCreated (arquivo CSV).
    Lê o CSV do bucket/origem e grava nas tabelas do Dynamo mapeadas em TABLES.
    - fk_sensor == "1": processa via Pandas e grava itens completos
    - fk_sensor != "1": grava linha a linha (data_captura, valor)
    """
    source_bucket = None
    source_key = None
    registros = 0

    try:
        # 1) Extrai info do evento S3
        source_bucket = event["Records"][0]["s3"]["bucket"]["name"]
        source_key = urllib.parse.unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding="utf-8")
        print(f"Processando arquivo: {source_key} (bucket: {source_bucket})")

        if not source_key.endswith(".csv"):
            print(f"Ignorando arquivo não-CSV: {source_key}")
            return {"status": "ignorado", "arquivo": source_key}

        # 2) Lê CSV em memória
        obj = s3.get_object(Bucket=source_bucket, Key=source_key)
        csv_content = obj["Body"].read().decode("utf-8-sig")

        csv_buffer = io.StringIO(csv_content)
        reader_peek = csv.DictReader(csv_buffer)

        try:
            first_row = next(reader_peek)
        except StopIteration:
            print("CSV vazio. Nada a fazer.")
            return {"status": "ignorado", "arquivo": source_key}

        sensor_id = (first_row.get("fk_sensor") or "").strip()
        if not sensor_id:
            raise ValueError("fk_sensor ausente na primeira linha do CSV.")

        # reset do buffer para reler o arquivo
        csv_buffer.seek(0)

        if sensor_id == "1":
            # -------------------------
            # CAMINHO: sensor 1 (Pandas)
            # -------------------------
            print("Sensor '1' detectado. Rodando análise com Pandas...")

            df_bruto = pd.read_csv(csv_buffer)

            # Guard rails mínimos para tipos
            if COLUNA_VALOR in df_bruto.columns:
                df_bruto[COLUNA_VALOR] = pd.to_numeric(df_bruto[COLUNA_VALOR], errors="coerce")

            df_final = calcular_indicadores(df_bruto)

            # 🔧 Conversão garantida de datetime -> str (evita 'Timestamp is not JSON serializable')
            if COLUNA_TEMPO in df_final.columns:
                df_final[COLUNA_TEMPO] = df_final[COLUNA_TEMPO].astype(str)

            # Envio ao Dynamo
            table_name = TABLES[sensor_id]
            tabela = dynamo.Table(table_name)

            for item in df_final.to_dict("records"):
                # Converte para tipos aceitos pelo Dynamo:
                # - default=str: se sobrar qualquer tipo estranho, vira string (ex. Timestamp)
                # - parse_float=Decimal: garante Decimal para floats
                item_dynamo = json.loads(json.dumps(item, default=str), parse_float=Decimal)

                # Ajuste de tipos críticos
                item_dynamo["alerta_sobrecarga"] = bool(item_dynamo.get("alerta_sobrecarga", False))
                item_dynamo["fk_sensor"] = str(item_dynamo.get("fk_sensor", "1"))

                # data_captura já está string
                if COLUNA_TEMPO in item_dynamo and item_dynamo[COLUNA_TEMPO] is None:
                    # fallback (não deveria acontecer após astype(str))
                    item_dynamo[COLUNA_TEMPO] = ""

                tabela.put_item(Item=item_dynamo)
                registros += 1

        else:
            # -------------------------------------------
            # CAMINHO: demais sensores (lógica row-by-row)
            # -------------------------------------------
            print(f"Sensor '{sensor_id}' detectado. Rodando lógica original (row-by-row)...")
            if sensor_id not in TABLES:
                raise ValueError(f"fk_sensor {sensor_id} não mapeado em TABLES.")

            reader = csv.DictReader(csv_buffer)
            table_name = TABLES[sensor_id]
            tabela = dynamo.Table(table_name)

            for row in reader:
                if not row:
                    continue

                fk_sensor_row = (row.get("fk_sensor") or "").strip()
                if not fk_sensor_row or fk_sensor_row not in TABLES:
                    print(f"Linha ignorada (fk_sensor inválido): {row}")
                    continue

                valor = row.get("valor")
                data_captura = row.get("data_captura")

                if valor is None or data_captura is None:
                    print(f"Linha ignorada (dados incompletos): {row}")
                    continue

                try:
                    item = {
                        "data_captura": str(data_captura).strip(),
                        "valor": Decimal(str(valor).strip()),
                    }
                except Exception as conv_err:
                    print(f"Falha convertendo linha -> Decimal: {row} | err: {conv_err}")
                    continue

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
        traceback.print_exc()
        return {
            "status": "erro",
            "arquivo_processado": source_key,
            "mensagem": str(e),
        }
