import boto3
import csv
import urllib.parse
from decimal import Decimal
import io # Necessário para ler o CSV em memória
import json # Necessário para converter para o Dynamo

# --- [ Dependências que exigem um Lambda Layer ] ---
import pandas as pd
import numpy as np
# ----------------------------------------------------

s3 = boto3.client("s3")

TABLES = {
    "1": "sensor-corrente",
    "2": "sensor-tensao",
    "3": "sensor-temperatura",
    "4": "sensor-vibracao",
    "5": "sensor-pressao",
    "6": "sensor-frequencia"
}

dynamo = boto3.resource("dynamodb")

# --- [ INÍCIO DAS FUNÇÕES DE ANÁLISE (copiadas do seu script) ] ---

# --- Configurações de Análise (movidas para cá) ---
COLUNA_VALOR = 'valor'
COLUNA_TEMPO = 'data_captura'
LIMITE_CORRENTE_DESLIGADA = 0.5
LIMITE_CORRENTE_TRABALHO  = 10.0
LIMITE_CORRENTE_SOBRECARGA = 50.0

def definir_estado_operacional(corrente):
    """Classifica a corrente em um dos três estados operacionais."""
    if corrente < LIMITE_CORRENTE_DESLIGADA:
        return 'Desligada'
    elif corrente >= LIMITE_CORRENTE_TRABALHO:
        return 'Em Carga'
    else:
        return 'Ociosa'

def calcular_indicadores(df):
    """Função principal para calcular todas as métricas."""
    
    print("Iniciando análise completa (Pandas)...")
    
    # [ 2. PREPARAÇÃO DOS DADOS ]
    df[COLUNA_TEMPO] = pd.to_datetime(df[COLUNA_TEMPO])
    df = df.sort_values(by=COLUNA_TEMPO).reset_index(drop=True)
    df['duracao_segundos_linha'] = df[COLUNA_TEMPO].diff().dt.total_seconds()
    df['duracao_segundos_linha'].fillna(df['duracao_segundos_linha'].mean(), inplace=True)

    # [ 3. MÉTRICAS POR LINHA ]
    df['estado_operacional'] = df[COLUNA_VALOR].apply(definir_estado_operacional)
    df['alerta_sobrecarga'] = df[COLUNA_VALOR] > LIMITE_CORRENTE_SOBRECARGA

    # [ 4. CÁLCULO DAS MÉTRICAS AGREGADAS ]
    
    # a) Eficiência (OEE)
    tempo_total_segundos = df['duracao_segundos_linha'].sum()
    tempo_por_estado = df.groupby('estado_operacional')['duracao_segundos_linha'].sum()
    perc_em_carga  = (tempo_por_estado.get('Em Carga', 0) / tempo_total_segundos) * 100
    perc_ociosa    = (tempo_por_estado.get('Ociosa', 0) / tempo_total_segundos) * 100
    perc_desligada = (tempo_por_estado.get('Desligada', 0) / tempo_total_segundos) * 100

    # b) Confiabilidade (MTBF/MTTR)
    df['estado_mtbf'] = np.where(df['estado_operacional'] == 'Em Carga', 'UP', 'DOWN')
    df['mudou_estado_mtbf'] = df['estado_mtbf'].shift() != df['estado_mtbf']
    df.loc[0, 'mudou_estado_mtbf'] = True
    df['group_id'] = df['mudou_estado_mtbf'].cumsum()
    
    duracao_por_evento = df.groupby('group_id').agg(
        estado=('estado_mtbf', 'first'),
        duracao_total_segundos=('duracao_segundos_linha', 'sum')
    )
    periodos_uptime_seg = duracao_por_evento[duracao_por_evento['estado'] == 'UP']['duracao_total_segundos']
    periodos_downtime_seg = duracao_por_evento[duracao_por_evento['estado'] == 'DOWN']['duracao_total_segundos']
    
    mtbf_segundos = periodos_uptime_seg.mean()
    mttr_segundos = periodos_downtime_seg.mean()
    mtbf_final_minutos = (mtbf_segundos / 60) if pd.notna(mtbf_segundos) else 0
    mttr_final_minutos = (mttr_segundos / 60) if pd.notna(mttr_segundos) else 0

    total_uptime = periodos_uptime_seg.sum()
    total_downtime = periodos_downtime_seg.sum()
    confiabilidade_final_perc = (total_uptime / (total_uptime + total_downtime)) * 100 if (total_uptime + total_downtime) > 0 else 100.0

    # c) Preditiva (Carga Média)
    df_em_carga = df[df['estado_operacional'] == 'Em Carga']
    carga_media_trabalho_amps = df_em_carga[COLUNA_VALOR].mean() if not df_em_carga.empty else 0

    # d) Alertas (Contagem Total)
    total_eventos_sobrecarga = df['alerta_sobrecarga'].sum()

    # [ 5. ADICIONAR MÉTRICAS AGREGADAS ]
    df['mtbf_minutos'] = mtbf_final_minutos
    df['mttr_minutos'] = mttr_final_minutos
    df['confiabilidade_perc_oee'] = confiabilidade_final_perc
    df['perc_tempo_em_carga'] = perc_em_carga
    df['perc_tempo_ociosa'] = perc_ociosa
    df['perc_tempo_desligada'] = perc_desligada
    df['carga_media_trabalho_amps'] = carga_media_trabalho_amps
    df['total_eventos_sobrecarga'] = total_eventos_sobrecarga

    # [ 6. LIMPEZA ]
    colunas_para_remover = ['duracao_segundos_linha', 'estado_mtbf', 'mudou_estado_mtbf', 'group_id']
    df = df.drop(columns=colunas_para_remover)
    df.fillna(0, inplace=True)
    
    print("Análise completa (Pandas) concluída.")
    return df

# --- [ FIM DAS FUNÇÕES DE ANÁLISE ] ---


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

        # Lê o arquivo CSV do bucket para a memória
        obj = s3.get_object(Bucket=source_bucket, Key=source_key)
        csv_content = obj["Body"].read().decode("utf-8-sig")
        
        # Cria um buffer de texto para 'espiar' e 'reler' o arquivo
        csv_buffer = io.StringIO(csv_content)
        
        # Espia a primeira linha para descobrir o fk_sensor
        reader_peek = csv.DictReader(csv_buffer)
        try:
            first_row = next(reader_peek)
            sensor_id = first_row.get("fk_sensor", "").strip()
        except StopIteration:
            print("Arquivo CSV vazio. Ignorando.")
            return {"status": "ignorado", "arquivo": source_key}
        
        # Reinicia o buffer para o início
        csv_buffer.seek(0)
        
        
        # --- [ INÍCIO DA LÓGICA DE DIVISÃO ] ---
        
        registros = 0
        
        if sensor_id == "1":
            # --- CAMINHO 1: SENSOR DE CORRENTE ---
            print("Sensor '1' detectado. Rodando análise completa com Pandas...")
            
            # 1. Carrega o CSV no Pandas
            df_bruto = pd.read_csv(csv_buffer)
            
            # 2. Executa a análise completa
            df_completo = calcular_indicadores(df_bruto)
            
            # 3. Salva no DynamoDB
            tabela = dynamo.Table(TABLES[sensor_id])
            
            # Converte o DataFrame para uma lista de dicts
            itens_para_enviar = df_completo.to_dict('records')
            
            for item in itens_para_enviar:
                # Converte floats/bools/ints para o formato DynamoDB
                # Usa json.dumps/loads para converter float -> Decimal
                item_dynamo = json.loads(json.dumps(item), parse_float=Decimal)
                
                # Converte 'alerta_sobrecarga' de True/False para Booleano do Dynamo
                item_dynamo['alerta_sobrecarga'] = bool(item_dynamo.get('alerta_sobrecarga'))
                
                # Garante que fk_sensor é string (Pandas pode ter convertido para int)
                item_dynamo['fk_sensor'] = str(item_dynamo['fk_sensor'])
                
                # Converte data_captura de volta para string (Pandas converteu para datetime)
                item_dynamo['data_captura'] = str(item['data_captura'])

                tabela.put_item(Item=item_dynamo)
                registros += 1

        else:
            # --- CAMINHO 2: OUTROS SENSORES (LÓGICA ORIGINAL) ---
            print(f"Sensor '{sensor_id}' detectado. Rodando lógica original (row-by-row)...")
            
            # Recria o reader no buffer reiniciado
            reader = csv.DictReader(csv_buffer) 
            
            for row in reader:
                # (Esta é a sua lógica original, preservada 100%)
                if not row:
                    continue
                
                fk_sensor_row = row.get("fk_sensor", "").strip()
                if not fk_sensor_row:
                    print(f"Linha ignorada, fk_sensor ausente: {row}")
                    continue
                
                if fk_sensor_row not in TABLES:
                    print(f"fk_sensor {fk_sensor_row} não tem tabela associada. Ignorado.")
                    continue
                
                tabela_destino = TABLES[fk_sensor_row]
                tabela = dynamo.Table(tabela_destino)

                valor = row.get("valor")
                data_captura = row.get("data_captura")

                if valor is None or data_captura is None:
                    print(f"Linha ignorada, dados incompletos: {row}")
                    continue

                item = {
                    "data_captura": data_captura.strip(),
                    "valor": Decimal(str(valor).strip())
                }

                tabela.put_item(Item=item)
                registros += 1

        # --- [ FIM DA LÓGICA DE DIVISÃO ] ---

        print(f"Processado {registros} registros do arquivo {source_key}")
        return {
            "status": "sucesso",
            "arquivo_processado": source_key,
            "registros_inseridos": registros,
        }

    except Exception as e:
        print(f"Erro ao processar arquivo {source_key}: {e}")
        import traceback
        traceback.print_exc() # Imprime o stack trace completo no CloudWatch
        raise e