import boto3
import json
import urllib.parse
import csv
import pandas as pd
import numpy as np
import io
from datetime import datetime

s3 = boto3.client('s3')
RAW_BUCKET = 'raw-bucket-891377383993' 
TRUSTED_BUCKET = 'trusted-bucket-891377383993'
CLIENT_BUCKET = 'client-bucket-891377383993'

def lambda_handler(event, context):
    for record in event['Records']:
        source_bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        print(f"Processando arquivo: s3://{source_bucket}/{key}")

        try:
            # Processa a base ANEEL específica
            aneel_csv_key = aneel_raw_to_trusted(s3, RAW_BUCKET, TRUSTED_BUCKET)
            if aneel_csv_key:
                print(f"→ Chamando função de cliente para ANEEL: {aneel_csv_key}")
            raw_to_trusted(source_bucket, key)
            trusted_to_client(key)
        except Exception as e:
            print(f"function=lambda_handler_error file={key} message={e}")

    



def raw_to_trusted(source_bucket, key):
    try:
        # Lê o JSON do bucket de origem
        response = s3.get_object(Bucket=source_bucket, Key=key)
        raw_bytes = response['Body'].read()

        # Tenta decodificar o conteúdo
        try:
            content = raw_bytes.decode('utf-8')
        except UnicodeDecodeError:
            content = raw_bytes.decode('latin1')

        # Remove BOM se houver
        content = content.lstrip('\ufeff')

        # Converte o conteúdo JSON
        data = json.loads(content)
        if isinstance(data, dict):
            data = [data]

        print(f"Total de registros lidos: {len(data)}")

        valid_rows = []
        for row in data:
            if not row:
                continue

            data_captura = row.get("data_captura")
            if not data_captura:
                continue

            try:
                data_dt = datetime.strptime(data_captura, "%Y-%m-%d %H:%M:%S")
                dia_captura = data_dt.strftime("%Y-%m-%d")
                hora_captura = data_dt.strftime("%H:%M:%S")
            except ValueError:
                print(f"Formato de data inválido em {data_captura}, arquivo: {key}")
                continue

            valid_rows.append({
                "corrente": row.get("sensor_1"),
                "tensao": row.get("sensor_2"),
                "temperatura": row.get("sensor_3"),
                "vibracao": row.get("sensor_4"),
                "pressao": row.get("sensor_5"),
                "frequencia": row.get("sensor_6"),
                "dia_captura": dia_captura,
                "hora_captura": hora_captura
            })

        if not valid_rows:
            print(f"Nenhum dado válido encontrado no arquivo {key}.")
            return

        # Cria o CSV em memória
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=[
            "corrente", "tensao", "temperatura", "vibracao", "pressao", "frequencia",
            "dia_captura", "hora_captura"
        ])
        writer.writeheader()
        writer.writerows(valid_rows)
        csv_data = output.getvalue().encode('utf-8')
        output.close()

        # Define o nome do arquivo no bucket trusted
        csv_key = key.replace('.json', '.csv')

        # Envia para o bucket trusted
        s3.put_object(
            Bucket=TRUSTED_BUCKET,
            Key=csv_key,
            Body=csv_data,
            ContentType='text/csv'
        )

        print(f"CSV salvo no bucket '{TRUSTED_BUCKET}' como '{csv_key}'")

    except Exception as e:
        print(f"function=raw_to_trusted_error file={key} message={e}")

def trusted_to_client(key):
    try:
        csv_key = key.replace('.json', '.csv')

        # 1️⃣ Lê o CSV do trusted e cria o DataFrame
        obj = s3.get_object(Bucket=TRUSTED_BUCKET, Key=csv_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))

        # 2️⃣ Chama todas as funções de tratamento, passando o df
        for func in [corrente, tensao, temperatura, vibracao, pressao, frequencia]:
            try:
                df = func(df)  # <- alteração aqui
            except Exception as e:
                print(f"Erro na função {func.__name__}: {e}")

        
        # 3️⃣ Enriquecer com dados da ANEEL
        try:
            df = gerar_flat_table_aneel(df, s3)
        except Exception as e:
            print(f"Erro ao integrar ANEEL na flat table: {e}")

        # 3️⃣ Salva o DataFrame final tratado no client bucket
        out = io.StringIO()
        df.to_csv(out, index=False)
        s3.put_object(
            Bucket=CLIENT_BUCKET,
            Key=csv_key,
            Body=out.getvalue().encode('utf-8'),
            ContentType='text/csv'
        )

        print(f"CSV copiado com sucesso para o bucket '{CLIENT_BUCKET}' como '{csv_key}'")
    except Exception as e:
        print(f"function=trusted_to_client_error file={key} message={e}")

def aneel_raw_to_trusted(s3, raw_bucket, trusted_bucket):

    ANEEL_KEY = "falhas_energia_sbc.csv"

    try:
        # 1️⃣ Ler o arquivo do bucket RAW
        response = s3.get_object(Bucket=raw_bucket, Key=ANEEL_KEY)
        raw_bytes = response['Body'].read()

        # Detectar encoding automaticamente
        try:
            csv_content = raw_bytes.decode('utf-8')
        except UnicodeDecodeError:
            csv_content = raw_bytes.decode('latin1')

        df = pd.read_csv(io.StringIO(csv_content))
        print(f"✅ CSV lido com {len(df)} linhas e {len(df.columns)} colunas")

        # 2️⃣ Remover colunas irrelevantes se existirem
        colunas_remover = [
            '_id', 'DatGeracaoConjuntoDados', 'NumOrdemInterrupcao',
            'IdeMotivoInterrupcao', 'numCPF', 'rank'
        ]
        df.drop(columns=[c for c in colunas_remover if c in df.columns], inplace=True, errors='ignore')

        # 3️⃣ Limpar registros totalmente nulos
        df.dropna(how='all', inplace=True)
        df.replace({np.nan: None}, inplace=True)

        # 4️⃣ Filtrar por Alimentador "SBC 0113"
        if "DscAlimentadorSubestacao" in df.columns:
            df = df[df["DscAlimentadorSubestacao"].str.contains("0113", case=False, na=False)]
        else:
            print("⚠️ Coluna 'DscAlimentadorSubestacao' não encontrada — nenhum filtro aplicado.")

        print(f"📊 Linhas após filtro SBC 0113: {len(df)}")

        if df.empty:
            print("⚠️ Nenhum registro encontrado com Alimentador SBC 0113.")
        
        # 5️⃣ Separar e renomear colunas de data/hora
        def separar_data_hora(df, coluna_original, prefixo):
            if coluna_original not in df.columns:
                return df
            df[coluna_original] = pd.to_datetime(df[coluna_original], errors='coerce')
            df[f"ANEEL_{prefixo}_Data"] = df[coluna_original].dt.strftime("%Y-%m-%d")
            df[f"ANEEL_{prefixo}_Hora"] = df[coluna_original].dt.strftime("%H:%M")  # ⬅️ sem segundos
            df.drop(columns=[coluna_original], inplace=True)
            return df

        df = separar_data_hora(df, "DatInicioInterrupcao", "Inicio_Interrupcao")
        df = separar_data_hora(df, "DatFimInterrupcao", "Fim_Interrupcao")

        # 6️⃣ Renomear colunas principais (com prefixo ANEEL_)
        rename_map = {
            'IdeConjuntoUnidadeConsumidora': 'ANEEL_ID_Conjunto_UC',
            'DscConjuntoUnidadeConsumidora': 'ANEEL_Nome_Conjunto_UC',
            'DscAlimentadorSubestacao': 'ANEEL_Alimentador',
            'DscSubestacaoDistribuicao': 'ANEEL_Subestacao',
            'DscTipoInterrupcao': 'ANEEL_Tipo_Interrupcao',
            'DscFatoGeradorInterrupcao': 'ANEEL_Fato_Gerador',
            'NumNivelTensao': 'ANEEL_Nivel_Tensao',
            'NumUnidadeConsumidora': 'ANEEL_Qtd_UC_Afetadas',
            'NumConsumidorConjunto': 'ANEEL_Qtd_Consumidores_Afetados',
            'NumAno': 'ANEEL_Ano',
            'NomAgenteRegulado': 'ANEEL_Agente_Regulado',
            'SigAgente': 'ANEEL_Sigla_Agente'
        }

        df.rename(columns=rename_map, inplace=True)

        # 7️⃣ Limpeza final: remover duplicatas e resetar índices
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True, inplace=True)

        # 8️⃣ Salvar no bucket TRUSTED
        aneel_trusted_key = ANEEL_KEY.replace('.csv', '_trusted_vw.csv')
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        s3.put_object(
            Bucket=trusted_bucket,
            Key=aneel_trusted_key,
            Body=csv_buffer.getvalue().encode('utf-8'),
            ContentType='text/csv'
        )

        print(f"✅ Arquivo tratado salvo em s3://{trusted_bucket}/{aneel_trusted_key}")
        return aneel_trusted_key

    except Exception as e:
        print(f"❌ function=aneel_raw_to_trusted_error message={e}")
        return None

def gerar_flat_table_aneel(df, s3):
    TRUSTED_BUCKET = "trusted-bucket-891377383993"
    ANEEL_KEY = "falhas_energia_sbc_trusted_vw.csv"

    # 1) Lê ANEEL já tratada do trusted
    obj = s3.get_object(Bucket=TRUSTED_BUCKET, Key=ANEEL_KEY)
    aneel = pd.read_csv(io.BytesIO(obj["Body"].read()))

    # 2) Cria o datetime unificado de início e fim da falha
    aneel["ANEEL_Inicio"] = pd.to_datetime(
        aneel["ANEEL_Inicio_Interrupcao_Data"] + " " + aneel["ANEEL_Inicio_Interrupcao_Hora"],
        errors="coerce"
    )
    aneel["ANEEL_Fim"] = pd.to_datetime(
        aneel["ANEEL_Fim_Interrupcao_Data"] + " " + aneel["ANEEL_Fim_Interrupcao_Hora"],
        errors="coerce"
    )

    # 3) Converte data do sensor
    df["data_captura"] = pd.to_datetime(df["data_captura"], errors="coerce")

    # 4) Cria colunas ANEEL no df do sensor
    cols = [
        "ANEEL_Subestacao", "ANEEL_Alimentador", "ANEEL_Tipo_Interrupcao",
        "ANEEL_Fato_Gerador", "ANEEL_Nivel_Tensao", "ANEEL_Qtd_UC_Afetadas",
        "ANEEL_Qtd_Consumidores_Afetados", "ANEEL_Agente_Regulado"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None

    # 5) Marca falhas somente na granularidade do sensor
    for _, row in aneel.iterrows():
        mask = (df["data_captura"] >= row["ANEEL_Inicio"]) & (df["data_captura"] <= row["ANEEL_Fim"])
        for c in cols:
            if c in row:
                df.loc[mask, c] = row[c]

    print(f"✓ ANEEL integrada no df do sensor ({len(aneel)} eventos lidos)")
    return df



def corrente(df):
    print("→ Calculando métricas de corrente...")

    LIMITE_DESLIGADA = 0.5
    LIMITE_TRABALHO = 10.0
    LIMITE_SOBRECARGA = 50.0

    df['data_captura'] = pd.to_datetime(df['dia_captura'] + ' ' + df['hora_captura'])
    df = df.sort_values(by='data_captura').reset_index(drop=True)

    def definir_estado(c):
        if c < LIMITE_DESLIGADA:
            return "Desligada"
        elif c >= LIMITE_TRABALHO:
            return "Em Carga"
        else:
            return "Ociosa"

    df['estado_operacional'] = df['corrente'].apply(definir_estado)
    df['alerta_sobrecarga'] = df['corrente'] > LIMITE_SOBRECARGA
    df['duracao_segundos'] = df['data_captura'].diff().dt.total_seconds()
    df['duracao_segundos'].fillna(df['duracao_segundos'].mean(), inplace=True)

    tempo_total = df['duracao_segundos'].sum()
    tempo_por_estado = df.groupby('estado_operacional')['duracao_segundos'].sum()
    perc_em_carga = (tempo_por_estado.get('Em Carga', 0) / tempo_total) * 100
    perc_ociosa = (tempo_por_estado.get('Ociosa', 0) / tempo_total) * 100
    perc_desligada = (tempo_por_estado.get('Desligada', 0) / tempo_total) * 100

    df['estado_mtbf'] = np.where(df['estado_operacional'] == 'Em Carga', 'UP', 'DOWN')
    df['mudou_estado'] = df['estado_mtbf'].shift() != df['estado_mtbf']
    df.loc[0, 'mudou_estado'] = True
    df['grupo'] = df['mudou_estado'].cumsum()

    duracao_eventos = df.groupby('grupo').agg(
        estado=('estado_mtbf', 'first'),
        duracao_total=('duracao_segundos', 'sum')
    )

    uptime = duracao_eventos[duracao_eventos['estado'] == 'UP']['duracao_total']
    downtime = duracao_eventos[duracao_eventos['estado'] == 'DOWN']['duracao_total']

    mtbf_minutos = (uptime.mean() / 60) if not uptime.empty else 0
    mttr_minutos = (downtime.mean() / 60) if not downtime.empty else 0

    total_uptime = uptime.sum()
    total_downtime = downtime.sum()

    confiabilidade_perc = (
        (total_uptime / (total_uptime + total_downtime)) * 100
        if (total_uptime + total_downtime) > 0
        else 100
    )

    df_carga = df[df['estado_operacional'] == 'Em Carga']
    carga_media_trabalho_amps = df_carga['corrente'].mean() if not df_carga.empty else 0
    total_eventos_sobrecarga = df['alerta_sobrecarga'].sum()

    df['carga_media_trabalho_amps'] = carga_media_trabalho_amps
    df['mtbf_minutos'] = mtbf_minutos
    df['mttr_minutos'] = mttr_minutos
    df['perc_tempo_desligada'] = perc_desligada
    df['perc_tempo_em_carga'] = perc_em_carga
    df['perc_tempo_ociosa'] = perc_ociosa
    df['confiabilidade_perc_oee'] = confiabilidade_perc
    df['total_eventos_sobrecarga'] = total_eventos_sobrecarga

    df.drop(columns=['duracao_segundos', 'estado_mtbf', 'mudou_estado', 'grupo'], inplace=True)
    
    print("✓ Corrente processada com sucesso.")
    return df

def tensao(df):
    return df

def temperatura(df):
    return df   

def vibracao(df):
    return df   

def pressao(df):
    print("→ Iniciando junção com reclamações...")
    
    try:
        # 1️⃣ Nome do bucket e arquivo
        bucket = CLIENT_BUCKET
        key = "client_reclamacoes_bruto.csv"

        # 2️⃣ Lê o CSV de reclamações do bucket client
        response = s3.get_object(Bucket=bucket, Key=key)
        raw_bytes = response['Body'].read()

        # Detecta encoding automaticamente
        try:
            csv_content = raw_bytes.decode('utf-8')
        except UnicodeDecodeError:
            csv_content = raw_bytes.decode('latin1')

        df_reclamacoes = pd.read_csv(io.StringIO(csv_content))

        # 3️⃣ Tratar a coluna 'created' — deixar só a data
        df_reclamacoes['created'] = pd.to_datetime(df_reclamacoes['created'], errors='coerce').dt.date.astype(str)

        # 4️⃣ Renomear todas as colunas (coloque os novos nomes aqui)
        df_reclamacoes.rename(columns={
            'title': 'reclamacao_titulo',
            'description': 'reclamacao_descricao',
            'userstate': 'reclamacao_estado',
            'usercity': 'reclamacao_cidade',
            'status': 'reclamacao_status',
            'created': 'dia_captura',   # já padroniza a data para o join
            'url': 'reclamacao_url'
        }, inplace=True)

        print(f"✓ Colunas renomeadas: {list(df_reclamacoes.columns)}")

        # 5️⃣ Fazer FULL OUTER JOIN (mantém todos os dias)
        df_flat = pd.merge(df, df_reclamacoes, on='dia_captura', how='outer')

        print(f"✓ Junção concluída. Total de registros combinados: {len(df_flat)}")

        # 6️⃣ Salvar no bucket client
        out = io.StringIO()
        df_flat.to_csv(out, index=False)
        s3.put_object(
            Bucket=bucket,
            Key='flat_table.csv',
            Body=out.getvalue().encode('utf-8'),
            ContentType='text/csv'
        )

        print("✓ Flat table salva no bucket client com sucesso.")
        return df_flat

    except Exception as e:
        print(f"function=pressao_error message={e}")
        return df



def frequencia(df):
    return df