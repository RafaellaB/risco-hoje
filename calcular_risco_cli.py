import os
import sys
import requests
import pandas as pd
import numpy as np
import glob
import re
<<<<<<< HEAD
from datetime import datetime
=======
from datetime import datetime, date, timedelta
>>>>>>> ff542cf2 (.)
from pytz import timezone
from io import StringIO

URL_ARQUIVO_HISTORICO = 'https://raw.githubusercontent.com/RafaellaB/Painel-Diagrama-de-Risco/main/resultado_risco_final.csv'
URL_ARQUIVO_MARE_AM = 'https://raw.githubusercontent.com/RafaellaB/Diagramas-de-risco-din-mico/main/tide/mare_calculada_hora_em_hora_ano-completo.csv'
NOME_ARQUIVO_SAIDA_FINAL = 'resultado_risco_final.csv'
CSV_DELIMITADOR = ','
ESTACOES_DESEJADAS = ["Campina do Barreto", "Torreão", "RECIFE - APAC", "Imbiribeira", "Dois Irmãos"]

<<<<<<< HEAD
def carregar_dados_mare(url_am_data):
    try:
        df_am_raw = pd.read_csv(url_am_data, sep=';', decimal=',')
        df_am_raw.rename(columns={'Hora_Exata': 'datahora', 'Altura_m': 'AM'}, inplace=True)
        if 'datahora' not in df_am_raw.columns:
            raise KeyError("Coluna 'Hora_Exata' não encontrada.")
=======
# --- 2. FUNÇÕES DE SUPORTE ---
def carregar_dados_mare(url_am_data):
    try:
        # Tenta ler com o formato novo do GitHub (sep=; e decimal=,)
        df_am_raw = pd.read_csv(url_am_data, sep=';', decimal=',')
        
        # Se as colunas não forem encontradas, tenta o formato padrão
        if 'Hora_Exata' not in df_am_raw.columns:
            df_am_raw = pd.read_csv(url_am_data)

        df_am_raw.rename(columns={'Hora_Exata': 'datahora', 'Altura_m': 'AM', 'altura': 'AM'}, inplace=True)
        
>>>>>>> ff542cf2 (.)
        df_am_raw['datahora'] = pd.to_datetime(df_am_raw['datahora'])
        df_am_raw['data'] = df_am_raw['datahora'].dt.strftime('%Y-%m-%d')
        df_am_raw['hora_ref'] = df_am_raw['datahora'].dt.strftime('%H:00:00')
        
        return df_am_raw[['data', 'hora_ref', 'AM']]
    except Exception as e:
        print(f"ERRO Maré: {e}", file=sys.stderr)
        return pd.DataFrame()

<<<<<<< HEAD
def processar_chuva_arquivo(df_chuva, data_alvo):
    df = df_chuva[df_chuva['nomeEstacao'].isin(ESTACOES_DESEJADAS)].copy()
    df['datahora'] = pd.to_datetime(df['datahora'])
    df['data_str'] = df['datahora'].dt.strftime('%Y-%m-%d')
    df = df[df['data_str'] == data_alvo]
=======
def processar_dados_chuva_simplificado(df_chuva, data_alvo, estacoes_desejadas):
    df = df_chuva[df_chuva['nomeEstacao'].isin(estacoes_desejadas)].copy()
    df['datahora'] = pd.to_datetime(df['datahora'])
    df['data_str'] = df['datahora'].dt.strftime('%Y-%m-%d')
    df = df[df['data_str'] == data_alvo]
    
>>>>>>> ff542cf2 (.)
    if df.empty: return pd.DataFrame()
    
    df = df.set_index('datahora').sort_index()
    resultados = []
    for estacao, grupo in df.groupby('nomeEstacao'):
        chuva_10min = grupo['valorMedida'].rolling('10min').sum()
        chuva_2h = grupo['valorMedida'].rolling('2h').sum()
<<<<<<< HEAD
        temp = pd.DataFrame({'chuva_10min': chuva_10min, 'chuva_2h': chuva_2h})
        agregado = temp.resample('h').last()
        agregado['VP'] = (agregado['chuva_10min'] * 6) + agregado['chuva_2h']
        agregado['nomeEstacao'] = estacao
        resultados.append(agregado)
=======
        temp_df = pd.DataFrame({'chuva_10min': chuva_10min, 'chuva_2h': chuva_2h})
        agregado = temp_df.resample('h').last()
        agregado['VP'] = (agregado['chuva_10min'] * 6) + agregado['chuva_2h']
        agregado['nomeEstacao'] = estacao
        resultados.append(agregado)
    
>>>>>>> ff542cf2 (.)
    df_vp = pd.concat(resultados).reset_index()
    df_vp['data'] = df_vp['datahora'].dt.strftime('%Y-%m-%d')
    df_vp['hora_ref'] = df_vp['datahora'].dt.strftime('%H:00:00')
    return df_vp[['data', 'hora_ref', 'nomeEstacao', 'VP']]

<<<<<<< HEAD
if __name__ == "__main__":
    print("Iniciando Nova Versão do Script de Risco (Varredura de Arquivos)...")
    
    df_am = carregar_dados_mare(URL_ARQUIVO_MARE_AM)
    if df_am.empty: 
        print("Erro: Maré vazia")
        sys.exit(1)

    arquivos_disponiveis = glob.glob("chuva_recife_*.csv")
    print(f"Arquivos encontrados na pasta: {arquivos_disponiveis}")

    lista_novos_dados = []

    for arq in arquivos_disponiveis:
        match = re.search(r'(\d{4}-\d{2}-\d{2})', arq)
        if not match: continue
        data_do_arquivo = match.group(1)
        
        try:
            print(f"-> Processando: {data_do_arquivo}")
            df_raw = pd.read_csv(arq, sep=CSV_DELIMITADOR)
            df_raw.rename(columns={'nome': 'nomeEstacao', 'valor': 'valorMedida'}, inplace=True)
            df_vp = processar_chuva_arquivo(df_raw, data_do_arquivo)
            if not df_vp.empty:
                df_mesclado = pd.merge(df_vp, df_am, on=['data', 'hora_ref'], how='left')
                df_mesclado['Nivel_Risco_Valor'] = (df_mesclado['VP'].astype(float) * df_mesclado['AM'].astype(float)).round(2)
                bins = [-np.inf, 30, 50, 100, np.inf]
                labels = ['Baixo', 'Moderado', 'Moderado Alto', 'Alto']
                df_mesclado['Classificacao_Risco'] = pd.cut(df_mesclado['Nivel_Risco_Valor'], bins=bins, labels=labels)
                lista_novos_dados.append(df_mesclado)
        except Exception as e:
            print(f"Erro no arquivo {arq}: {e}")

    if not lista_novos_dados:
        print("Aviso: Nenhum arquivo de chuva foi processado com sucesso.")
        sys.exit(0)

    df_total_novo = pd.concat(lista_novos_dados, ignore_index=True)
    
    try:
        res = requests.get(URL_ARQUIVO_HISTORICO)
        df_historico = pd.read_csv(StringIO(res.text)) if res.status_code == 200 else pd.DataFrame()
    except:
        df_historico = pd.DataFrame()

    df_final = pd.concat([df_historico, df_total_novo], ignore_index=True)
    df_final.drop_duplicates(subset=['data', 'hora_ref', 'nomeEstacao'], keep='last', inplace=True)
    df_final.sort_values(['data', 'hora_ref'], ascending=[False, False], inplace=True)
    df_final.to_csv(NOME_ARQUIVO_SAIDA_FINAL, index=False)
    print(f"✅ Finalizado com {len(df_final)} registros.")
=======
def calcular_risco(df_final):
    if df_final.empty: return pd.DataFrame()
    df_final['VP'] = pd.to_numeric(df_final['VP'], errors='coerce').fillna(0)
    df_final['AM'] = pd.to_numeric(df_final['AM'], errors='coerce').fillna(0)
    df_final['Nivel_Risco_Valor'] = (df_final['VP'] * df_final['AM']).round(2)
    bins = [-np.inf, 30, 50, 100, np.inf]
    labels = ['Baixo', 'Moderado', 'Moderado Alto', 'Alto']
    df_final['Classificacao_Risco'] = pd.cut(df_final['Nivel_Risco_Valor'], bins=bins, labels=labels, right=False)
    return df_final

# --- 3. EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    tz_recife = timezone('America/Recife') 
    print(f"--- Iniciando Processamento (Modo Varredura) ---")

    df_am = carregar_dados_mare(URL_ARQUIVO_MARE_AM)
    if df_am.empty: sys.exit(1)

    # Busca TODOS os arquivos de chuva na pasta
    arquivos_chuva = glob.glob("chuva_recife_*.csv")
    arquivos_chuva.sort() 
    
    print(f"🔎 Total de arquivos encontrados: {len(arquivos_chuva)}")

    lista_novos_dados = []

    for nome_arquivo in arquivos_chuva:
        match = re.search(r'(\d{4}-\d{2}-\d{2})', nome_arquivo)
        if not match: continue
        data_str = match.group(1)
        
        try:
            # Deteta separador automaticamente
            with open(nome_arquivo, 'r') as f:
                header = f.readline()
                sep = ';' if ';' in header else ','

            df_chuva_raw = pd.read_csv(nome_arquivo, sep=sep)
            df_chuva_raw.rename(columns={'nome': 'nomeEstacao', 'valor': 'valorMedida'}, inplace=True)
            
            df_vp = processar_dados_chuva_simplificado(df_chuva_raw, data_str, ESTACOES_DESEJADAS)
            
            if not df_vp.empty:
                df_dia = pd.merge(df_vp, df_am, on=['data', 'hora_ref'], how='left')
                df_risco = calcular_risco(df_dia)
                lista_novos_dados.append(df_risco)
                print(f"✅ Processado: {data_str}")
        except Exception as e:
            print(f"⚠️ Erro no arquivo {nome_arquivo}: {e}")

    if not lista_novos_dados:
        print("❌ Nada novo para processar.")
        sys.exit(0)

    # Consolidação
    df_novo_bloco = pd.concat(lista_novos_dados, ignore_index=True)
    
    try:
        response = requests.get(URL_ARQUIVO_HISTORICO)
        df_hist = pd.read_csv(StringIO(response.text)) if response.status_code == 200 else pd.DataFrame()
    except:
        df_hist = pd.DataFrame()

    df_final = pd.concat([df_hist, df_novo_bloco], ignore_index=True)
    df_final.drop_duplicates(subset=['data', 'hora_ref', 'nomeEstacao'], keep='last', inplace=True)
    df_final.sort_values(['data', 'hora_ref'], ascending=[False, False], inplace=True)

    df_final.to_csv(NOME_ARQUIVO_SAIDA_FINAL, index=False)
    print(f"✅ Sucesso! Total de linhas no histórico: {len(df_final)}")
>>>>>>> ff542cf2 (.)
