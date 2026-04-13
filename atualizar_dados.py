import os
import sys
import pandas as pd
import requests
from datetime import datetime, timedelta 
from pytz import timezone

def obter_token(email, senha):
    """Obtém o token de autenticação da API do CEMADEN."""
    if not email or not senha:
        print("ERRO: Credenciais do Cemaden (email/senha) não encontradas nos segredos.", file=sys.stderr)
        sys.exit(1)
    try:
        token_url = 'https://sgaa.cemaden.gov.br/SGAA/rest/controle-token/tokens'
        login = {'email': email, 'password': senha}
        print("Tentando obter o token de acesso...")
        response = requests.post(token_url, json=login)
        response.raise_for_status()
        content = response.json()
        token = content.get('token')
        if token:
            print("✅ Token obtido com sucesso!")
            return token
        else:
            print("❌ Erro: A resposta da API não continha um token.", file=sys.stderr)
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao obter token: {e}", file=sys.stderr)
        return None

def buscar_dados_cemaden(token, lista_estacoes, uf='PE', rede='11', sensor='10'):
    """Busca os dados e converte os horários para o fuso local de Recife."""
    if not token:
        print("❌ Token de acesso não fornecido.", file=sys.stderr)
        return pd.DataFrame()
    
    url_base = 'https://sws.cemaden.gov.br/PED/rest/pcds/pcds-dados-recentes'
    headers = {'token': token}
    lista_dfs = []
    
    for codestacao in lista_estacoes:
        params = {'codestacao': codestacao, 'uf': uf, 'rede': rede, 'sensor': sensor, 'formato': 'JSON'}
        try:
            response = requests.get(url_base, headers=headers, params=params)
            response.raise_for_status()
            dados = response.json()
            if isinstance(dados, dict) and 'Nenhum resultado foi encontrado' in dados.get('Info', ''):
                continue
            if dados:
                dados_para_df = [dados] if isinstance(dados, dict) else dados
                lista_dfs.append(pd.DataFrame(dados_para_df))
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na estação {codestacao}: {e}", file=sys.stderr)
            
    if not lista_dfs: return pd.DataFrame()
    
    df_final = pd.concat(lista_dfs, ignore_index=True)

    if not df_final.empty and 'datahora' in df_final.columns:
        df_final['datahora'] = pd.to_datetime(df_final['datahora'])
        df_final['datahora'] = df_final['datahora'].dt.tz_localize('UTC').dt.tz_convert('America/Recife')
        df_final['datahora'] = df_final['datahora'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return df_final

def atualizar_csv_diario(df_novos_dados, nome_arquivo):
    """Combina dados novos com existentes e remove duplicatas."""
    if os.path.exists(nome_arquivo):
        try:
            df_existente = pd.read_csv(nome_arquivo)
            df_combinado = pd.concat([df_existente, df_novos_dados], ignore_index=True)
        except pd.errors.EmptyDataError:
            df_combinado = df_novos_dados
    else:
        df_combinado = df_novos_dados
    
    df_final = df_combinado.drop_duplicates(subset=['codestacao', 'datahora'], keep='last')
    df_final.to_csv(nome_arquivo, index=False)
    print(f"✅ Arquivo '{nome_arquivo}' atualizado. Total: {len(df_final)} registros.")


def main():
    """
    Orquestra o processo buscando dados para Hoje (Tempo Real) 
    e Ontem (Consolidação do Histórico).
    """
    cemaden_email = os.getenv("CEMADEN_EMAIL")
    cemaden_senha = os.getenv("CEMADEN_PASS")
    
    token_acesso = obter_token(cemaden_email, cemaden_senha)
    
    if token_acesso:
        estacoes_de_recife = [
            '261160614A', '261160609A', '261160623A', '261160618A', '261160603A'
        ]
        
        df_chuva_recente = buscar_dados_cemaden(token_acesso, estacoes_de_recife)

        if not df_chuva_recente.empty:
            tz_recife = timezone('America/Recife')
            agora = datetime.now(tz_recife)
            
            # Criar coluna temporária para filtro de datas
            df_chuva_recente['data_temp'] = pd.to_datetime(df_chuva_recente['datahora']).dt.strftime('%Y-%m-%d')

            # --- 1. LÓGICA DE HOJE (Tempo Real) ---
            data_hoje = agora.strftime('%Y-%m-%d')
            nome_hoje = f"chuva_recife_{data_hoje}.csv"
            df_hoje = df_chuva_recente[df_chuva_recente['data_temp'] == data_hoje].copy()
            
            if not df_hoje.empty:
                print(f"Atualizando dados de HOJE ({data_hoje})...")
                df_hoje.drop(columns=['data_temp'], inplace=True)
                atualizar_csv_diario(df_hoje, nome_hoje)

            # --- 2. LÓGICA DE ONTEM (Consolidação D-1) ---
            data_ontem = (agora - timedelta(days=1)).strftime('%Y-%m-%d')
            nome_ontem = f"chuva_recife_{data_ontem}.csv"
            df_ontem = df_chuva_recente[df_chuva_recente['data_temp'] == data_ontem].copy()
            
            if not df_ontem.empty:
                print(f"Consolidando dados de ONTEM ({data_ontem})...")
                df_ontem.drop(columns=['data_temp'], inplace=True)
                atualizar_csv_diario(df_ontem, nome_ontem)
            
            print("🚀 Processamento de datas concluído.")
        else:
            print("Nenhum dado retornado pela API nas últimas horas.")
    else:
        print("Falha na autenticação.")

if __name__ == "__main__":
    main()