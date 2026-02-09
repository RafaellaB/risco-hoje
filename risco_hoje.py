import pandas as pd
import requests
import numpy as np
from datetime import datetime, date
import pytz 
import streamlit as st 
import plotly.graph_objects as go 
from io import StringIO

# 1. AMBIENTE DOS ARQUIVOS
URL_BASE_CHUVAS = 'https://raw.githubusercontent.com/RafaellaB/risco-hoje/main/chuva_recife_' 
SUFIXO_ARQUIVO_CHUVAS = '.csv'
URL_ARQUIVO_MARE_AM = 'https://raw.githubusercontent.com/RafaellaB/risco-hoje/main/tide/mare_calculada_hora_em_hora_ano-completo.csv'
CSV_DELIMITADOR = ',' 
COLUNAS_NO_CSV_CHUVAS = ['datahora', 'nome', 'valor'] 

# DICIONÁRIO DE TRADUÇÕES
traducoes = {
    "Português": {
        "titulo_pagina": "Risco de Alagamentos - Recife",
        "btn_atualizar": "Atualizar Dados",
        "btn_historico": "Ver Histórico",
        "msg_aguardando": "Aguardando dados de hoje",
        "msg_erro": "Erro ao processar. Tente atualizar.",
        "header_grafico": "Diagrama de Risco",
        "eixo_x": "Chuva (mm)",
        "eixo_y": "Maré (m)",
        "tempo": "Hora",
        "risco": "Risco",
        "sigla_chuva": "VP",
        "sigla_mare": "AM"
    },
    "English": {
        "titulo_pagina": "Flood Risk - Recife",
        "btn_atualizar": "Update Data",
        "btn_historico": "View History",
        "msg_aguardando": "Waiting for today's data",
        "msg_erro": "Processing error. Please try updating.",
        "header_grafico": "Risk Diagram",
        "eixo_x": "Rainfall (mm)",
        "eixo_y": "Tide (m)",
        "tempo": "Time",
        "risco": "Risk",
        "sigla_chuva": "RVI",
        "sigla_mare": "THI"
    }
}

# 2. FUNÇÕES DE CACHE
@st.cache_data(show_spinner=False)
def carregar_dados_mare_cache(url_am_data):
    try:
        response = requests.get(url_am_data)
        if response.status_code != 200: return pd.DataFrame()
        linhas = [l for l in response.text.splitlines() if not l.startswith(('<<<<', '====', '>>>>')) and l.strip()]
        if not linhas: return pd.DataFrame()
        conteudo_limpo = "\n".join(linhas)
        separador = ';' if ';' in linhas[0] else ','
        df = pd.read_csv(StringIO(conteudo_limpo), sep=separador, decimal=',', encoding='utf-8')
        mapeamento = {'Hora_Exata': 'datahora', 'datahora': 'datahora', 'Altura_m': 'AM', 'altura': 'AM', 'AM': 'AM'}
        df = df.rename(columns=mapeamento)
        df['datahora'] = pd.to_datetime(df['datahora'].astype(str).str.split(';').str[0], errors='coerce')
        df = df.dropna(subset=['datahora'])
        df['data'] = df['datahora'].dt.strftime('%Y-%m-%d')
        df['hora_ref'] = df['datahora'].dt.strftime('%H:00:00')
        df['AM'] = pd.to_numeric(df['AM'].astype(str).str.replace(',', '.'), errors='coerce')
        return df[['data', 'hora_ref', 'AM']]
    except: return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False) 
def carregar_dados_chuva_cache(url_base, data_de_hoje_str, separador, colunas_csv):
    url_completa = f"{url_base}{data_de_hoje_str}{SUFIXO_ARQUIVO_CHUVAS}"
    try:
        df_chuva_raw = pd.read_csv(url_completa, encoding='utf-8', sep=separador)
        df_chuva_raw.rename(columns={'nome': 'nomeEstacao', 'valor': 'valorMedida'}, inplace=True)
        df_chuva_raw['datahora'] = pd.to_datetime(df_chuva_raw['datahora'])
        return df_chuva_raw
    except: return pd.DataFrame()

# 3. FUNÇÕES DE PROCESSAMENTO
def processar_dados_chuva_simplificado(df_chuva, datas_desejadas, estacoes_desejadas):
    df = df_chuva[df_chuva['nomeEstacao'].isin(estacoes_desejadas)].copy()
    df['data'] = df['datahora'].dt.date.astype(str)
    df = df[df['data'].isin(datas_desejadas)]
    if df.empty: return pd.DataFrame()
    df = df.set_index('datahora').sort_index()
    resultados_por_estacao = []
    for estacao, grupo in df.groupby('nomeEstacao'):
        chuva_10min = grupo['valorMedida'].rolling('10min').sum()
        chuva_2h = grupo['valorMedida'].rolling('2h').sum()
        temp_df = pd.DataFrame({'chuva_10min': chuva_10min, 'chuva_2h': chuva_2h})
        agregado_horario = temp_df.resample('h').last()
        agregado_horario['VP'] = (agregado_horario['chuva_10min'] * 6) + agregado_horario['chuva_2h']
        agregado_horario['nomeEstacao'] = estacao
        resultados_por_estacao.append(agregado_horario)
    df_vp = pd.concat(resultados_por_estacao).reset_index()
    df_vp['data'] = df_vp['datahora'].dt.strftime('%Y-%m-%d')
    df_vp['hora_ref'] = df_vp['datahora'].dt.strftime('%H:00:00')
    return df_vp

def gerar_diagramas(df_analisado, idioma):
    t = traducoes[idioma]
    mapa_de_cores = {'Alto': '#D32F2F', 'Moderado Alto': '#FFA500', 'Moderado': '#FFC107', 'Baixo': '#4CAF50'}
    
    for (data, estacao), grupo in df_analisado.groupby(['data', 'nomeEstacao']):
        st.subheader(f"{t['header_grafico']}: {estacao}")
        fig = go.Figure()
        
        lim_x = max(110, grupo['VP'].max() * 1.2 if not grupo.empty else 110)
        lim_y = 5 
        x_grid, y_grid = np.arange(0, lim_x, 1), np.linspace(0, lim_y, 100)
        z_grid = np.array([x * y for y in y_grid for x in x_grid]).reshape(len(y_grid), len(x_grid))
        
        fig.add_trace(go.Heatmap(x=x_grid, y=y_grid, z=z_grid, colorscale=[[0, "#90EE90"], [0.3, "#FFD700"], [0.5, "#FFA500"], [1.0, "#D32F2F"]], showscale=False, zmin=0, zmax=100, hoverinfo='none'))
        
        grupo = grupo.sort_values(by='hora_ref')
        fig.add_trace(go.Scatter(x=grupo['VP'], y=grupo['AM'], mode='lines', line=dict(color='black', width=1, dash='dash'), hoverinfo='none', showlegend=False))
        
        for _, ponto in grupo.iterrows():
            cor_ponto = mapa_de_cores.get(ponto['Classificacao_Risco'], 'black')
            fig.add_trace(go.Scatter(
                x=[ponto['VP']], y=[ponto['AM']], 
                mode='markers', 
                marker=dict(color=cor_ponto, size=10, line=dict(width=1, color='black')),
                hoverinfo='text',
                hovertext=f"<b>{t['tempo']}:</b> {ponto['hora_ref']}<br><b>{t['risco']}:</b> {ponto['Classificacao_Risco']}<br><b>{t['sigla_chuva']}:</b> {ponto['VP']:.2f}<br><b>{t['sigla_mare']}:</b> {ponto['AM']:.2f}",
                showlegend=False
            ))
        
        fig.update_layout(xaxis_title=t['eixo_x'], yaxis_title=t['eixo_y'], margin=dict(l=40, r=40, t=40, b=40))
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{data}_{estacao}")

# BLOCO PRINCIPAL
if __name__ == "__main__":
    st.set_page_config(page_title="Risco Recife Hoje", layout="wide")
    fuso = pytz.timezone('America/Recife') 
    data_hoje_str = datetime.now(fuso).strftime('%Y-%m-%d')

    # --- SIDEBAR (ORDEM VISUAL PADRONIZADA) ---
    idioma_sel = st.sidebar.radio("Idioma / Language", ["Português", "English"], horizontal=True, label_visibility="collapsed")
    t = traducoes[idioma_sel]
    
    st.sidebar.markdown("---")
    
    # 1. Botão Atualizar (Ajustado para o padrão Primary e sem ícones)
    if st.sidebar.button(t['btn_atualizar'], use_container_width=True, type="primary"):
        carregar_dados_chuva_cache.clear()
        st.rerun()

    # Espaçamento dinâmico para o rodapé
    st.sidebar.markdown("<br>"*12, unsafe_allow_html=True)
    st.sidebar.markdown("---")

    # 2. Botão Histórico (Padrão Primary)
    st.sidebar.link_button(t['btn_historico'], "https://painel-diagrama-de-risco-f5n2bwurkppdppawqhqkmz.streamlit.app/", use_container_width=True, type="primary")

    # --- CONTEÚDO PRINCIPAL ---
    st.title(t['titulo_pagina'])
    st.divider()

    try:
        df_am = carregar_dados_mare_cache(URL_ARQUIVO_MARE_AM)
        df_chuva_raw = carregar_dados_chuva_cache(URL_BASE_CHUVAS, data_hoje_str, CSV_DELIMITADOR, COLUNAS_NO_CSV_CHUVAS)
        
        if df_chuva_raw.empty or df_am.empty:
            st.info(f"{t['msg_aguardando']} ({datetime.now(fuso).strftime('%d/%m/%Y')}).")
        else:
            df_vp = processar_dados_chuva_simplificado(df_chuva_raw, [data_hoje_str], ["Campina do Barreto", "Torreão", "RECIFE - APAC", "Imbiribeira", "Dois Irmãos"])
            df_final = pd.merge(df_vp, df_am, on=['data', 'hora_ref'], how='left')
            
            df_final['Nivel_Risco_Valor'] = (df_final['VP'] * df_final['AM']).fillna(0)
            bins = [-np.inf, 30, 50, 100, np.inf]
            df_final['Classificacao_Risco'] = pd.cut(df_final['Nivel_Risco_Valor'], bins=bins, labels=['Baixo', 'Moderado', 'Moderado Alto', 'Alto'])
            
            gerar_diagramas(df_final, idioma_sel)
    except:
        st.error(t['msg_erro'])