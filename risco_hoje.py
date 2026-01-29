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
        if len(df.columns) == 1:
            col_nome = df.columns[0]
            if ';' in col_nome or ',' in col_nome:
                sep_manual = ';' if ';' in col_nome else ','
                temp = df[col_nome].astype(str).str.split(sep_manual, expand=True)
                df['datahora'] = temp[0]
                df['AM'] = temp[1]
        df['datahora'] = df['datahora'].astype(str).str.split(';').str[0].str.split(',').str[0]
        df['datahora'] = pd.to_datetime(df['datahora'], errors='coerce')
        df = df.dropna(subset=['datahora'])
        df['data'] = df['datahora'].dt.strftime('%Y-%m-%d')
        df['hora_ref'] = df['datahora'].dt.strftime('%H:00:00')
        df['AM'] = pd.to_numeric(df['AM'].astype(str).str.replace(',', '.'), errors='coerce')
        return df[['data', 'hora_ref', 'AM']]
    except Exception as e:
        st.error(f"Erro crítico no processamento da Maré: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False) 
def carregar_dados_chuva_cache(url_base, data_de_hoje_str, separador, colunas_csv):
    url_completa = f"{url_base}{data_de_hoje_str}{SUFIXO_ARQUIVO_CHUVAS}"
    try:
        df_chuva_raw = pd.read_csv(url_completa, encoding='utf-8', sep=separador)
        if not all(col in df_chuva_raw.columns for col in colunas_csv):
             return pd.DataFrame()
        df_chuva_raw.rename(columns={'nome': 'nomeEstacao', 'valor': 'valorMedida'}, inplace=True)
        df_chuva_raw['datahora'] = pd.to_datetime(df_chuva_raw['datahora'])
        return df_chuva_raw
    except:
        return pd.DataFrame()

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
    df_vp.dropna(subset=['VP'], inplace=True)
    df_vp['data'] = df_vp['datahora'].dt.strftime('%Y-%m-%d')
    df_vp['hora_ref'] = df_vp['datahora'].dt.strftime('%H:00:00')
    return df_vp[['data', 'hora_ref', 'nomeEstacao', 'VP']]

def calcular_risco(df_final):
    if df_final.empty: return pd.DataFrame()
    df_final['VP'] = pd.to_numeric(df_final['VP'], errors='coerce').round(2) 
    df_final['AM'] = pd.to_numeric(df_final['AM'], errors='coerce').round(2)
    df_final['Nivel_Risco_Valor'] = (df_final['VP'] * df_final['AM']).fillna(0).round(2)
    bins = [-np.inf, 30, 50, 100, np.inf]
    labels = ['Baixo', 'Moderado', 'Moderado Alto', 'Alto']
    df_final['Classificacao_Risco'] = pd.cut(df_final['Nivel_Risco_Valor'], bins=bins, labels=labels, right=False)
    return df_final

def executar_analise_risco_completa(df_vp_calculado, df_am):
    if df_vp_calculado.empty: return pd.DataFrame()
    df_final = pd.merge(df_vp_calculado, df_am, on=['data', 'hora_ref'], how='left')
    return calcular_risco(df_final)

def gerar_diagramas(df_analisado):
    mapa_de_cores = {'Alto': '#D32F2F', 'Moderado Alto': '#FFA500', 'Moderado': '#FFC107', 'Baixo': '#4CAF50'}
    definicoes_risco = {'Baixo': 'RA < 30', 'Moderado': '30 ≤ RA < 50', 'Moderado Alto': '50 ≤ RA < 100', 'Alto': 'RA ≥ 100'}
    for (data, estacao), grupo in df_analisado.groupby(['data', 'nomeEstacao']):
        st.subheader(f"Diagrama de Risco: {estacao} - {pd.to_datetime(data).strftime('%d/%m/%Y')}")
        fig = go.Figure()
        lim_x = max(110, grupo['VP'].max() * 1.2 if not grupo.empty else 110)
        lim_y = 5 
        x_grid, y_grid = np.arange(0, lim_x, 1), np.linspace(0, lim_y, 100)
        z_grid = np.array([x * y for y in y_grid for x in x_grid]).reshape(len(y_grid), len(x_grid))
        colorscale = [[0, "#90EE90"], [30/100, "#FFD700"], [50/100, "#FFA500"], [1.0, "#D32F2F"]]
        fig.add_trace(go.Heatmap(x=x_grid, y=y_grid, z=z_grid, colorscale=colorscale, showscale=False, zmin=0, zmax=100, hoverinfo='none'))
        grupo = grupo.sort_values(by='hora_ref')
        fig.add_trace(go.Scatter(x=grupo['VP'], y=grupo['AM'], mode='lines', line=dict(color='black', width=1.5, dash='dash'), hoverinfo='none', showlegend=False))
        for _, ponto in grupo.iterrows():
            cor_ponto = mapa_de_cores.get(ponto['Classificacao_Risco'], 'black')
            fig.add_trace(go.Scatter(x=[ponto['VP']], y=[ponto['AM']], mode='markers', marker=dict(color=cor_ponto, size=12, line=dict(width=1, color='black')), 
                                     hoverinfo='text', 
                                     hovertext=f"<b>Hora:</b> {ponto['hora_ref']}<br><b>Risco:</b> {ponto['Classificacao_Risco']} ({ponto['Nivel_Risco_Valor']})<br><b>VP:</b> {ponto['VP']}<br><b>AM:</b> {ponto['AM']}", 
                                     showlegend=False))
        for risco, definicao in definicoes_risco.items():
            fig.add_trace(go.Scatter(x=[None], y=[None], mode='markers', marker=dict(color=mapa_de_cores[risco], size=10, symbol='square'), name=f"<b>{risco}</b>: {definicao}"))
        fig.update_layout(title=f'<b>{estacao}</b>', xaxis_title='Índice de Precipitação (mm)', yaxis_title='Índice de Altura da Maré (m)', margin=dict(l=40, r=40, t=40, b=40), showlegend=True, legend_title_text='<b>Níveis de Risco</b>')
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{data}_{estacao}")


if __name__ == "__main__":
    st.set_page_config(page_title="Risco de Alagamento - Recife", layout="wide")
    
    fuso_horario_referencia = pytz.timezone('America/Recife') 
    data_hoje = datetime.now(fuso_horario_referencia).date()
    data_hoje_str = data_hoje.strftime('%Y-%m-%d')
    
    st.title("Risco de Alagamento na Cidade do Recife - Hoje")

    # CSS PARA BOTÕES CENTRALIZADOS E PADRONIZADOS
    st.markdown("""
    <style>
    div.stButton > button, div.stLinkButton > a {
        background-color: #4F8BF9 !important;
        color: white !important;
        border-radius: 8px !important;
        border: none !important;
        width: 280px !important;
        height: 48px !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        text-decoration: none !important;
        margin: auto !important;
        font-weight: 500 !important;
        font-size: 16px !important;
    }
    div.stButton > button:hover, div.stLinkButton > a:hover {
        background-color: #3A6FCC !important;
        box-shadow: 0px 4px 12px rgba(0,0,0,0.15) !important;
    }
    div.stLinkButton > a > div { color: white !important; }
    </style>
    """, unsafe_allow_html=True)

    # Colunas para centralizar os botões
    m_esq, col1, col2, m_dir = st.columns([1, 2, 2, 1])
    
    with col1:
        if st.button("Atualizar Dados"):
            carregar_dados_chuva_cache.clear()
            st.rerun() 
    
    with col2:
        st.link_button("Ver Histórico dos Diagramas de Risco", "https://painel-diagrama-de-risco-f5n2bwurkppdppawqhqkmz.streamlit.app/")

    st.divider()

    try:
        df_am = carregar_dados_mare_cache(URL_ARQUIVO_MARE_AM)
        df_chuva_raw = carregar_dados_chuva_cache(URL_BASE_CHUVAS, data_hoje_str, CSV_DELIMITADOR, COLUNAS_NO_CSV_CHUVAS)
    except Exception as e:
        st.error("Erro ao carregar dados. Verifique a conexão.")
        st.stop() 

    if df_chuva_raw.empty or df_am.empty:
        st.warning(f"Aguardando os primeiros dados de chuva para o dia de hoje ({pd.to_datetime(data_hoje_str).strftime('%d/%m/%Y')}).")
    else:
        df_vp_calculado = processar_dados_chuva_simplificado(df_chuva_raw, [data_hoje_str], ["Campina do Barreto", "Torreão", "RECIFE - APAC", "Imbiribeira", "Dois Irmãos"])
        df_risco_final = executar_analise_risco_completa(df_vp_calculado, df_am)
        
        if not df_risco_final.empty:
           
            gerar_diagramas(df_risco_final)
            with st.expander("Ver Tabela de Risco Detalhada"):
                 st.dataframe(df_risco_final[['data', 'hora_ref', 'nomeEstacao', 'VP', 'AM', 'Nivel_Risco_Valor', 'Classificacao_Risco']])