import json
import os

path = '/home/hspassos/mestrado/banking/modelo_estrutural_indices.ipynb'

# Helper para criar células
def code_cell(source):
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [line + '\n' for line in source.split('\n')]
    }

def md_cell(source):
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + '\n' for line in source.split('\n')]
    }

cells = []

cells.append(md_cell("# Modelo Estrutural de Negócios Bancários (Abordagem por Índices Clássicos)\nEsta versão constrói 6 a 7 índices financeiros da literatura de modelos de negócios para criar clusters mais bem separados e com forte apelo teórico."))

dados_bancos_code = """import pandas as pd
import sqlite3
import numpy as np

def dados_bancos():
    conn = sqlite3.connect('dados/banking.db')
    financial_indicators = ['ano', 'cnpj', 'NOME_INSTITUICAO', 'TAXONOMIA', 'CONTA', 'NOME_CONTA', 'saldo_medio']
    query = f"SELECT {', '.join(financial_indicators)} FROM balancetes_media_ano WHERE cnpj IN (SELECT cnpj FROM balancetes_media_ano GROUP BY ano, cnpj);"
    df = pd.read_sql_query(query, conn)
    conn.close()

    df = df.rename(columns={'NOME_INSTITUICAO': 'nome', 'TAXONOMIA': 'taxonomia', 'CONTA': 'conta', 'NOME_CONTA': 'nome_conta', 'saldo_medio': 'saldo'})

    colunas_para_limpar = ['nome_conta', 'taxonomia']
    for col in colunas_para_limpar:
        df[col] = df[col].str.lower().str.normalize('NFD').str.replace(r'[\\u0300-\\u036f]', '', regex=True)

    df.loc[df['conta'] == 10000007, 'nome_conta'] = "ativo realizavel"
    df.loc[df['conta'] == 13000004, 'nome_conta'] = "titulos e valores mobiliarios e instrumentos financeiros derivativos"
    df.loc[df['conta'] == 40000008, 'nome_conta'] = "passivo exigivel"
    
    # Removendo contas duplicadas
    df = df[~df['conta'].isin([88000008, 47100004, 51100007, 78100004, 61000001])]

    mapa_contas = df.groupby('conta')['nome_conta'].agg(lambda x: x.value_counts().index[0]).to_dict()
    novo_mapeamento = {}
    for conta, nome in mapa_contas.items():
        conta_str = str(conta)
        if conta_str.startswith('1'): novo_nome = f"ativo realizavel - {nome}"
        elif conta_str.startswith('2'): novo_nome = f"ativo permanente - {nome}"
        elif conta_str.startswith('3'): novo_nome = f"compensacao ativa - {nome}"
        elif conta_str.startswith('4'): novo_nome = f"passivo exigivel - {nome}"
        elif conta_str.startswith('6'): novo_nome = f"patrimonio liquido - {nome}"
        elif conta_str.startswith('7'): novo_nome = f"resultado credor - {nome}"
        elif conta_str.startswith('8'): novo_nome = f"resultado devedor - {nome}"
        elif conta_str.startswith('9'): novo_nome = f"compensacao passiva - {nome}"
        else: novo_nome = nome
        novo_mapeamento[conta] = novo_nome

    df = df.pivot(index=['ano', 'cnpj', 'nome', 'taxonomia'], columns='conta', values='saldo').reset_index()
    df['ano'] = df['ano'].astype(int)

    # Filtro Ativo / Crédito
    df = df[df[16000001] > 1000000]

    # Exclusões taxonômicas
    taxonomias_excluir = [
        'banco nacional de desenvolvimento economico social', 'bancos de desenvolvimento', 'agencias de fomento ou de desenvolvimento',
        'cooperativas de credito', 'soc. corretora de titulos e valores mobiliarios', 'soc distribuidora de titulos e valores mobiliarios',
        'sociedades de credito imobiliario', 'sociedade de credito imobiliario', 'companhias hipotecarias', 'associacao de poupanca e emprestimos',
        'sociedade de credito ao microempreendedor'
    ]
    df = df[~df['taxonomia'].isin(taxonomias_excluir)]

    colunas_para_inverter = [col for col in df.columns if col not in ['ano', 'cnpj', 'nome', 'taxonomia']]
    for col in colunas_para_inverter:
        if pd.notna(df[col].max()) and (df[col].max() <= 0) and (df[col].min() < 0):
            df[col] = df[col] * -1

    df.fillna(0, inplace=True)
    df_original = df.copy().rename(columns=novo_mapeamento)
    
    return df_original

df_original = dados_bancos()
"""
cells.append(code_cell(dados_bancos_code))

cells.append(md_cell("### Construção dos Índices Estruturais"))

indices_code = """def criar_indices_estruturais(df_orig):
    indices = df_orig[['ano', 'cnpj', 'nome', 'taxonomia']].copy()
    
    # Localizando a coluna de Ativo Total (39999993 pode estar como número ou string, mas renomeamos via mapa)
    ativo_col = 39999993 if 39999993 in df_orig.columns else 'compensacao ativa - total geral do ativo' # ou ativo realizavel
    # Vamos achar a conta de Ativo Total dinamicamente se o ID não existir
    if ativo_col not in df_orig.columns:
        ativo_col = [c for c in df_orig.columns if 'ativo' in str(c) and 'total' in str(c)]
        ativo_col = ativo_col[0] if ativo_col else 39999993
        
    # Se ainda assim não achar, pegamos o ativo pelo id
    try:
        ativo_total = df_orig[ativo_col]
    except KeyError:
        ativo_total = df_orig[39999993] # Fallback
    
    # 1. Foco em Crédito
    col_cred = [c for c in df_orig.columns if 'operacoes de credito' in str(c) and 'provisoes' not in str(c)]
    if col_cred: indices['idx_credito'] = df_orig[col_cred[0]] / ativo_total
    
    # 2. Foco em Tesouraria
    col_tes = [c for c in df_orig.columns if 'titulos e valores mobiliarios' in str(c) and 'ativo' in str(c)]
    if col_tes: indices['idx_tesouraria'] = df_orig[col_tes[0]] / ativo_total
        
    # 3. Liquidez
    cols_liq = [c for c in df_orig.columns if 'disponibilidade' in str(c) or 'aplicacoes interfinanceiras de liquidez' in str(c)]
    indices['idx_liquidez'] = df_orig[cols_liq].sum(axis=1) / ativo_total
    
    # 4. Funding de Varejo (Depósitos)
    cols_dep = [c for c in df_orig.columns if 'deposit' in str(c) and 'passivo' in str(c)]
    indices['idx_funding_varejo'] = df_orig[cols_dep].sum(axis=1) / ativo_total
    
    # 5. Dependência de Serviços
    cols_serv = [c for c in df_orig.columns if 'resultado credor' in str(c) and ('servico' in str(c) or 'tarifa' in str(c))]
    indices['idx_servicos'] = df_orig[cols_serv].sum(axis=1) / ativo_total
    
    # 6. Alavancagem (Proxy via Passivo)
    cols_pass = [c for c in df_orig.columns if 'passivo exigivel' in str(c)]
    passivo_total = df_orig[cols_pass].sum(axis=1)
    indices['idx_capitalizacao'] = 1 - (passivo_total / ativo_total)
    
    # 7. Risco de Crédito Retido
    cols_prov = [c for c in df_orig.columns if 'provisoes para operacoes de credito' in str(c)]
    if cols_prov and col_cred:
        indices['idx_risco_credito'] = df_orig[cols_prov[0]].abs() / df_orig[col_cred[0]].replace(0, np.nan)
        indices['idx_risco_credito'] = indices['idx_risco_credito'].fillna(0)
        
    indices.fillna(0, inplace=True)
    return indices

df_indices = criar_indices_estruturais(df_original)
print(df_indices.head())
"""
cells.append(code_cell(indices_code))

cells.append(md_cell("### Preparação, Winsorização e Z-Score"))

prep_code = """def preparar_dados_painel(df, coluna_tempo='ano', p_inf=0.01, p_sup=0.99):
    df_processado = df.copy()
    df_processado = df_processado.replace([np.inf, -np.inf], np.nan).fillna(0)
    colunas_financeiras = [col for col in df_processado.columns if col.startswith('idx_')]

    for col in colunas_financeiras:
        df_processado[col] = df_processado.groupby(coluna_tempo)[col].transform(
            lambda x: x.clip(lower=x.quantile(p_inf), upper=x.quantile(p_sup))
        )
        df_processado[col] = df_processado.groupby(coluna_tempo)[col].transform(
            lambda x: (x - x.mean()) / x.std() if x.std() > 0 else 0
        )
    return df_processado

X_raw = df_indices.copy()
X_raw['chave_r'] = X_raw['ano'].astype(str) + "_" + X_raw['cnpj'].astype(str)
X_raw = X_raw.set_index('chave_r')

X_scaled = preparar_dados_painel(X_raw)
"""
cells.append(code_cell(prep_code))

cells.append(code_cell("%load_ext rpy2.ipython"))

r_code = """%%R -i X_scaled -o df_clusters_finais -o resultado_final

library(clustrd)

colunas_id <- c("ano", "cnpj", "nome", "taxonomia")
dados_filtrados <- X_scaled[, !(colnames(X_scaled) %in% colunas_id)]
dados_filtrados <- as.matrix(dados_filtrados)

set.seed(77)

# Calibragem: Aqui podemos usar ndim=3 porque temos apenas 7 índices ultra-densos
calibragem <- tuneclus(data = dados_filtrados, 
                       nclusrange = 3:6, 
                       ndimrange = 2:4, 
                       method = "FKM", 
                       criterion = "asw", 
                       center = FALSE, scale = FALSE, nstart = 50)
print("================ TUNECLUS SUMMARY ================")
print(summary(calibragem))

# Definindo o modelo final (ajuste os valores nclus e ndim baseado no sumário acima!)
resultado_final <- cluspca(data = dados_filtrados, 
                           nclus = 4,   # Modifique de acordo com a silhueta
                           ndim = 3,    # Modifique de acordo com a silhueta
                           method = "FKM",
                           center = FALSE,
                           scale = FALSE,
                           nstart = 100)

plot(resultado_final)

df_clusters_finais <- data.frame(
  Cluster = resultado_final$cluster,
  Fator_1 = resultado_final$obscoord[,1],
  Fator_2 = resultado_final$obscoord[,2],
  Fator_3 = resultado_final$obscoord[,3], # Remova se usar ndim=2
  row.names = rownames(dados_filtrados)
)
"""
cells.append(code_cell(r_code))

python_plot_code = """import matplotlib.pyplot as plt
import seaborn as sns

# Visualizando em 3D
fig = plt.figure(figsize=(10, 8))
ax = fig.add_subplot(111, projection='3d')

df_clusters_finais['Cluster'] = df_clusters_finais['Cluster'].astype(str)
clusters = sorted(df_clusters_finais['Cluster'].unique())
paleta = sns.color_palette('tab10', n_colors=len(clusters))

for i, c in enumerate(clusters):
    subset = df_clusters_finais[df_clusters_finais['Cluster'] == c]
    ax.scatter(subset['Fator_1'], subset['Fator_2'], subset['Fator_3'], 
               label=f'Cluster {c}', color=paleta[i], s=60, edgecolor='w', alpha=0.8)

ax.set_title('Mapeamento 3D: Modelos de Negócio via Índices Estruturais')
ax.set_xlabel('Fator 1')
ax.set_ylabel('Fator 2')
ax.set_zlabel('Fator 3')
ax.legend()
plt.show()
"""
cells.append(code_cell(python_plot_code))

notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3"
        },
        "language_info": {
            "name": "python",
            "version": "3.8"
        }
    },
    "nbformat": 4,
    "nbformat_minor": 4
}

with open(path, 'w', encoding='utf-8') as f:
    json.dump(notebook, f, indent=1, ensure_ascii=False)
