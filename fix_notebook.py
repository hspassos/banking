import json

path = '/home/hspassos/mestrado/banking/modelo_estrutural.ipynb'

with open(path, 'r', encoding='utf-8') as f:
    nb = json.load(f)

for c in nb.get('cells', []):
    if c.get('cell_type') != 'code':
        continue
    source_lines = c['source']
    source_text = ''.join(source_lines)
    
    # FIX 1: Sparsity Limiar
    if 'limiar_corte = 0.99' in source_text and 'eliminar_colunas_esparsas' in source_text:
        new_source = source_text.replace('limiar_corte = 0.99', 'limiar_corte = 0.70\n    # limiar_corte = 0.99 (Alterado)')
        c['source'] = [line + '\n' for line in new_source.split('\n')]
        # fixing trailing newlines
        if not source_text.endswith('\n'):
            c['source'][-1] = c['source'][-1].rstrip('\n')
            
    # FIX 2 & 3: Reorder Winsorization and Redundancy, and prevent Double-Winsorization
    # We will look for the "remover_colunas_redundantes" function cell
    if 'def remover_colunas_redundantes(df_filtrado):' in source_text:
        new_source = """# Análise de Correlação para Identificar Variáveis Redundantes (Corrigido)
import numpy as np
import seaborn as sns

def remover_colunas_redundantes(df_filtrado):
    X = df_filtrado.copy()
    X['chave_r'] = X['ano'].astype(str) + "_" + X['cnpj'].astype(str)    
    X = X.set_index('chave_r')    
    X = X.drop(columns=['cnpj', 'nome', 'taxonomia'], axis=1)
    
    # 1. CORREÇÃO: Fazer a winsorização ANTES da correlação
    # Preenchemos infinitos e aplicamos winsorização p/ que outliers não inflem o Pearson
    X_winsorizado = X.copy()
    X_winsorizado = X_winsorizado.replace([np.inf, -np.inf], np.nan).fillna(0)
    colunas_financeiras = [col for col in X_winsorizado.columns if col not in ['ano']]
    
    for col in colunas_financeiras:
        X_winsorizado[col] = X_winsorizado.groupby('ano')[col].transform(
            lambda x: x.clip(lower=x.quantile(0.01), upper=x.quantile(0.99))
        )
    
    # 2. Correlação (agora nos dados limpos de outliers)
    corr_matrix = X_winsorizado.corr(method='pearson')
    corr_matrix_abs = corr_matrix.abs()
    upper = corr_matrix_abs.where(np.triu(np.ones(corr_matrix_abs.shape), k=1).astype(bool))
    
    threshold = 0.85
    to_drop = [column for column in upper.columns if any(upper[column] > threshold)]

    print(f"\\n--- Contas Redundantes a Descartar (Correlação > {threshold}) ---")
    print(f"Número de variáveis sugeridas para remoção: {len(to_drop)}")
    
    df_filtrado.drop(columns=to_drop, inplace=True)
    X.drop(columns=to_drop, inplace=True)
    X.columns = X.columns.astype(str)
    
    # Guardamos X cru (sem Z-score) para uso futuro (evitar dupla padronização)
    return X, df_filtrado, to_drop

X_raw, df_filtrado, var_redundantes = remover_colunas_redundantes(df_filtrado)
variaveis = pd.DataFrame({'Contas': [col for col in X_raw.columns]})
"""
        c['source'] = [line + '\n' for line in new_source.split('\n')]
        c['source'][-1] = c['source'][-1].rstrip('\n')

    # Now fix the preparar_dados_painel cell which follows
    if 'def preparar_dados_painel(' in source_text and 'X_scaled =' in source_text:
        new_source = """# Normalização dos dados

def preparar_dados_painel(df, coluna_tempo='ano', p_inf=0.01, p_sup=0.99):
    df_processado = df.copy()
    df_processado = df_processado.replace([np.inf, -np.inf], np.nan).fillna(0)
    colunas_financeiras = [col for col in df_processado.columns if col not in ['ano']]

    for col in colunas_financeiras:
        # A: Winsorização Ano a Ano
        df_processado[col] = df_processado.groupby(coluna_tempo)[col].transform(
            lambda x: x.clip(lower=x.quantile(p_inf), upper=x.quantile(p_sup))
        )
        
        # B: Z-Score Ano a Ano
        df_processado[col] = df_processado.groupby(coluna_tempo)[col].transform(
            lambda x: (x - x.mean()) / x.std() if x.std() > 0 else 0
        )
        
    df_processado = df_processado.fillna(0)
    return df_processado

# X_raw já não possui variáveis redundantes.
X_scaled = preparar_dados_painel(X_raw, coluna_tempo='ano', p_inf=0.01, p_sup=0.99)
"""
        c['source'] = [line + '\n' for line in new_source.split('\n')]
        c['source'][-1] = c['source'][-1].rstrip('\n')

    # Fix the ajuste_outliers cell to avoid double standardization
    if 'def ajuste_outliers(df_resultados_r):' in source_text and 'X_scaled_limpa = preparar_dados_painel(X_scaled_limpa' in source_text:
        new_source = """def ajuste_outliers(df_resultados_r):
    df_resultados_r['cnpj'] = df_resultados_r.index.str.split('_').str[1]
    taxa_outlier = df_resultados_r.groupby('cnpj')['Mahalanobis_Outlier'].mean()

    cnpjs_persistentes = taxa_outlier[taxa_outlier > 0.3].index
    print(f"Bancos excluídos permanentemente da amostra: {len(cnpjs_persistentes)}")

    # CORREÇÃO: Remover outliers do X_raw (base crua) e DEPOIS padronizar de forma única!
    X_raw['cnpj'] = X_raw.index.str.split('_').str[1]
    X_raw_limpa = X_raw[~X_raw['cnpj'].isin(cnpjs_persistentes)].copy()
    X_raw_limpa = X_raw_limpa.drop(columns=['cnpj'])
    
    # Remove também do X_raw original para não acumular sujeira
    X_raw.drop(columns=['cnpj'], inplace=True, errors='ignore')
    
    return X_raw_limpa, taxa_outlier

X_raw_limpa, taxa_outlier = ajuste_outliers(df_resultados_r)

# Agora sim, padronizamos a base definitivamente (sem amputar dados duas vezes)
X_scaled_limpa = preparar_dados_painel(X_raw_limpa, coluna_tempo='ano', p_inf=0.01, p_sup=0.99)
"""
        c['source'] = [line + '\n' for line in new_source.split('\n')]
        c['source'][-1] = c['source'][-1].rstrip('\n')


with open(path, 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)
