import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time

def extrair_dados_historico_taxas_juros():
    base_url = "https://www.bcb.gov.br/estatisticas/reporttxjuroshistorico/"
    params = {
        "codigoSegmento": "1",
        "codigoModalidade": "402101",
        "tipoModalidade": "D",
        "InicioPeriodo": "1994-07-01",  # Data inicial mais antiga disponível
        "FimPeriodo": datetime.now().strftime("%Y-%m-%d")  # Data atual
    }
    
    all_data = []
    page = 1
    max_pages = 1000  # Limite de segurança
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    while page <= max_pages:
        params["historicotaxajurosdiario_page"] = page
        print(f"Extraindo página {page}...")
        
        try:
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', {'class': 'table table-striped'})
            
            if not table:
                print("Nenhuma tabela encontrada. Finalizando.")
                break
                
            rows = table.find_all('tr')
            
            # Pular o cabeçalho
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    data = {
                        'data': cols[0].text.strip(),
                        'taxa': cols[1].text.strip().replace(',', '.')
                    }
                    all_data.append(data)
            
            # Verificar se há próxima página
            next_button = soup.find('a', {'class': 'next'})
            if not next_button or 'disabled' in next_button.get('class', []):
                break
                
            page += 1
            time.sleep(1)  # Delay para evitar sobrecarregar o servidor
            
        except Exception as e:
            print(f"Erro ao extrair página {page}: {str(e)}")
            break
    
    # Converter para DataFrame
    df = pd.DataFrame(all_data)
    
    # Converter tipos de dados
    df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
    df['taxa'] = df['taxa'].astype(float)
    
    return df

# Executar a extração
dados_taxas = extrair_dados_historico_taxas_juros()

# Salvar em CSV
dados_taxas.to_csv('taxas_juros_historico_bcb.csv', index=False)

print("Extração concluída. Dados salvos em 'taxas_juros_historico_bcb.csv'")
print(dados_taxas.head())
