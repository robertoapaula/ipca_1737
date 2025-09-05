# Bibliotecas Necessárias
import pandas as pd
import requests
import json
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# Configurar o sistema de logging
# Nível INFO mostra o progresso, Nível ERROR mostra apenas falhas graves
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def processar(url: str, nome_arquivo_saida: str):
    # Função que gerencia todas as etapas do processamento.

    logging.info("-> Iniciando o Processamento...")

    # Tenta baixar os dados diretamente
    logging.info("-> Fazendo o Download...")
    json_data = baixar_dados(url)

    # Se não conseguiu, tenta via navegação
    if not json_data:
        logging.warning("Não foi possível obter o JSON via link direto. Tentando captura via navegador...")
        json_data = capturar_dados_navegando_site()

    # Se conseguiu de alguma forma, processa
    if json_data:
        logging.info("-> Transformando os dados em DataFrame tabular...")
        df = transformar_dados(json_data)

        logging.info("-> Salvando o DataFrame no formato Parquet...")
        salvar_parquet(df, nome_arquivo_saida)
    else:
        logging.error("Não foi possível capturar os dados mesmo navegando no site.")

    logging.info("-> Processamento Concluído.")


def baixar_dados(url: str) -> dict | None:
    # Faz requisição para a URL fornecida e retorna o conteúdo JSON.

    try:
        resposta = requests.get(url, timeout=30) # Adiciona um timeout de 30 segundos para a requisição
        resposta.raise_for_status() 
        return resposta.json()
    except requests.exceptions.HTTPError as e:
        logging.error(f"Erro HTTP ao obter dados da URL '{url}': {e}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro de requisição para a URL '{url}': {e}")
    except json.JSONDecodeError:
        logging.error(f"A resposta da URL '{url}' não é um JSON válido.")
    return None


def capturar_dados_navegando_site() -> dict | None:
    # Simula um usuário navegando no site para chegar ao JSON.
    # Pode incluir login, cliques em menus, download de arquivo, etc.
    
    try:
        # Configura o Chrome para rodar em modo headless (sem abrir janela)
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Caminho do chromedriver (ajuste conforme sua instalação)
        service = Service("/usr/local/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # URL inicial do site
        driver.get("https://sidra.ibge.gov.br/")
        logging.info("-> Carregando Site...")
        
        # Exemplo: esperar menu carregar e clicar
        # Ajuste os seletores CSS/XPath de acordo com o site real
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Tabelas"))
        ).click()
        logging.info("-> Clique no Menu Tabelas")
        
        # Espera a página carregar e clica na tabela específica
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "IPCA"))
        ).click()
        logging.info("-> Clique na Tabela IPCA")
        
        # Extrai o link final do JSON ou o conteúdo diretamente
        json_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "jsonData"))
        )
        json_text = json_element.text
        
        driver.quit()
        return json.loads(json_text)
    
    except Exception as e:
        logging.error(f"Erro ao capturar dados navegando no site: {e}")
        try:
            driver.quit()
        except:
            pass
        return None


def transformar_dados(json_data: dict) -> pd.DataFrame:
    # Transforma o dicionário JSON em um DataFrame tabular.

    if not isinstance(json_data, dict) or not json_data:
        logging.warning("O JSON não é um dicionário válido ou está vazio.")
        return pd.DataFrame()

    dados_processados = {
        'Tabela_Id': json_data.get('Id'),
        'Tabela_Nome': json_data.get('Nome'),
        'Fonte': json_data.get('Fonte'),
        'Notas': json_data.get('Notas'),
        'Pesquisa_Id': json_data.get('Pesquisa', {}).get('Id'),
        'Pesquisa_Nome': json_data.get('Pesquisa', {}).get('Nome'),
        'Pesquisa_UrlSidra': json_data.get('Pesquisa', {}).get('UrlSidra'),
        'Pesquisa_Temas': json_data.get('Pesquisa', {}).get('Temas')
    }

    # Converta as listas de dicionários para strings JSON para manter a estrutura
    dados_processados['Periodos_JSON'] = json.dumps(json_data.get('Periodos', {}).get('Periodos', []))
    dados_processados['Variaveis_JSON'] = json.dumps(json_data.get('Variaveis', []))
    
    # Cria um DataFrame de uma única linha
    df = pd.DataFrame([dados_processados])
    return df


def salvar_parquet(dataframe: pd.DataFrame, nome_arquivo: str):
    # Salva um DataFrame no formato Parquet.

    if dataframe.empty:
        logging.info("DataFrame vazio. Nada para salvar.")
        return

    try:
        dataframe.to_parquet(nome_arquivo, index=False)
        logging.info(f"-> Arquivo salvo com sucesso: '{nome_arquivo}'")
    except Exception as e:
        logging.error(f"Erro ao salvar arquivo Parquet '{nome_arquivo}': {e}")


def main():
    # URL para os metadados da tabela 1737 do IPCA
    url_ipca = "https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1"
    nome_arquivo_ipca = "ipca_metadados.parquet"
    
    processar(url_ipca, nome_arquivo_ipca)


if __name__ == "__main__":
    main()