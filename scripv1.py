import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt
import seaborn as sns
import os
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from supabase import create_client
from bcb import sgs

# Carregar variáveis do arquivo .env
load_dotenv()

# Configurações do Supabase
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("⚠️ AVISO: Variáveis de ambiente SUPABASE_URL e/ou SUPABASE_KEY não definidas.")
    exit(1)

# Inicializa o cliente do Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Configuração de visualização
def configurar_visualizacao():
    sns.set(style='whitegrid')
    plt.rcParams['figure.figsize'] = (14, 8)

configurar_visualizacao()

def obter_ultimo_registro_data(ticker):
    try:
        response = supabase.table('dados_historicos')\
            .select('data')\
            .eq('ticker', ticker)\
            .order('data', desc=True)\
            .limit(1)\
            .execute()
        
        if response.data:
            return response.data[0]['data']
        return None
    except Exception as e:
        print(f"⚠️ Erro ao consultar último registro para {ticker}: {str(e)}")
        return None

def processar_cdi():
    try:
        print("\n📡 Obtendo dados do CDI via Banco Central do Brasil...")

        ultima_data = obter_ultimo_registro_data('CDI')
        print(f"🔍 Última data no banco: {ultima_data}")

        data_inicial = datetime.strptime(ultima_data, '%Y-%m-%d') + timedelta(days=1) if ultima_data else datetime.now() - relativedelta(years=5)
        data_final = datetime.now()

        print(f"📅 Buscando dados entre {data_inicial.strftime('%Y-%m-%d')} e {data_final.strftime('%Y-%m-%d')}")

        if data_inicial.date() >= data_final.date():
            print(f"✅ Dados do CDI já estão atualizados até {ultima_data}")
            return None

        # Captura a resposta da API do Banco Central
        cdi_diario = sgs.get({'CDI': 12}, start=data_inicial.strftime('%Y-%m-%d'), end=data_final.strftime('%Y-%m-%d'))

        if cdi_diario is None or cdi_diario.empty:
            print("⚠️ API do Banco Central não retornou dados para o CDI.")
            return None

        print(f"✅ Obtidos {len(cdi_diario)} registros para o CDI.")

        # Processamento dos dados
        cdi_diario['CDI_Acumulado'] = (1 + cdi_diario['CDI'] / 100).cumprod()
        primeiro_valor = cdi_diario['CDI_Acumulado'].iloc[0]
        cdi_diario['CDI_Indice'] = cdi_diario['CDI_Acumulado'] / primeiro_valor * 100
        cdi_diario['Open'] = cdi_diario['CDI_Indice']
        cdi_diario['High'] = cdi_diario['CDI_Indice']
        cdi_diario['Low'] = cdi_diario['CDI_Indice']
        cdi_diario['Close'] = cdi_diario['CDI_Indice']
        cdi_diario['Volume'] = 0
        cdi_diario['Retorno_Diario'] = cdi_diario['Close'].pct_change() * 100
        cdi_diario['Peak'] = cdi_diario['Close'].cummax()
        cdi_diario['Drawdown'] = (cdi_diario['Close'] / cdi_diario['Peak'] - 1) * 100

        return cdi_diario

    except Exception as e:
        print(f"⚠️ Erro ao obter dados do CDI: {str(e)}")
        return None
def preparar_dados_historicos(dados, ticker, nome):
    try:
        dados = dados.reset_index()
        dados = dados.rename(columns={
            'Date': 'data', 'Open': 'abertura', 'High': 'maxima', 'Low': 'minima', 
            'Close': 'fechamento', 'Volume': 'volume', 'Retorno_Diario': 'retorno_diario',
            'Peak': 'pico', 'Drawdown': 'drawdown'
        })
        dados['ticker'] = ticker
        dados['nome_ativo'] = nome
        dados['data'] = dados['data'].astype(str)

        # ❌ Remover colunas que não pertencem à tabela `dados_historicos`
        colunas_validas = {
            'data', 'abertura', 'maxima', 'minima', 'fechamento', 
            'fechamento_ajustado', 'volume', 'retorno_diario', 'pico', 
            'drawdown', 'ticker', 'nome_ativo'
        }
        dados = dados[[col for col in dados.columns if col in colunas_validas]]

        # ✅ Converter NaN e NaT para None para evitar erro no Supabase
        dados = dados.replace({np.nan: None})

        return dados.to_dict('records')
    except Exception as e:
        print(f"⚠️ Erro ao preparar dados históricos: {str(e)}")
        return []

def inserir_dados_historicos(dados_historicos):
    if not dados_historicos:
        return False
    try:
        print("📊 Exemplo de dado antes da inserção:")
        print(json.dumps(dados_historicos[0], indent=2))  # Exibir estrutura

        tamanho_lote = 100
        for i in range(0, len(dados_historicos), tamanho_lote):
            lote = dados_historicos[i:i+tamanho_lote]
            supabase.table('dados_historicos').upsert(lote, on_conflict='ticker,data').execute()

        print("✅ Dados históricos inseridos com sucesso!")
        return True
    except Exception as e:
        print(f"⚠️ Erro ao inserir dados históricos: {str(e)}")
        return False

def atualizar_dados():
    print("\n🚀 Iniciando atualização de dados...")
    cdi_diario = processar_cdi()
    if cdi_diario is not None:
        dados_historicos_cdi = preparar_dados_historicos(cdi_diario, 'CDI', 'CDI')
        inserir_dados_historicos(dados_historicos_cdi)
    print("\n✅ Atualização concluída!")

if __name__ == "__main__":
    atualizar_dados()
