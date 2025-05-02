import pandas as pd
import numpy as np
from datetime import datetime
from supabase import create_client
import os
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

# Configurações do Supabase
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# Verificar se as variáveis de ambiente estão definidas
if not SUPABASE_URL or not SUPABASE_KEY:
    print("\n⚠️ AVISO: Variáveis de ambiente SUPABASE_URL e/ou SUPABASE_KEY não definidas.")
    # Fallback para valores hardcoded (apenas para desenvolvimento)
    SUPABASE_URL = 'https://dxwebxduuazebqtkumtv.supabase.co'
    SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR4d2VieGR1dWF6ZWJxdGt1bXR2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDE1OTMxMzcsImV4cCI6MjA1NzE2OTEzN30.v53W6iz_BJup66qst03jWqjHzJ0DGKmUC6WrVGLpt-Y'

# Inicializa o cliente do Supabase
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"⚠️ Erro ao conectar com o Supabase: {str(e)}")
    exit(1)

def obter_dados_historicos(ticker, periodo_anos=5):
    """
    Obtém os dados históricos de um ativo do banco de dados
    
    Args:
        ticker (str): O ticker do ativo
        periodo_anos (int): Período em anos para busca (padrão: 5)
    
    Returns:
        pandas.DataFrame: DataFrame com os dados históricos ou None em caso de erro
    """
    try:
        # Calcular data inicial
        data_hoje = datetime.now()
        data_inicial = data_hoje.replace(year=data_hoje.year - periodo_anos).strftime('%Y-%m-%d')
        
        # Buscar dados no banco
        response = supabase.table('dados_historicos') \
            .select('*') \
            .eq('ticker', ticker) \
            .gte('data', data_inicial) \
            .order('data', desc=False) \
            .execute()
        
        if response.data and len(response.data) > 0:
            df = pd.DataFrame(response.data)
            
            # Converter tipos de dados
            df['data'] = pd.to_datetime(df['data'])
            df.set_index('data', inplace=True)
            
            # Garantir ordenação por data
            df = df.sort_index()
            
            return df
        else:
            print(f"Nenhum dado encontrado para {ticker}")
            return None
    except Exception as e:
        print(f"⚠️ Erro ao obter dados históricos de {ticker}: {str(e)}")
        return None

def calcular_retorno_acumulado(ticker, periodo_anos=5):
    """
    Calcula o retorno acumulado para um ativo
    
    Args:
        ticker (str): O ticker do ativo
        periodo_anos (int): Período em anos para cálculo (padrão: 5)
    
    Returns:
        float: Retorno acumulado em percentual ou None em caso de erro
    """
    dados = obter_dados_historicos(ticker, periodo_anos)
    
    if dados is None or dados.empty:
        return None
    
    try:
        preco_inicial = dados['fechamento'].iloc[0]  
        preco_final = dados['fechamento'].iloc[-1]   
        
        retorno_acumulado = ((preco_final / preco_inicial) - 1) * 100
        return round(retorno_acumulado, 2)
    except Exception as e:
        print(f"⚠️ Erro ao calcular retorno acumulado para {ticker}: {str(e)}")
        return None

def calcular_retorno_anualizado(ticker, periodo_anos=5):
    """
    Calcula o retorno anualizado para um ativo
    
    Args:
        ticker (str): O ticker do ativo
        periodo_anos (int): Período em anos para cálculo (padrão: 5)
    
    Returns:
        float: Retorno anualizado em percentual ou None em caso de erro
    """
    dados = obter_dados_historicos(ticker, periodo_anos)
    
    if dados is None or dados.empty:
        return None
    
    try:
        preco_inicial = dados['fechamento'].iloc[0]  # Changed from 'fechamento_ajustado'
        preco_final = dados['fechamento'].iloc[-1]   # Changed from 'fechamento_ajustado'
        
        # Calcular o número de anos decorridos
        dias_totais = (dados.index[-1] - dados.index[0]).days
        anos = dias_totais / 365.25
        
        # Retorno anualizado
        retorno_anualizado = ((preco_final / preco_inicial) ** (1 / max(anos, 0.01)) - 1) * 100
        return round(retorno_anualizado, 2)
    except Exception as e:
        print(f"⚠️ Erro ao calcular retorno anualizado para {ticker}: {str(e)}")
        return None

def calcular_volatilidade(ticker, periodo_anos=5):
    """
    Calcula a volatilidade anualizada para um ativo
    
    Args:
        ticker (str): O ticker do ativo
        periodo_anos (int): Período em anos para cálculo (padrão: 5)
    
    Returns:
        float: Volatilidade anualizada em percentual ou None em caso de erro
    """
    dados = obter_dados_historicos(ticker, periodo_anos)
    
    if dados is None or dados.empty:
        return None
    
    try:
        # Calcular retornos diários se não existirem
        if 'retorno_diario' not in dados.columns:
            dados['retorno_diario'] = dados['fechamento'].pct_change() * 100
        
        # Calcular volatilidade anualizada (desvio padrão dos retornos diários * raiz de 252)
        volatilidade = dados['retorno_diario'].std() * np.sqrt(252)
        return round(volatilidade, 2)
    except Exception as e:
        print(f"⚠️ Erro ao calcular volatilidade para {ticker}: {str(e)}")
        return None

def calcular_max_drawdown(ticker, periodo_anos=5):
    """
    Calcula o máximo drawdown para um ativo
    
    Args:
        ticker (str): O ticker do ativo
        periodo_anos (int): Período em anos para cálculo (padrão: 5)
    
    Returns:
        float: Máximo drawdown em percentual ou None em caso de erro
    """
    dados = obter_dados_historicos(ticker, periodo_anos)
    
    if dados is None or dados.empty:
        return None
    
    try:
        # Calcular pico e drawdown
        dados['pico'] = dados['fechamento'].cummax()  # Changed from 'fechamento_ajustado'
        dados['drawdown'] = (dados['fechamento'] / dados['pico'] - 1) * 100  # Changed from 'fechamento_ajustado'
        
        # Obter o mínimo drawdown (valor mais negativo)
        max_drawdown = dados['drawdown'].min()
        return round(max_drawdown, 2)
    except Exception as e:
        print(f"⚠️ Erro ao calcular máximo drawdown para {ticker}: {str(e)}")
        return None

def calcular_sharpe(ticker, periodo_anos=5, taxa_livre_risco=None):
    """
    Calcula o índice de Sharpe para um ativo
    
    Args:
        ticker (str): O ticker do ativo
        periodo_anos (int): Período em anos para cálculo (padrão: 5)
        taxa_livre_risco (float): Taxa livre de risco anualizada (padrão: CDI)
    
    Returns:
        float: Índice de Sharpe ou None em caso de erro
    """
    # Obter retorno anualizado e volatilidade
    retorno_anualizado = calcular_retorno_anualizado(ticker, periodo_anos)
    volatilidade = calcular_volatilidade(ticker, periodo_anos)
    
    if retorno_anualizado is None or volatilidade is None or volatilidade == 0:
        return None
    
    try:
        # Se a taxa livre de risco não for fornecida, usar o CDI
        if taxa_livre_risco is None:
            # Tentar obter o retorno anualizado do CDI para o mesmo período
            taxa_livre_risco = calcular_retorno_anualizado('CDI', periodo_anos) or 0
        
        # Calcular o índice de Sharpe
        sharpe = (retorno_anualizado - taxa_livre_risco) / volatilidade
        return round(sharpe, 2)
    except Exception as e:
        print(f"⚠️ Erro ao calcular índice de Sharpe para {ticker}: {str(e)}")
        return None

# Função para obter o resumo completo de um ativo
def obter_resumo_ativo(ticker, periodo_anos=5):
    """
    Obtém o resumo completo de um ativo, incluindo todos os indicadores
    
    Args:
        ticker (str): O ticker do ativo
        periodo_anos (int): Período em anos para cálculo (padrão: 5)
    
    Returns:
        dict: Dicionário com todos os indicadores ou None em caso de erro
    """
    try:
        # Obter informações básicas do ativo
        response = supabase.table('ativos').select('*').eq('ticker', ticker).execute()
        
        if not response.data or len(response.data) == 0:
            print(f"Ativo {ticker} não encontrado na base de dados")
            return None
        
        info_basica = response.data[0]
        
        # Calcular indicadores
        retorno_acumulado = calcular_retorno_acumulado(ticker, periodo_anos)
        retorno_anualizado = calcular_retorno_anualizado(ticker, periodo_anos)
        volatilidade = calcular_volatilidade(ticker, periodo_anos)
        max_drawdown = calcular_max_drawdown(ticker, periodo_anos)
        sharpe = calcular_sharpe(ticker, periodo_anos)
        
        # Montar resumo completo
        resumo = {
            'id': info_basica.get('id'),
            'ticker': ticker,
            'nome': info_basica.get('nome'),
            'preco_atual': info_basica.get('preco_atual'),
            'data_atualizacao': info_basica.get('data_atualizacao'),
            'retorno_acumulado': retorno_acumulado,
            'retorno_anualizado': retorno_anualizado,
            'volatilidade': volatilidade,
            'max_drawdown': max_drawdown,
            'sharpe': sharpe,
            'periodo_anos': periodo_anos
        }
        
        return resumo
    except Exception as e:
        print(f"⚠️ Erro ao obter resumo do ativo {ticker}: {str(e)}")
        return None