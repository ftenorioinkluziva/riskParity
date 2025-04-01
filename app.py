from flask import Flask, jsonify, request
from supabase import create_client
import os
from flask_cors import CORS
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

app = Flask(__name__)
CORS(app)  # Habilita CORS para todas as rotas

# Configurações do Supabase
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# Verificar se as variáveis de ambiente estão definidas
if not SUPABASE_URL or not SUPABASE_KEY:
    print("\n⚠️ AVISO: Variáveis de ambiente SUPABASE_URL e/ou SUPABASE_KEY não definidas.")
    print("Defina estas variáveis ou insira os valores diretamente no código (apenas para desenvolvimento):\n")
    print('SUPABASE_URL = "https://seu-projeto.supabase.co"  # Substitua pelo seu URL real')
    print('SUPABASE_KEY = "sua-chave-api"  # Substitua pela sua chave real\n')
    # Fallback para valores hardcoded (apenas para desenvolvimento)
    SUPABASE_URL = 'https://dxwebxduuazebqtkumtv.supabase.co'
    SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR4d2VieGR1dWF6ZWJxdGt1bXR2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDE1OTMxMzcsImV4cCI6MjA1NzE2OTEzN30.v53W6iz_BJup66qst03jWqjHzJ0DGKmUC6WrVGLpt-Y'

# Inicializar o cliente Supabase apenas se as credenciais estiverem presentes
supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✓ Conexão com Supabase estabelecida com sucesso.")
    except Exception as e:
        print(f"⚠️ Erro ao conectar com o Supabase: {str(e)}")
        print("Verifique se a URL e a chave estão corretas.")

# =========================
# Funções para cálculos financeiros
# =========================

def obter_dados_historicos(ticker, periodo_anos=5):
    """
    Obtém os dados históricos de um ativo do banco de dados
    
    Args:
        ticker (str): O ticker do ativo
        periodo_anos (int): Período em anos para busca (padrão: 5)
    
    Returns:
        pandas.DataFrame: DataFrame com os dados históricos ou None em caso de erro
    """
    if not supabase:
        print("Conexão com Supabase não estabelecida")
        return None
        
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
        preco_inicial = dados['fechamento'].iloc[0]
        preco_final = dados['fechamento'].iloc[-1]
        
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
        dados['pico'] = dados['fechamento'].cummax()
        dados['drawdown'] = (dados['fechamento'] / dados['pico'] - 1) * 100
        
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

def obter_resumo_ativo(ticker, periodo_anos=5):
    """
    Obtém o resumo completo de um ativo, incluindo todos os indicadores
    
    Args:
        ticker (str): O ticker do ativo
        periodo_anos (int): Período em anos para cálculo (padrão: 5)
    
    Returns:
        dict: Dicionário com todos os indicadores ou None em caso de erro
    """
    if not supabase:
        print("Conexão com Supabase não estabelecida")
        return None
        
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

# =========================
# Rotas API existentes
# =========================

@app.route('/api/status', methods=['GET'])
def status():
    """Endpoint para verificar o status da API e conexão com Supabase"""
    if supabase:
        try:
            # Modifique a consulta para evitar o erro de sintaxe
            # Em vez de usar count(*), vamos apenas selecionar um registro limitado
            response = supabase.table('ativos').select('*').limit(1).execute()
            connection_status = "conectado"
        except Exception as e:
            connection_status = f"erro: {str(e)}"
    else:
        connection_status = "desconectado"
    
    return jsonify({
        "status": "online",
        "timestamp": datetime.now().isoformat(),
        "supabase_connection": connection_status
    })

@app.route('/api/ativos', methods=['GET'])
def obter_ativos():
    """Endpoint para obter a lista de todos os ativos"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        response = supabase.table('ativos').select('*').execute()
        return jsonify(response.data)
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/ativo/<ticker>', methods=['GET'])
def obter_ativo(ticker):
    """Endpoint para obter detalhes de um ativo específico"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        response = supabase.table('ativos').select('*').eq('ticker', ticker).execute()
        if response.data:
            return jsonify(response.data[0])
        return jsonify({"erro": "Ativo não encontrado"}), 404
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/historico/<ticker>', methods=['GET'])
def obter_historico(ticker):
    """Endpoint para obter o histórico de preços de um ativo"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        # Obtém últimos 30 dias como padrão
        dias = request.args.get('dias', default=30, type=int)
        data_limite = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')
        
        response = supabase.table('dados_historicos') \
            .select('*') \
            .eq('ticker', ticker) \
            .gte('data', data_limite) \
            .order('data', desc=False) \
            .execute()
            
        return jsonify(response.data)
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/comparativo', methods=['GET'])
def obter_comparativo():
    """Endpoint para comparar o desempenho de múltiplos ativos"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        # Obtém últimos 30 dias como padrão
        dias = request.args.get('dias', default=30, type=int)
        data_limite = (datetime.now() - timedelta(days=dias)).strftime('%Y-%m-%d')
        
        # Obter todos os tickers solicitados
        tickers = request.args.get('tickers', default='BOVA11.SA,CDI', type=str)
        tickers_lista = tickers.split(',')
        
        resultado = {}
        
        for ticker in tickers_lista:
            response = supabase.table('dados_historicos') \
                .select('data,fechamento_ajustado') \
                .eq('ticker', ticker) \
                .gte('data', data_limite) \
                .order('data', desc=False) \
                .execute()
            
            # Normalizar para base 100
            if response.data:
                dados = response.data
                primeiro_valor = dados[0]['fechamento_ajustado']
                if primeiro_valor:  # Verificar se não é None ou 0
                    dados_normalizados = [
                        {
                            'data': item['data'],
                            'valor': (item['fechamento_ajustado'] / primeiro_valor) * 100 if item['fechamento_ajustado'] else None
                        }
                        for item in dados
                    ]
                    resultado[ticker] = dados_normalizados
        
        return jsonify(resultado)
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

# =========================
# Novas rotas para cálculos
# =========================

@app.route('/api/calculo/retorno-acumulado/<ticker>', methods=['GET'])
def api_retorno_acumulado(ticker):
    """Endpoint para calcular o retorno acumulado de um ativo"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    periodo_anos = request.args.get('periodo', default=5, type=int)
    resultado = calcular_retorno_acumulado(ticker, periodo_anos)
    
    if resultado is None:
        return jsonify({'erro': f'Não foi possível calcular o retorno acumulado para {ticker}'}), 404
    
    return jsonify({
        'ticker': ticker,
        'retorno_acumulado': resultado,
        'periodo_anos': periodo_anos
    })

@app.route('/api/calculo/retorno-anualizado/<ticker>', methods=['GET'])
def api_retorno_anualizado(ticker):
    """Endpoint para calcular o retorno anualizado de um ativo"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    periodo_anos = request.args.get('periodo', default=5, type=int)
    resultado = calcular_retorno_anualizado(ticker, periodo_anos)
    
    if resultado is None:
        return jsonify({'erro': f'Não foi possível calcular o retorno anualizado para {ticker}'}), 404
    
    return jsonify({
        'ticker': ticker,
        'retorno_anualizado': resultado,
        'periodo_anos': periodo_anos
    })

@app.route('/api/calculo/volatilidade/<ticker>', methods=['GET'])
def api_volatilidade(ticker):
    """Endpoint para calcular a volatilidade de um ativo"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    periodo_anos = request.args.get('periodo', default=5, type=int)
    resultado = calcular_volatilidade(ticker, periodo_anos)
    
    if resultado is None:
        return jsonify({'erro': f'Não foi possível calcular a volatilidade para {ticker}'}), 404
    
    return jsonify({
        'ticker': ticker,
        'volatilidade': resultado,
        'periodo_anos': periodo_anos
    })

@app.route('/api/calculo/max-drawdown/<ticker>', methods=['GET'])
def api_max_drawdown(ticker):
    """Endpoint para calcular o máximo drawdown de um ativo"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    periodo_anos = request.args.get('periodo', default=5, type=int)
    resultado = calcular_max_drawdown(ticker, periodo_anos)
    
    if resultado is None:
        return jsonify({'erro': f'Não foi possível calcular o máximo drawdown para {ticker}'}), 404
    
    return jsonify({
        'ticker': ticker,
        'max_drawdown': resultado,
        'periodo_anos': periodo_anos
    })

@app.route('/api/calculo/sharpe/<ticker>', methods=['GET'])
def api_sharpe(ticker):
    """Endpoint para calcular o índice de Sharpe de um ativo"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    periodo_anos = request.args.get('periodo', default=5, type=int)
    taxa_livre_risco = request.args.get('taxa_livre_risco', default=None, type=float)
    resultado = calcular_sharpe(ticker, periodo_anos, taxa_livre_risco)
    
    if resultado is None:
        return jsonify({'erro': f'Não foi possível calcular o índice de Sharpe para {ticker}'}), 404
    
    return jsonify({
        'ticker': ticker,
        'sharpe': resultado,
        'periodo_anos': periodo_anos,
        'taxa_livre_risco': taxa_livre_risco if taxa_livre_risco is not None else 'CDI'
    })

@app.route('/api/calculo/resumo/<ticker>', methods=['GET'])
def api_resumo_ativo(ticker):
    """Endpoint para obter o resumo completo de um ativo com todos os indicadores calculados"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    periodo_anos = request.args.get('periodo', default=5, type=int)
    resultado = obter_resumo_ativo(ticker, periodo_anos)
    
    if resultado is None:
        return jsonify({'erro': f'Não foi possível obter o resumo para {ticker}'}), 404
    
    return jsonify(resultado)

@app.route('/api/calculo/resumo-varios', methods=['GET'])
def api_resumo_varios():
    """Endpoint para obter o resumo completo de múltiplos ativos"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    # Obter lista de tickers da query string (exemplo: ?tickers=BOVA11.SA,USDBRL=X,CDI)
    tickers_param = request.args.get('tickers', '')
    if not tickers_param:
        return jsonify({'erro': 'Parâmetro "tickers" não fornecido'}), 400
    
    tickers = tickers_param.split(',')
    periodo_anos = request.args.get('periodo', default=5, type=int)
    
    resultados = {}
    for ticker in tickers:
        ticker = ticker.strip()
        resultado = obter_resumo_ativo(ticker, periodo_anos)
        if resultado is not None:
            resultados[ticker] = resultado
    
    if not resultados:
        return jsonify({'erro': 'Não foi possível obter dados para nenhum dos tickers fornecidos'}), 404
    
    return jsonify({
        'ativos': resultados,
        'periodo_anos': periodo_anos,
        'total_ativos': len(resultados)
    })

if __name__ == '__main__':
    print("\n🚀 Iniciando servidor de API...\n")
    
    # Verificar conexão com o Supabase
    if not supabase:
        print("⚠️ API iniciada sem conexão com o Supabase. Endpoints relacionados a dados não funcionarão.")
        print("Defina as variáveis de ambiente SUPABASE_URL e SUPABASE_KEY ou configure-as no código.\n")
    
    # Instruções de uso
    print("\nEndpoints existentes:")
    print("- GET /api/status - Verifica status da API")
    print("- GET /api/ativos - Lista todos os ativos")
    print("- GET /api/ativo/<ticker> - Detalhes de um ativo específico")
    print("- GET /api/historico/<ticker>?dias=30 - Histórico de preços de um ativo")
    print("- GET /api/comparativo?tickers=BOVA11.SA,CDI&dias=30 - Comparação de desempenho")
    
    print("\nNovos endpoints de cálculo:")
    print("- GET /api/calculo/retorno-acumulado/<ticker>?periodo=5 - Retorno acumulado")
    print("- GET /api/calculo/retorno-anualizado/<ticker>?periodo=5 - Retorno anualizado")
    print("- GET /api/calculo/volatilidade/<ticker>?periodo=5 - Volatilidade anualizada")
    print("- GET /api/calculo/max-drawdown/<ticker>?periodo=5 - Máximo drawdown")
    print("- GET /api/calculo/sharpe/<ticker>?periodo=5 - Índice de Sharpe")
    print("- GET /api/calculo/resumo/<ticker>?periodo=5 - Resumo completo de um ativo")
    print("- GET /api/calculo/resumo-varios?tickers=ticker1,ticker2&periodo=5 - Resumo de múltiplos ativos\n")
    
    app.run(debug=True, port=5000)