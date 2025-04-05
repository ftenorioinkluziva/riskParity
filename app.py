from flask import Flask, jsonify, request
from supabase import create_client
import os
from flask_cors import CORS
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import json

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

# =========================
# Novas rotas para gerenciar cestas de ativos
# =========================

# Rota para obter todas as cestas
@app.route('/api/cestas', methods=['GET'])
def obter_cestas():
    """Endpoint para obter todas as cestas do usuário"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        response = supabase.table('cestas').select('*').execute()
        return jsonify(response.data)
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

# Rota para obter uma cesta específica
@app.route('/api/cesta/<int:id>', methods=['GET'])
def obter_cesta(id):
    """Endpoint para obter detalhes de uma cesta específica"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        response = supabase.table('cestas').select('*').eq('id', id).execute()
        if response.data and len(response.data) > 0:
            return jsonify(response.data[0])
        return jsonify({"erro": "Cesta não encontrada"}), 404
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

# Rota para criar uma nova cesta
@app.route('/api/cesta', methods=['POST'])
def criar_cesta():
    """Endpoint para criar uma nova cesta"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        # Obter dados da requisição
        dados = request.json
        
        if not dados or not dados.get('nome') or not dados.get('ativos'):
            return jsonify({"erro": "Dados incompletos para criação da cesta"}), 400
        
        # Verificar se ativos é um objeto JSON válido
        if isinstance(dados.get('ativos'), dict):
            # Se já é um dicionário, mantemos assim
            ativos_json = dados.get('ativos')
        else:
            # Se é uma string, tentamos converter para JSON
            try:
                ativos_json = json.loads(dados.get('ativos'))
            except:
                return jsonify({"erro": "O campo 'ativos' deve ser um objeto JSON válido"}), 400
        
        # Criar nova cesta
        nova_cesta = {
            'nome': dados.get('nome'),
            'descricao': dados.get('descricao', ''),
            'ativos': ativos_json,
            'data_criacao': datetime.now().isoformat(),
            'data_atualizacao': datetime.now().isoformat()
        }
        
        response = supabase.table('cestas').insert(nova_cesta).execute()
        
        if response.data and len(response.data) > 0:
            return jsonify(response.data[0]), 201
        return jsonify({"erro": "Erro ao criar cesta"}), 500
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

# Rota para atualizar uma cesta existente
@app.route('/api/cesta/<int:id>', methods=['PUT'])
def atualizar_cesta(id):
    """Endpoint para atualizar uma cesta existente"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        # Verificar se a cesta existe
        response = supabase.table('cestas').select('*').eq('id', id).execute()
        if not response.data or len(response.data) == 0:
            return jsonify({"erro": "Cesta não encontrada"}), 404
        
        # Obter dados da requisição
        dados = request.json
        
        if not dados:
            return jsonify({"erro": "Dados não fornecidos para atualização"}), 400
        
        # Verificar se ativos é um objeto JSON válido, se fornecido
        if 'ativos' in dados:
            if isinstance(dados.get('ativos'), dict):
                # Se já é um dicionário, mantemos assim
                ativos_json = dados.get('ativos')
            else:
                # Se é uma string, tentamos converter para JSON
                try:
                    ativos_json = json.loads(dados.get('ativos'))
                except:
                    return jsonify({"erro": "O campo 'ativos' deve ser um objeto JSON válido"}), 400
            
            dados['ativos'] = ativos_json
        
        # Adicionar data de atualização
        dados['data_atualizacao'] = datetime.now().isoformat()
        
        # Atualizar cesta
        response = supabase.table('cestas').update(dados).eq('id', id).execute()
        
        if response.data and len(response.data) > 0:
            return jsonify(response.data[0])
        return jsonify({"erro": "Erro ao atualizar cesta"}), 500
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

# Rota para excluir uma cesta
@app.route('/api/cesta/<int:id>', methods=['DELETE'])
def excluir_cesta(id):
    """Endpoint para excluir uma cesta"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        # Verificar se a cesta existe
        response = supabase.table('cestas').select('*').eq('id', id).execute()
        if not response.data or len(response.data) == 0:
            return jsonify({"erro": "Cesta não encontrada"}), 404
        
        # Excluir cesta
        supabase.table('cestas').delete().eq('id', id).execute()
        
        return jsonify({"mensagem": "Cesta excluída com sucesso"})
    except Exception as e:
        return jsonify({"erro": str(e)}), 500
    
# Rotas para gerenciar transações
@app.route('/api/transacoes', methods=['GET'])
def get_transacoes():
    """Endpoint to get all transactions with asset details"""
    if not supabase:
        return jsonify({"erro": "Supabase connection not established"}), 500
    
    try:
        # Get all transactions, ordered by date (newest first)
        response = supabase.table('transacoes').select('*').order('date', desc=True).execute()
        
        if not response.data:
            return jsonify([])
        
        transactions = response.data
        
        # Get all assets for lookup
        assets_response = supabase.table('ativos').select('*').execute()
        
        if assets_response.data:
            # Create a lookup dictionary for quick asset access
            assets_lookup = {asset['id']: asset for asset in assets_response.data}
            
            # Enhance each transaction with asset details
            for transaction in transactions:
                ativo_id = transaction.get('ativo_id')
                
                if ativo_id and ativo_id in assets_lookup:
                    transaction['asset_details'] = assets_lookup[ativo_id]
                else:
                    transaction['asset_details'] = {"ticker": "Unknown", "nome": "Unknown Asset"}
        
        # Calculate totalValue for consistency if not already present
        for transaction in transactions:
            if 'totalValue' not in transaction and 'quantity' in transaction and 'price' in transaction:
                transaction['totalValue'] = float(transaction['quantity']) * float(transaction['price'])
        
        return jsonify(transactions)
    except Exception as e:
        print(f"Error fetching transactions: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
@app.route('/api/transacoes', methods=['POST'])
def add_transacao():
    """Endpoint to add a new transaction with asset relationship"""
    if not supabase:
        return jsonify({"erro": "Supabase connection not established"}), 500
    
    try:
        data = request.json
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        # Validate required fields
        required_fields = ['type', 'ativo_id', 'quantity', 'price', 'date']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Required field '{field}' is missing"}), 400
        
        # Validate transaction type
        if data['type'] not in ['buy', 'sell']:
            return jsonify({"error": "Transaction type must be 'buy' or 'sell'"}), 400
        
        # Validate numeric values
        try:
            quantity = float(data['quantity'])
            price = float(data['price'])
            
            if quantity <= 0 or price <= 0:
                return jsonify({"error": "Quantity and price must be positive values"}), 400
        except ValueError:
            return jsonify({"error": "Quantity and price must be numeric values"}), 400
        
        # Validate date
        try:
            transaction_date = datetime.strptime(data['date'], '%Y-%m-%d')
            current_date = datetime.now()
            
            if transaction_date > current_date:
                return jsonify({"error": "Transaction date cannot be in the future"}), 400
            
            # Convert to ISO string for storage
            data['date'] = transaction_date.strftime('%Y-%m-%d')
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
        
        # Verify asset exists
        ativo_id = data['ativo_id']
        asset_response = supabase.table('ativos').select('*').eq('id', ativo_id).execute()
        
        if not asset_response.data:
            return jsonify({"error": f"Asset with ID {ativo_id} not found"}), 404
        
        # If selling, verify sufficient quantity
        if data['type'] == 'sell':
            # Get all previous transactions for this asset
            prev_transactions = supabase.table('transacoes').select('*').eq('ativo_id', ativo_id).execute()
            
            if prev_transactions.data:
                # Calculate current quantity
                current_quantity = 0
                for tx in prev_transactions.data:
                    if tx['type'] == 'buy':
                        current_quantity += float(tx['quantity'])
                    else:
                        current_quantity -= float(tx['quantity'])
                
                if current_quantity < quantity:
                    return jsonify({
                        "error": f"Insufficient quantity for sale. You have {current_quantity} units of this asset"
                    }), 400
        
        # Add creation timestamp
        data['created_at'] = datetime.now().isoformat()
        
        # Remove totalvalue if it exists in the input data
        if 'totalvalue' in data:
            del data['totalvalue']
        
        # Insert into database
        response = supabase.table('transacoes').insert(data).execute()
        
        if response.data:
            # Add asset details to response
            transaction = response.data[0]
            transaction['asset_details'] = asset_response.data[0]
            
            # Calculate totalValue for the response
            transaction['totalValue'] = float(transaction['quantity']) * float(transaction['price'])
            
            return jsonify(transaction), 201
        else:
            return jsonify({"error": "Failed to insert transaction"}), 500
        
    except Exception as e:
        print(f"Error adding transaction: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/transacoes/<int:id>', methods=['DELETE'])
def delete_transacao(id):
    """Endpoint para excluir uma transação"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        # Verificar se a transação existe
        response = supabase.table('transacoes').select('*').eq('id', id).execute()
        
        if not response.data or len(response.data) == 0:
            return jsonify({"erro": "Transação não encontrada"}), 404
        
        # Obter informações da transação para validação
        transacao = response.data[0]
        
        # Se for uma compra, verificar se há vendas dependentes desta compra
        if transacao['type'] == 'buy':
            asset = transacao['asset']
            quantidade_compra = float(transacao['quantity'])
            
            # Buscar todas as transações deste ativo
            response_todas = supabase.table('transacoes').select('*').eq('asset', asset).execute()
            
            if response_todas.data:
                # Calcular o saldo de compras excluindo esta transação
                total_compras = 0
                total_vendas = 0
                
                for t in response_todas.data:
                    if t['id'] == id:
                        continue  # Ignorar a transação que será excluída
                        
                    if t['type'] == 'buy':
                        total_compras += float(t['quantity'])
                    else:
                        total_vendas += float(t['quantity'])
                
                # Se o saldo após remover esta compra for negativo, não permite exclusão
                if total_compras < total_vendas:
                    return jsonify({
                        "erro": "Não é possível excluir esta compra pois há vendas que dependem dela."
                    }), 400
        
        # Excluir a transação
        supabase.table('transacoes').delete().eq('id', id).execute()
        
        return jsonify({"mensagem": "Transação excluída com sucesso"})
    except Exception as e:
        print(f"⚠️ Erro ao excluir transação: {str(e)}")
        return jsonify({"erro": str(e)}), 500

@app.route('/api/carteira', methods=['GET'])
def get_carteira():
    """Endpoint para obter o resumo da carteira"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        # Buscar todas as transações
        response = supabase.table('transacoes').select('*').execute()
        
        if not response.data:
            return jsonify({"ativos": [], "total": 0})
        
        # Buscar preços atuais dos ativos
        response_ativos = supabase.table('ativos').select('*').execute()
        
        # Criar mapa de ticker para preço atual
        precos_atuais = {}
        if response_ativos.data:
            for ativo in response_ativos.data:
                precos_atuais[ativo['ticker']] = {
                    'preco_atual': ativo['preco_atual'],
                    'nome': ativo['nome']
                }
        
        # Calcular a carteira
        carteira = {}
        
        for transacao in response.data:
            asset = transacao['asset']
            
            if asset not in carteira:
                carteira[asset] = {
                    'asset': asset,
                    'nome': precos_atuais.get(asset, {}).get('nome', asset),
                    'quantidade': 0,
                    'preco_medio': 0,
                    'total_investido': 0,
                    'preco_atual': precos_atuais.get(asset, {}).get('preco_atual', 0),
                    'valor_atual': 0,
                    'lucro': 0,
                    'rendimento': 0
                }
            
            ativo = carteira[asset]
            
            if transacao['type'] == 'buy':
                # Cálculo do preço médio para compras
                quantidade_antiga = ativo['quantidade']
                valor_antigo = quantidade_antiga * ativo['preco_medio']
                quantidade_nova = float(transacao['quantity'])
                valor_novo = quantidade_nova * float(transacao['price'])
                quantidade_total = quantidade_antiga + quantidade_nova
                
                if quantidade_total > 0:
                    ativo['preco_medio'] = (valor_antigo + valor_novo) / quantidade_total
                
                ativo['quantidade'] += quantidade_nova
                ativo['total_investido'] += valor_novo
            else:  # sell
                ativo['quantidade'] -= float(transacao['quantity'])
                
                # Ajustar o valor investido proporcionalmente
                if ativo['quantidade'] > 0:
                    ativo['total_investido'] = ativo['quantidade'] * ativo['preco_medio']
                else:
                    ativo['quantidade'] = 0
                    ativo['total_investido'] = 0
        
        # Calcular valores atuais e rendimentos
        for asset, ativo in carteira.items():
            ativo['valor_atual'] = ativo['quantidade'] * ativo['preco_atual']
            ativo['lucro'] = ativo['valor_atual'] - ativo['total_investido']
            
            if ativo['total_investido'] > 0:
                ativo['rendimento'] = (ativo['lucro'] / ativo['total_investido']) * 100
            else:
                ativo['rendimento'] = 0
        
        # Filtrar apenas ativos com quantidade > 0
        carteira_filtrada = [ativo for ativo in carteira.values() if ativo['quantidade'] > 0]
        
        # Calcular totais
        total_investido = sum(ativo['total_investido'] for ativo in carteira_filtrada)
        valor_atual = sum(ativo['valor_atual'] for ativo in carteira_filtrada)
        lucro_total = sum(ativo['lucro'] for ativo in carteira_filtrada)
        
        rendimento_carteira = 0
        if total_investido > 0:
            rendimento_carteira = (lucro_total / total_investido) * 100
        
        # Preparar resposta
        resposta = {
            "ativos": carteira_filtrada,
            "totais": {
                "investido": total_investido,
                "atual": valor_atual,
                "lucro": lucro_total,
                "rendimento": rendimento_carteira
            }
        }
        
        return jsonify(resposta)
    except Exception as e:
        print(f"⚠️ Erro ao calcular carteira: {str(e)}")
        return jsonify({"erro": str(e)}), 500

# =========================
# Endpoints para Fundos de Investimento
# =========================

@app.route('/api/investment-funds', methods=['GET'])
def get_investment_funds():
    """Endpoint para obter todos os fundos de investimento"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        response = supabase.table('investment_funds').select('*').order('name', desc=False).execute()
        return jsonify(response.data)
    except Exception as e:
        print(f"Erro ao buscar fundos de investimento: {str(e)}")
        return jsonify({"erro": str(e)}), 500


@app.route('/api/investment-funds', methods=['POST'])
def create_investment_fund():
    """Endpoint para criar um novo fundo de investimento"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        data = request.json
        
        # Validar dados recebidos
        required_fields = ['name', 'initial_investment', 'current_value', 'investment_date']
        for field in required_fields:
            if field not in data:
                return jsonify({"erro": f"Campo obrigatório '{field}' não informado"}), 400
        
        # Validar valores numéricos
        try:
            initial_investment = float(data['initial_investment'])
            current_value = float(data['current_value'])
            
            if initial_investment < 0 or current_value < 0:
                return jsonify({"erro": "Valores de investimento não podem ser negativos"}), 400
        except ValueError:
            return jsonify({"erro": "Valores de investimento devem ser numéricos"}), 400
        
        # Validar data
        try:
            investment_date = datetime.strptime(data['investment_date'], '%Y-%m-%d')
            if investment_date > datetime.now():
                return jsonify({"erro": "Data de investimento não pode ser futura"}), 400
            
            # Formatando a data para o formato ISO
            data['investment_date'] = investment_date.strftime('%Y-%m-%d')
        except ValueError:
            return jsonify({"erro": "Formato de data inválido. Use YYYY-MM-DD"}), 400
        
        # Adicionar timestamps
        data['created_at'] = datetime.now().isoformat()
        data['updated_at'] = datetime.now().isoformat()
        
        # Inserir no banco de dados
        response = supabase.table('investment_funds').insert(data).execute()
        
        if response.data and len(response.data) > 0:
            return jsonify(response.data[0]), 201
        else:
            return jsonify({"erro": "Erro ao inserir fundo de investimento"}), 500
            
    except Exception as e:
        print(f"Erro ao criar fundo de investimento: {str(e)}")
        return jsonify({"erro": str(e)}), 500


@app.route('/api/investment-funds/<int:fund_id>', methods=['PUT'])
def update_investment_fund(fund_id):
    """Endpoint para atualizar um fundo de investimento existente"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        data = request.json
        
        # Verificar se o fundo existe
        check_response = supabase.table('investment_funds').select('*').eq('id', fund_id).execute()
        if not check_response.data or len(check_response.data) == 0:
            return jsonify({"erro": "Fundo de investimento não encontrado"}), 404
            
        # Validar valores numéricos se fornecidos
        if 'initial_investment' in data:
            try:
                initial_investment = float(data['initial_investment'])
                if initial_investment < 0:
                    return jsonify({"erro": "Valor de investimento inicial não pode ser negativo"}), 400
            except ValueError:
                return jsonify({"erro": "Valor de investimento inicial deve ser numérico"}), 400
                
        if 'current_value' in data:
            try:
                current_value = float(data['current_value'])
                if current_value < 0:
                    return jsonify({"erro": "Valor atual não pode ser negativo"}), 400
            except ValueError:
                return jsonify({"erro": "Valor atual deve ser numérico"}), 400
                
        # Validar data se fornecida
        if 'investment_date' in data:
            try:
                investment_date = datetime.strptime(data['investment_date'], '%Y-%m-%d')
                if investment_date > datetime.now():
                    return jsonify({"erro": "Data de investimento não pode ser futura"}), 400
                
                # Formatando a data para o formato ISO
                data['investment_date'] = investment_date.strftime('%Y-%m-%d')
            except ValueError:
                return jsonify({"erro": "Formato de data inválido. Use YYYY-MM-DD"}), 400
        
        # Atualizar timestamp
        data['updated_at'] = datetime.now().isoformat()
        
        # Atualizar no banco de dados
        response = supabase.table('investment_funds').update(data).eq('id', fund_id).execute()
        
        if response.data and len(response.data) > 0:
            return jsonify(response.data[0])
        else:
            return jsonify({"erro": "Erro ao atualizar fundo de investimento"}), 500
            
    except Exception as e:
        print(f"Erro ao atualizar fundo de investimento: {str(e)}")
        return jsonify({"erro": str(e)}), 500


@app.route('/api/investment-funds/<int:fund_id>', methods=['DELETE'])
def delete_investment_fund(fund_id):
    """Endpoint para excluir um fundo de investimento"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        # Verificar se o fundo existe
        check_response = supabase.table('investment_funds').select('*').eq('id', fund_id).execute()
        if not check_response.data or len(check_response.data) == 0:
            return jsonify({"erro": "Fundo de investimento não encontrado"}), 404
            
        # Excluir do banco de dados
        supabase.table('investment_funds').delete().eq('id', fund_id).execute()
        
        return jsonify({"mensagem": "Fundo de investimento excluído com sucesso"})
            
    except Exception as e:
        print(f"Erro ao excluir fundo de investimento: {str(e)}")
        return jsonify({"erro": str(e)}), 500


# =========================
# Endpoints para Saldo em Caixa
# =========================

@app.route('/api/cash-balance', methods=['GET'])
def get_cash_balance():
    """Endpoint para obter o saldo em caixa atual"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        response = supabase.table('cash_balance').select('*').order('id', desc=True).limit(1).execute()
        
        if response.data and len(response.data) > 0:
            return jsonify(response.data[0])
        else:
            # Se não houver registro, retornar valor zero
            return jsonify({"value": 0.00, "last_update": datetime.now().isoformat()})
            
    except Exception as e:
        print(f"Erro ao buscar saldo em caixa: {str(e)}")
        return jsonify({"erro": str(e)}), 500


@app.route('/api/cash-balance', methods=['PUT'])
def update_cash_balance():
    """Endpoint para atualizar o saldo em caixa"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        data = request.json
        
        # Validar dados recebidos
        if 'value' not in data:
            return jsonify({"erro": "Valor do saldo em caixa não informado"}), 400
            
        # Validar valor numérico
        try:
            cash_value = float(data['value'])
            if cash_value < 0:
                return jsonify({"erro": "Valor do saldo em caixa não pode ser negativo"}), 400
        except ValueError:
            return jsonify({"erro": "Valor do saldo em caixa deve ser numérico"}), 400
            
        # Verificar se já existe registro
        check_response = supabase.table('cash_balance').select('*').order('id', desc=True).limit(1).execute()
        
        if check_response.data and len(check_response.data) > 0:
            # Atualizar registro existente
            update_data = {
                'value': cash_value,
                'last_update': datetime.now().isoformat()
            }
            
            response = supabase.table('cash_balance').update(update_data).eq('id', check_response.data[0]['id']).execute()
            
            if response.data and len(response.data) > 0:
                return jsonify(response.data[0])
            else:
                return jsonify({"erro": "Erro ao atualizar saldo em caixa"}), 500
        else:
            # Criar novo registro
            insert_data = {
                'value': cash_value,
                'last_update': datetime.now().isoformat()
            }
            
            response = supabase.table('cash_balance').insert(insert_data).execute()
            
            if response.data and len(response.data) > 0:
                return jsonify(response.data[0])
            else:
                return jsonify({"erro": "Erro ao inserir saldo em caixa"}), 500
                
    except Exception as e:
        print(f"Erro ao atualizar saldo em caixa: {str(e)}")
        return jsonify({"erro": str(e)}), 500
    

@app.route('/api/update-prices', methods=['POST'])
def update_prices():
    """Endpoint para atualizar preços dos ativos utilizando o CD3 Connector"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        # Requisição assíncrona para o script de atualização
        import subprocess
        import os
        import sys
        
        # Caminho para o script de atualização
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "atualizar_precos_cedro.py")
        
        # Verificar se o script existe
        if not os.path.exists(script_path):
            return jsonify({"erro": f"Script de atualização não encontrado em {script_path}"}), 404
        
        # Usar sys.executable para garantir que o mesmo Python seja usado
        python_exe = sys.executable
        
        # Iniciar o processo em segundo plano
        subprocess.Popen([
            python_exe, 
            script_path, 
            '--single-run',
            '--timeout', '30'
        ], 
        # Redirecionar saída para um arquivo
        stdout=open('atualizador_output.log', 'w'),
        stderr=subprocess.STDOUT
        )
        
        return jsonify({
            "mensagem": "Processo de atualização de preços iniciado com sucesso",
            "status": "processing",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        print(f"Erro ao iniciar atualização de preços: {str(e)}")
        return jsonify({"erro": str(e)}), 500

@app.route('/api/last-update', methods=['GET'])
def get_last_update():
    """Endpoint para obter a data da última atualização de preços"""
    if not supabase:
        return jsonify({"erro": "Conexão com Supabase não estabelecida"}), 500
    
    try:
        # Obter todos os ativos ordenados pela data de atualização (mais recente primeiro)
        response = supabase.table('ativos').select('data_atualizacao').order('data_atualizacao', desc=True).limit(1).execute()
        
        resultado = {
            "timestamp": datetime.now().isoformat()
        }
        
        if response.data and len(response.data) > 0:
            resultado["last_update"] = response.data[0]['data_atualizacao']
        else:
            resultado["last_update"] = None
        
        return jsonify(resultado)
    except Exception as e:
        print(f"Erro ao obter última atualização: {str(e)}")
        return jsonify({"erro": str(e)}), 500

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