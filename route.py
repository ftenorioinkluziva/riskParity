from flask import Flask, jsonify, request
from calculos_financeiros import (
    calcular_retorno_acumulado,
    calcular_retorno_anualizado,
    calcular_volatilidade,
    calcular_max_drawdown,
    calcular_sharpe,
    obter_resumo_ativo
)

app = Flask(__name__)

# Configuração CORS para permitir requisições de diferentes origens
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,POST,OPTIONS')
    return response

# Rota para obter retorno acumulado
@app.route('/api/retorno-acumulado/<ticker>', methods=['GET'])
def api_retorno_acumulado(ticker):
    periodo_anos = request.args.get('periodo', default=5, type=int)
    resultado = calcular_retorno_acumulado(ticker, periodo_anos)
    
    if resultado is None:
        return jsonify({'error': f'Não foi possível calcular o retorno acumulado para {ticker}'}), 404
    
    return jsonify({
        'ticker': ticker,
        'retorno_acumulado': resultado,
        'periodo_anos': periodo_anos
    })

# Rota para obter retorno anualizado
@app.route('/api/retorno-anualizado/<ticker>', methods=['GET'])
def api_retorno_anualizado(ticker):
    periodo_anos = request.args.get('periodo', default=5, type=int)
    resultado = calcular_retorno_anualizado(ticker, periodo_anos)
    
    if resultado is None:
        return jsonify({'error': f'Não foi possível calcular o retorno anualizado para {ticker}'}), 404
    
    return jsonify({
        'ticker': ticker,
        'retorno_anualizado': resultado,
        'periodo_anos': periodo_anos
    })

# Rota para obter volatilidade
@app.route('/api/volatilidade/<ticker>', methods=['GET'])
def api_volatilidade(ticker):
    periodo_anos = request.args.get('periodo', default=5, type=int)
    resultado = calcular_volatilidade(ticker, periodo_anos)
    
    if resultado is None:
        return jsonify({'error': f'Não foi possível calcular a volatilidade para {ticker}'}), 404
    
    return jsonify({
        'ticker': ticker,
        'volatilidade': resultado,
        'periodo_anos': periodo_anos
    })

# Rota para obter máximo drawdown
@app.route('/api/max-drawdown/<ticker>', methods=['GET'])
def api_max_drawdown(ticker):
    periodo_anos = request.args.get('periodo', default=5, type=int)
    resultado = calcular_max_drawdown(ticker, periodo_anos)
    
    if resultado is None:
        return jsonify({'error': f'Não foi possível calcular o máximo drawdown para {ticker}'}), 404
    
    return jsonify({
        'ticker': ticker,
        'max_drawdown': resultado,
        'periodo_anos': periodo_anos
    })

# Rota para obter índice de Sharpe
@app.route('/api/sharpe/<ticker>', methods=['GET'])
def api_sharpe(ticker):
    periodo_anos = request.args.get('periodo', default=5, type=int)
    taxa_livre_risco = request.args.get('taxa_livre_risco', default=None, type=float)
    resultado = calcular_sharpe(ticker, periodo_anos, taxa_livre_risco)
    
    if resultado is None:
        return jsonify({'error': f'Não foi possível calcular o índice de Sharpe para {ticker}'}), 404
    
    return jsonify({
        'ticker': ticker,
        'sharpe': resultado,
        'periodo_anos': periodo_anos,
        'taxa_livre_risco': taxa_livre_risco if taxa_livre_risco is not None else 'CDI'
    })

# Rota para obter resumo completo de um ativo
@app.route('/api/resumo/<ticker>', methods=['GET'])
def api_resumo_ativo(ticker):
    periodo_anos = request.args.get('periodo', default=5, type=int)
    resultado = obter_resumo_ativo(ticker, periodo_anos)
    
    if resultado is None:
        return jsonify({'error': f'Não foi possível obter o resumo para {ticker}'}), 404
    
    return jsonify(resultado)

# Rota para obter resumo de múltiplos ativos
@app.route('/api/resumo-varios', methods=['GET'])
def api_resumo_varios():
    # Obter lista de tickers da query string (exemplo: ?tickers=BOVA11.SA,USDBRL=X,CDI)
    tickers_param = request.args.get('tickers', '')
    if not tickers_param:
        return jsonify({'error': 'Parâmetro "tickers" não fornecido'}), 400
    
    tickers = tickers_param.split(',')
    periodo_anos = request.args.get('periodo', default=5, type=int)
    
    resultados = {}
    for ticker in tickers:
        ticker = ticker.strip()
        resultado = obter_resumo_ativo(ticker, periodo_anos)
        if resultado is not None:
            resultados[ticker] = resultado
    
    if not resultados:
        return jsonify({'error': 'Não foi possível obter dados para nenhum dos tickers fornecidos'}), 404
    
    return jsonify({
        'ativos': resultados,
        'periodo_anos': periodo_anos,
        'total_ativos': len(resultados)
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)