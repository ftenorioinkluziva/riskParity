import threading
import time
import logging
import sys
import os
import argparse
import requests
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

# Configurações do Supabase
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://dxwebxduuazebqtkumtv.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR4d2VieGR1dWF6ZWJxdGt1bXR2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDE1OTMxMzcsImV4cCI6MjA1NzE2OTEzN30.v53W6iz_BJup66qst03jWqjHzJ0DGKmUC6WrVGLpt-Y')

# Configurações da API RTD
RTD_API_URL = os.environ.get('RTD_API_URL', 'http://localhost:5000/api/MarketData')

# Configuração de logging
LOG_FILENAME = "rtd_price_update.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # Usar encoding utf-8 para o arquivo de log
        logging.FileHandler(LOG_FILENAME, encoding='utf-8'),
        # Não usar emojis no console para evitar problemas de encoding
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("atualizador_precos_rtd")


class RTDUpdater:
    def __init__(self, interval_seconds=60, timeout=20):
        """
        Inicializa o atualizador de preços usando a API RTD
        
        Args:
            interval_seconds (int): Intervalo entre atualizações em segundos
            timeout (int): Tempo máximo para receber cotações em segundos
        """
        self.interval_seconds = interval_seconds
        self.timeout = timeout
        self.api_url = RTD_API_URL
        
        # Inicializar variáveis
        self._running = False
        self._signal = threading.Event()
        self._atualizacoes_recebidas = 0
        self._atualizacoes_esperadas = 0
        self._tempo_inicio = 0
        self._ultima_atualizacao = 0

        # Conexão com Supabase
        try:
            self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            logger.info("Conexão com Supabase estabelecida com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao conectar com o Supabase: {str(e)}")
            self.supabase = None
            
        # Dicionário para armazenar a correspondência entre tickers
        self.ticker_map = {}
        
        # Carregar lista de ativos do banco de dados
        self.ativos = []
        self.tickers_rtd = []
        self._load_ativos()
    
    def _load_ativos(self):
        """Carrega a lista de ativos do banco de dados Supabase"""
        if not self.supabase:
            logger.error("Supabase não inicializado. Não foi possível carregar ativos.")
            return
            
        try:
            response = self.supabase.table('ativos').select('*').execute()
            self.ativos = response.data
            
            # Tabela de equivalência específica entre tickers do banco e tickers da API RTD
            equivalencia_tickers = {
                'BOVA11.SA': 'BOVA11',
                'XFIX11.SA': 'XFIX11',
                'IB5M11.SA': 'IB5M11',
                'B5P211.SA': 'B5P211',
                'FIXA11.SA': 'FIXA11',
                'USDBRL=X': 'DOLCV'
            }
            
            # Criar mapeamento para os tickers
            for ativo in self.ativos:
                ticker_banco = ativo['ticker']
                # Usar a tabela de equivalência, se disponível
                if ticker_banco in equivalencia_tickers:
                    ticker_rtd = equivalencia_tickers[ticker_banco]
                else:
                    # Para outros casos, aplicar a regra geral (remover sufixos)
                    ticker_rtd = ticker_banco.split('.')[0].replace('=', '')
                
                # Armazenar o mapeamento nos dois sentidos
                self.ticker_map[ticker_rtd] = ticker_banco
                
                logger.debug(f"Mapeamento: {ticker_banco} -> {ticker_rtd}")
                
            logger.info(f"Carregados {len(self.ativos)} ativos do banco de dados.")
            
            # Filtrar tickers disponíveis (excluir CDI e USDBRL se necessário)
            self.tickers_rtd = list(self.ticker_map.keys())
            self._atualizacoes_esperadas = len(self.tickers_rtd)
            
        except Exception as e:
            logger.error(f"Erro ao carregar ativos: {str(e)}")
    
    def start(self):
        """Inicia o loop de atualização"""
        if self._running:
            logger.warning("Atualizador já está em execução")
            return
            
        self._running = True
        
        logger.info(f"Iniciando loop de atualização a cada {self.interval_seconds} segundos")
        logger.info(f"Timeout para receber cotações: {self.timeout} segundos")
        
        # Iniciar o loop em uma thread separada
        threading.Thread(target=self._update_loop).start()
    
    def stop(self):
        """Para o loop de atualização"""
        self._running = False
        self._signal.set()
        logger.info("Parando atualizador...")

    def _update_loop(self):
        """Loop de atualização de preços"""
        try:
            while self._running:
                self._tempo_inicio = time.time()
                now = time.strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"Iniciando ciclo de atualização em {now}")
                
                self._atualizacoes_recebidas = 0
                
                self._solicitar_cotacoes()
                
                elapsed = time.time() - self._tempo_inicio
                logger.info(f"Ciclo de atualização concluído em {elapsed:.1f} segundos")
                logger.info(f"Atualizações recebidas: {self._atualizacoes_recebidas}/{self._atualizacoes_esperadas}")
                self._ultima_atualizacao = time.time()
                
                next_update = self.interval_seconds - elapsed
                if next_update > 0:
                    next_time = time.strftime("%H:%M:%S", time.localtime(time.time() + next_update))
                    logger.info(f"Próxima atualização às {next_time} ({next_update:.1f} segundos)")
                    self._signal.wait(next_update)
                    
                else:
                    logger.warning("Ciclo de atualização demorou mais que o intervalo configurado")
                    
        except Exception as e:
            logger.error(f"Erro no loop de atualização: {str(e)}")
        finally:
            logger.info("Loop de atualização encerrado")
    
    def _solicitar_cotacoes(self):
        """Solicita cotações para todos os ativos usando a API RTD"""
        logger.info("Iniciando solicitação de cotações via API RTD")
            
        try:
            for ticker_rtd in self.tickers_rtd:
                try:
                    # Tipo de dado fixo como 'ULT' para o último preço
                    data_type = "ULT"
                    logger.info(f"Solicitando cotação para {ticker_rtd} ({data_type})")
                    
                    # Fazer requisição HTTP para a API RTD
                    url = f"{self.api_url}/{ticker_rtd}/{data_type}"
                    response = requests.get(url, timeout=self.timeout)
                    
                    if response.status_code == 200:
                        data = response.json()
                        price_str = data.get('value', '')
                        
                        # Converter o valor para float, substituindo a vírgula por ponto se necessário
                        price_str = price_str.replace(',', '.')
                        
                        try:
                            price = float(price_str)
                            logger.info(f"Preço obtido para {ticker_rtd}: {price}")
                            
                            # Obter o ticker original do banco de dados
                            original_ticker = self.ticker_map.get(ticker_rtd)
                            if original_ticker:
                                # Atualizar no banco de dados
                                self._update_price(original_ticker, price)
                                self._atualizacoes_recebidas += 1
                            else:
                                logger.warning(f"Ticker não encontrado no mapeamento: {ticker_rtd}")
                        except ValueError:
                            logger.warning(f"Valor não numérico recebido para {ticker_rtd}: {price_str}")
                    else:
                        logger.error(f"Erro ao obter cotação para {ticker_rtd}: {response.status_code}")
                        
                except requests.RequestException as e:
                    logger.error(f"Erro na requisição para {ticker_rtd}: {str(e)}")
                except Exception as e:
                    logger.error(f"Erro ao processar cotação para {ticker_rtd}: {str(e)}")
                
                # Pequeno delay entre requisições para não sobrecarregar a API
                time.sleep(0.2)
                
        except Exception as e:
            logger.error(f"Erro ao solicitar cotações: {str(e)}")
    
    def _update_price(self, ticker: str, price: float):
        """Atualiza o preço de um ativo no banco de dados"""
        if not self.supabase:
            logger.error("Supabase não inicializado. Não é possível atualizar preços.")
            return

        try:
            update_data = {
                'preco_atual': price,
                'data_atualizacao': datetime.now().isoformat()  # Atualizando a data da cotação
            }

            # Realiza a atualização no banco de dados
            response = self.supabase.table('ativos').update(update_data).eq('ticker', ticker).execute()

            # Verifica se a atualização foi realizada com sucesso
            if response.data and len(response.data) > 0:
                logger.info(f"Preço atualizado para {ticker}: R$ {price:.2f}")

                # Atualiza o objeto local também para manter a consistência
                for ativo in self.ativos:
                    if ativo['ticker'] == ticker:
                        ativo['preco_atual'] = price
                        ativo['data_atualizacao'] = update_data['data_atualizacao']
                        break
            else:
                logger.warning(f"Nenhum registro atualizado para {ticker}")
        except Exception as e:
            logger.error(f"Erro ao atualizar preço para {ticker}: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description='Atualizador de preços via API RTD')
    parser.add_argument('--interval', type=int, default=60,
                      help='Intervalo entre atualizações em segundos (padrão: 60)')
    parser.add_argument('--timeout', type=int, default=20,
                      help='Timeout para receber cotações em segundos (padrão: 20)')
    parser.add_argument('--single-run', action='store_true',
                      help='Executa apenas uma atualização e encerra')
    
    args = parser.parse_args()
    
    INTERVALO = args.interval
    TIMEOUT = args.timeout
    SINGLE_RUN = args.single_run
    
    logger.info("=" * 60)
    logger.info(f"Iniciando atualizador de preços via API RTD")
    if SINGLE_RUN:
        logger.info(f"Modo: Execução única")
    else:
        logger.info(f"Intervalo: {INTERVALO} segundos | Timeout: {TIMEOUT} segundos")
    logger.info(f"API URL: {RTD_API_URL}")
    logger.info("=" * 60)
    
    updater = RTDUpdater(interval_seconds=INTERVALO, timeout=TIMEOUT)
    
    if SINGLE_RUN:
        logger.info("Executando atualização única...")
        updater._solicitar_cotacoes()
        logger.info("Atualização única completada!")
    else:
        updater.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Interrupção detectada. Finalizando...")
        finally:
            updater.stop()

    logger.info("Obrigado por usar o atualizador de preços via API RTD!")


if __name__ == "__main__":
    main()