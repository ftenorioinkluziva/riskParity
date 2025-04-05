import threading
import time
from queue import Queue, Empty
import cd3_connector
import logging
import sys
import os
import argparse
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

# Configurações do Supabase
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://dxwebxduuazebqtkumtv.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR4d2VieGR1dWF6ZWJxdGt1bXR2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDE1OTMxMzcsImV4cCI6MjA1NzE2OTEzN30.v53W6iz_BJup66qst03jWqjHzJ0DGKmUC6WrVGLpt-Y')

# Credenciais para o CD3 Connector
CD3_USERNAME = os.environ.get('CD3_USERNAME', 'fernando_cd3_python')
CD3_PASSWORD = os.environ.get('CD3_PASSWORD', 'c3&Rss')

# Configuração de logging
LOG_FILENAME = "preco_update.log"
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
logger = logging.getLogger("atualizador_precos")

class CedroUpdater:
    def __init__(self, user, password, interval_seconds=60, timeout=20):
        """
        Inicializa o atualizador de preços usando o CD3 Connector
        
        Args:
            user (str): Nome de usuário da Cedro
            password (str): Senha da Cedro
            interval_seconds (int): Intervalo entre atualizações em segundos
            timeout (int): Tempo máximo para receber cotações em segundos
        """
        # Armazenar parâmetros
        self.user = user
        self.password = password
        self.interval_seconds = interval_seconds
        self.timeout = timeout
        
        # Configurar log path
        self.log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs_cd3")
        os.makedirs(self.log_path, exist_ok=True)
        
        # Inicializar variáveis
        self._conn = None
        self._queue = Queue()
        self._running = False
        self._signal = threading.Event()
        self._atualizacoes_recebidas = 0
        self._atualizacoes_esperadas = 0
        self._tempo_inicio = 0
        self._ultima_atualizacao = 0
        self._connected = False
        
        # Conexão com Supabase
        try:
            self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            logger.info("Conexão com Supabase estabelecida com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao conectar com o Supabase: {str(e)}")
            self.supabase = None
            
        # Dicionário para armazenar a correspondência entre símbolos da Cedro e tickers da B3
        self.ticker_map = {}
        
        # Carregar lista de ativos do banco de dados
        self.ativos = []
        self.tickers_cedro = []
        self._load_ativos()
    
    def _load_ativos(self):
        """Carrega a lista de ativos do banco de dados Supabase"""
        if not self.supabase:
            logger.error("Supabase não inicializado. Não foi possível carregar ativos.")
            return
            
        try:
            response = self.supabase.table('ativos').select('*').execute()
            self.ativos = response.data
            
            # Tabela de equivalência específica entre tickers do banco e tickers Cedro
            equivalencia_tickers = {
                'BOVA11.SA': 'BOVA11',
                'XFIX11.SA': 'XFIX11',
                'IB5M11.SA': 'IB5M11',
                'B5P211.SA': 'B5P211',
                'FIXA11.SA': 'FIXA11',
                'USDBRL=X': 'USDBRL'
            }
            
            # Criar mapeamento para os tickers
            for ativo in self.ativos:
                ticker_banco = ativo['ticker']
                # Usar a tabela de equivalência, se disponível
                if ticker_banco in equivalencia_tickers:
                    ticker_cedro = equivalencia_tickers[ticker_banco]
                else:
                    # Para outros casos, aplicar a regra geral (remover sufixos)
                    ticker_cedro = ticker_banco.split('.')[0].replace('=', '')
                
                # Armazenar o mapeamento nos dois sentidos
                self.ticker_map[ticker_cedro] = ticker_banco
                
                logger.debug(f"Mapeamento: {ticker_banco} -> {ticker_cedro}")
                
            logger.info(f"Carregados {len(self.ativos)} ativos do banco de dados.")
            
            # Filtrar tickers disponíveis na Cedro (excluir CDI e USDBRL)
            self.tickers_cedro = [
                cedro for cedro, _ in self.ticker_map.items() 
                if cedro not in ['CDI', 'USDBRL']
            ]
            self._atualizacoes_esperadas = len(self.tickers_cedro)
            
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
                # Registrar início do ciclo
                self._tempo_inicio = time.time()
                now = time.strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"Iniciando ciclo de atualização em {now}")
                
                # Reiniciar contadores
                self._atualizacoes_recebidas = 0
                self._queue = Queue()
                
                # Iniciar conexão
                self._iniciar_conexao()
                
                # Enviar comandos de consulta
                self._solicitar_cotacoes()
                
                # Aguardar as cotações
                self._aguardar_cotacoes()
                
                # Finalizar conexão
                self._finalizar_conexao()
                
                # Registrar fim do ciclo
                elapsed = time.time() - self._tempo_inicio
                logger.info(f"Ciclo de atualização concluído em {elapsed:.1f} segundos")
                logger.info(f"Atualizações recebidas: {self._atualizacoes_recebidas}/{self._atualizacoes_esperadas}")
                
                # Atualizar timestamp da última atualização
                self._ultima_atualizacao = time.time()
                
                # Calcular tempo até a próxima atualização
                next_update = self.interval_seconds - elapsed
                if next_update > 0:
                    next_time = time.strftime("%H:%M:%S", time.localtime(time.time() + next_update))
                    logger.info(f"Próxima atualização às {next_time} ({next_update:.1f} segundos)")
                    
                    # Aguardar até o próximo ciclo, a menos que seja solicitado para parar
                    self._signal.wait(next_update)
                    
                    # Se o sinal foi disparado, verificar se ainda estamos em execução
                    if not self._running:
                        break
                else:
                    logger.warning("Ciclo de atualização demorou mais que o intervalo configurado")
                    
        except Exception as e:
            logger.error(f"Erro no loop de atualização: {str(e)}")
        finally:
            logger.info("Loop de atualização encerrado")
    
    def _iniciar_conexao(self):
        """Inicia uma nova conexão com o servidor CD3"""
        try:
            # Adicionar um sufixo único ao usuário para evitar erro de 'Duplicate Login'
            # Isso ajuda quando a conexão anterior não foi encerrada corretamente
            timestamp = int(time.time()) % 10000  # Um número entre 0 e 9999
            username_com_sufixo = f"{self.user}_{timestamp}"
            
            logger.info(f"Conectando com o usuário {username_com_sufixo}")
            
            # Inicializar conexão
            self._conn = cd3_connector.CD3Connector(
                username_com_sufixo, self.password, 
                self._on_disconnect, 
                self._on_message, 
                self._on_connect,
                log_level=logging.WARNING,  # Reduzir o nível de log para evitar poluição
                log_path=self.log_path
            )
            
            # Iniciar conexão
            self._conn.start()
            
            # Aguardar conexão
            start_time = time.time()
            while not self._connected:
                time.sleep(0.1)
                if time.time() - start_time > 10:  # Timeout após 10 segundos
                    logger.error("Timeout ao aguardar conexão")
                    break
            
            # Aguardar um pouco para a conexão estabilizar
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Erro ao iniciar conexão CD3: {str(e)}")
    
    def _finalizar_conexao(self):
        """Finaliza a conexão com o servidor CD3"""
        try:
            if self._conn:
                self._conn.stop()
                self._conn = None
                logger.info("Conexão CD3 encerrada")
        except Exception as e:
            logger.error(f"Erro ao finalizar conexão CD3: {str(e)}")
    
    def _solicitar_cotacoes(self):
        """Solicita cotações para todos os ativos usando o modo snapshot (N)"""
        if not self._conn:
            logger.error("Não foi possível solicitar cotações - conexão CD3 não estabelecida")
            return
            
        try:
            for ticker_cedro in self.tickers_cedro:
                try:
                    # Usar o modo snapshot com o parâmetro N
                    logger.info(f"Solicitando snapshot para {ticker_cedro}")
                    self._conn.send_command(f"SQT {ticker_cedro} N")
                    # Pequena pausa para não sobrecarregar o servidor
                    time.sleep(0.3)
                except Exception as e:
                    logger.error(f"Erro ao solicitar cotação para {ticker_cedro}: {str(e)}")
        except Exception as e:
            logger.error(f"Erro ao solicitar cotações: {str(e)}")
    
    def _aguardar_cotacoes(self):
        """Aguarda o recebimento das cotações por um tempo máximo de timeout"""
        try:
            timeout_time = time.time() + self.timeout
            
            while time.time() < timeout_time and self._atualizacoes_recebidas < self._atualizacoes_esperadas:
                # Processar mensagens recebidas
                try:
                    msg = self._queue.get(timeout=0.5)
                    self._process_message(msg)
                    self._queue.task_done()
                except Empty:
                    # Timeout da fila, verificar se atingimos o timeout total
                    if time.time() >= timeout_time:
                        logger.warning(f"Timeout atingido ao aguardar cotações")
                        break
                except Exception as e:
                    logger.error(f"Erro ao processar mensagem da fila: {str(e)}")
            
            # Verificar se recebemos todas as cotações esperadas
            if self._atualizacoes_recebidas < self._atualizacoes_esperadas:
                logger.warning(f"Não recebemos todas as cotações esperadas: {self._atualizacoes_recebidas}/{self._atualizacoes_esperadas}")
                
        except Exception as e:
            logger.error(f"Erro ao aguardar cotações: {str(e)}")
    
    def _on_connect(self):
        """Callback executado quando a conexão com CD3 é estabelecida"""
        logger.info("Conectado ao servidor CD3")
        self._connected = True
    
    def _on_disconnect(self):
        """Callback executado quando a conexão com CD3 é perdida"""
        logger.warning("Desconectado do servidor CD3")
        self._connected = False
    
    def _on_message(self, msg: str):
        """
        Callback executado quando uma mensagem é recebida do CD3
        
        Args:
            msg (str): Mensagem recebida
        """
        # Adicionar mensagem à fila para processamento
        if msg and msg.strip():
            self._queue.put(msg)
    
    def _process_message(self, msg: str):
        """
        Processa uma mensagem recebida do CD3
        
        Args:
            msg (str): Mensagem recebida
        """
        try:
            # Verificar mensagens de erro
            if msg.startswith("E:"):
                logger.warning(f"Erro na cotação: {msg}")
                return
                
            # Processar mensagem de cotação
            self._process_quote_message(msg)
                
        except Exception as e:
            logger.error(f"Erro ao processar mensagem: {str(e)}")
            logger.error(f"Mensagem: {msg}")
    
    def _process_quote_message(self, msg: str):
        """
        Processa uma mensagem de cotação do CD3 e atualiza o banco de dados
        
        Args:
            msg (str): Mensagem de cotação
        """
        try:
            # Verifica se a mensagem contém dados de cotação
            if not msg or len(msg) < 10:
                return
                
            # Verificar se é uma resposta no formato T: (Tick) conforme documentação Cedro
            # Formato: T:TICKER:TIMESTAMP:1:DATA:2:LAST_PRICE:3:BID:4:ASK:...
            if msg.startswith("T:"):
                # Dividir a mensagem por :
                parts = msg.split(":")
                
                # O ticker está na posição 1 (após o T:)
                if len(parts) < 2:
                    return
                    
                symbol = parts[1]
                
                # De acordo com a documentação, o índice 2 é o preço do último negócio
                # Procuramos pelo campo ":2:" na mensagem
                if ":2:" in msg:
                    # Encontrar o índice onde começa ":2:"
                    field_marker = ":2:"
                    idx = msg.find(field_marker)
                    if idx == -1:
                        return
                        
                    # O valor começa após ":2:"
                    idx += len(field_marker)
                    
                    # O valor termina no próximo ":"
                    end_idx = msg.find(":", idx)
                    if end_idx == -1:
                        end_idx = len(msg)
                        
                    # Extrair e converter o valor
                    try:
                        price_str = msg[idx:end_idx]
                        last_price = float(price_str)
                        
                        # Verificar se o preço é válido
                        if last_price <= 0:
                            logger.warning(f"Preço inválido para {symbol}: {last_price}")
                            return
                            
                        # Encontrar o ticker completo (com sufixo) no mapeamento
                        original_ticker = self.ticker_map.get(symbol)
                        
                        if not original_ticker:
                            logger.warning(f"Ticker não encontrado no mapeamento: {symbol}")
                            return
                        
                        logger.info(f"Preço de {symbol} ({original_ticker}): R$ {last_price:.2f}")
                        
                        # Atualizar o preço no banco de dados
                        self._update_price(original_ticker, last_price)
                        
                        # Incrementar contador de atualizações recebidas
                        self._atualizacoes_recebidas += 1
                        
                    except ValueError:
                        logger.warning(f"Não foi possível converter o preço: {msg[idx:end_idx]}")
                else:
                    # Se não encontrar o campo 2, é uma atualização parcial
                    logger.debug(f"Mensagem de atualização parcial ignorada: {msg}")
                    
        except Exception as e:
            logger.error(f"Erro ao processar mensagem de cotação: {str(e)}")
            logger.error(f"Mensagem: {msg}")
    
    def _update_price(self, ticker: str, price: float):
        """
        Atualiza o preço de um ativo no banco de dados
        
        Args:
            ticker (str): Ticker do ativo
            price (float): Novo preço
        """
        if not self.supabase:
            logger.error("Supabase não inicializado. Não é possível atualizar preços.")
            return
            
        try:
            # Atualizar o preço e a data de atualização
            update_data = {
                'preco_atual': price,
                'data_atualizacao': datetime.now().isoformat()
            }
            
            # Executar atualização
            response = self.supabase.table('ativos').update(update_data).eq('ticker', ticker).execute()
            
            if response.data and len(response.data) > 0:
                logger.info(f"Preço atualizado para {ticker}: R$ {price:.2f}")
                
                # Atualizar o objeto local também para manter consistência
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
    # Criar parser para argumentos de linha de comando
    parser = argparse.ArgumentParser(description='Atualizador de preços via Cedro CD3')
    parser.add_argument('--interval', type=int, default=60,
                      help='Intervalo entre atualizações em segundos (padrão: 60)')
    parser.add_argument('--timeout', type=int, default=20,
                      help='Timeout para receber cotações em segundos (padrão: 20)')
    parser.add_argument('--single-run', action='store_true',
                      help='Executa apenas uma atualização e encerra')
    
    args = parser.parse_args()
    
    # Configurar intervalo e timeout
    INTERVALO = args.interval
    TIMEOUT = args.timeout
    SINGLE_RUN = args.single_run
    
    logger.info("=" * 60)
    logger.info(f"Iniciando atualizador de preços via Cedro CD3")
    if SINGLE_RUN:
        logger.info(f"Modo: Execução única")
    else:
        logger.info(f"Intervalo: {INTERVALO} segundos | Timeout: {TIMEOUT} segundos")
    logger.info(f"Usuário: {CD3_USERNAME}")
    logger.info("=" * 60)
    
    # Aviso sobre tickers não disponíveis
    logger.info("Aviso: CDI e USDBRL não estão disponíveis na Cedro e serão ignorados")
    
    # Criar o atualizador
    updater = CedroUpdater(CD3_USERNAME, CD3_PASSWORD, interval_seconds=INTERVALO, timeout=TIMEOUT)
    
    if SINGLE_RUN:
        # Execução única - não iniciar o loop
        logger.info("Executando atualização única...")
        
        # Iniciar conexão
        updater._iniciar_conexao()
        
        if updater._conn:  # Verificar se a conexão foi estabelecida com sucesso
            # Enviar comandos de consulta
            updater._solicitar_cotacoes()
            
            # Aguardar as cotações
            updater._aguardar_cotacoes()
            
            # Finalizar conexão
            updater._finalizar_conexao()
            
            logger.info("Atualização única completada!")
        else:
            logger.error("Falha ao estabelecer conexão. Atualização cancelada.")
    else:
        # Modo loop contínuo
        updater.start()
        
        try:
            # Manter o programa em execução até Ctrl+C
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Interrupção detectada. Finalizando...")
        finally:
            # Garantir que o atualizador seja corretamente finalizado
            updater.stop()
    
    logger.info("Obrigado por usar o atualizador de preços via Cedro CD3!")


if __name__ == "__main__":
    main()