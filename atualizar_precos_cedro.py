import threading
import time
from queue import Queue, Empty
import cd3_connector
import logging
from supabase import create_client
import os
from dotenv import load_dotenv
import json

# Carregar variáveis do arquivo .env
load_dotenv()

# Configurações do Supabase
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# Verificar se as variáveis de ambiente estão definidas
if not SUPABASE_URL or not SUPABASE_KEY:
    print("\n⚠️ AVISO: Variáveis de ambiente SUPABASE_URL e/ou SUPABASE_KEY não definidas.")
    print("Defina estas variáveis ou insira os valores diretamente no código (apenas para desenvolvimento):\n")
    # Fallback para valores hardcoded (apenas para desenvolvimento)
    SUPABASE_URL = 'https://dxwebxduuazebqtkumtv.supabase.co'
    SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR4d2VieGR1dWF6ZWJxdGt1bXR2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDE1OTMxMzcsImV4cCI6MjA1NzE2OTEzN30.v53W6iz_BJup66qst03jWqjHzJ0DGKmUC6WrVGLpt-Y'

# Credenciais para o CD3 Connector
CD3_USERNAME = os.environ.get('CD3_USERNAME', 'fernando_cd3_python')
CD3_PASSWORD = os.environ.get('CD3_PASSWORD', 'c3&Rss')
CD3_SERVER = os.environ.get('CD3_SERVER', 'datafeed1.cedrotech.com')
CD3_PORT = os.environ.get('CD3_PORT', '81')

class CedroUpdater:
    def __init__(self, user: str, password: str):
        """
        Inicializa o atualizador de preços usando o CD3 Connector
        
        Args:
            user (str): Nome de usuário da Cedro
            password (str): Senha da Cedro
        """
        # Configurar logging
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs_cd3")
        os.makedirs(log_path, exist_ok=True)
        
        # Inicializar conexão com CD3
        # A versão da biblioteca que você está usando parece não aceitar os parâmetros server e port
        # diretamente no construtor. Vamos usar os parâmetros básicos.
        self._conn = cd3_connector.CD3Connector(
            user, password, 
            self._on_disconnect, 
            self._on_message, 
            self._on_connect,
            log_level=logging.INFO,
            log_path=log_path
        )
        
        # Se a biblioteca tiver método para configurar servidor e porta, usaríamos assim:
        # self._conn.set_server(CD3_SERVER, int(CD3_PORT))  # Descomente se disponível na sua versão
        
        # Fila para processamento assíncrono de mensagens
        self._queue = Queue()
        self._consumer = threading.Thread(target=self._process_messages)
        self._signal = threading.Event()
        self._restart_conn = True
        self._running = False
        
        # Conexão com Supabase
        try:
            self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            print("✅ Conexão com Supabase estabelecida com sucesso.")
        except Exception as e:
            print(f"⚠️ Erro ao conectar com o Supabase: {str(e)}")
            self.supabase = None
            
        # Dicionário para armazenar a correspondência entre símbolos da Cedro e tickers da B3
        self.ticker_map = {}
        
        # Carregar lista de ativos do banco de dados
        self.ativos = []
        self._load_ativos()
        
        # Iniciar o thread de processamento
        self._consumer.start()
    
    def _load_ativos(self):
        """Carrega a lista de ativos do banco de dados Supabase"""
        if not self.supabase:
            print("⚠️ Supabase não inicializado. Não foi possível carregar ativos.")
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
                'USDBRL=X': 'USD/BRL'
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
                
                print(f"Mapeamento: {ticker_banco} -> {ticker_cedro}")
                
            print(f"✅ Carregados {len(self.ativos)} ativos do banco de dados.")
        except Exception as e:
            print(f"⚠️ Erro ao carregar ativos: {str(e)}")
    
    def start(self):
        """Inicia a conexão com o servidor CD3"""
        if not self._running:
            self._running = True
            self._conn.start()
    
    def stop(self):
        """Finaliza a conexão e os threads de processamento"""
        self._running = False
        self._restart_conn = False
        self._signal.set()
        
        try:
            self._conn.stop()
        except Exception as e:
            print(f"Erro ao desconectar: {str(e)}")
            
        self._consumer.join(timeout=5.0)
        print("✅ Atualização de preços finalizada.")
    
    def _on_connect(self):
        """Callback executado quando a conexão com CD3 é estabelecida"""
        print("✅ Conectado ao servidor CD3.")
        
        # Aguardar um curto período antes de enviar comandos
        time.sleep(1)
        
        # Solicitar cotações para cada ativo
        for ativo in self.ativos:
            # Obter o símbolo correspondente na Cedro
            ticker_banco = ativo['ticker']
            ticker_cedro = None
            
            # Procurar o ticker Cedro correspondente ao ticker do banco
            for cedro_ticker, db_ticker in self.ticker_map.items():
                if db_ticker == ticker_banco:
                    ticker_cedro = cedro_ticker
                    break
            
            if not ticker_cedro:
                print(f"⚠️ Não foi possível determinar o ticker Cedro para {ticker_banco}")
                continue
                
            print(f"📊 Solicitando cotação para {ticker_cedro}...")
            self._conn.send_command(f"SQT {ticker_cedro} N")
            # Aguardar um curto período para não sobrecarregar o servidor
            time.sleep(0.5)
    
    def _on_disconnect(self):
        """Callback executado quando a conexão com CD3 é perdida"""
        print("⚠️ Desconectado do servidor CD3.")
        
        # Tentar reconectar automaticamente se necessário
        if self._restart_conn and self._running:
            print("🔄 Tentando reconectar...")
            time.sleep(5)  # Esperar 5 segundos antes de tentar reconectar
            try:
                self._conn.start()
            except RuntimeError as e:
                print(f"⚠️ Erro ao reconectar: {str(e)}")
                print("Criando nova instância do conector...")
                
                # Criar nova instância do conector se a anterior foi destruída
                log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs_cd3")
                self._conn = cd3_connector.CD3Connector(
                    CD3_USERNAME, CD3_PASSWORD, 
                    self._on_disconnect, 
                    self._on_message, 
                    self._on_connect,
                    log_level=logging.INFO,
                    log_path=log_path
                )
                self._conn.start()
    
    def _on_message(self, msg: str):
        """
        Callback executado quando uma mensagem é recebida do CD3
        
        Args:
            msg (str): Mensagem recebida
        """
        # Adicionar mensagem à fila para processamento
        if msg and msg.strip():
            self._queue.put(msg)
    
    def _process_messages(self):
        """Processa mensagens da fila em um thread separado"""
        while not self._signal.is_set():
            try:
                # Obter mensagem da fila (bloqueia até haver mensagem)
                msg = self._queue.get(timeout=1.0)
                
                # Verificar mensagens de erro
                if (msg.lower() == "invalid login.") or \
                   (msg.lower() == "software key not found.") or \
                   (msg.lower() == "you don't have any permission for this software."):
                    print(f"❌ Erro de autenticação: {msg}")
                    self._restart_conn = False
                    self._running = False
                    break
                
                # Processar mensagem de cotação
                self._process_quote_message(msg)
                
                # Marcar tarefa como concluída
                self._queue.task_done()
                
            except Exception as e:
                # Timeout da fila ou outros erros
                if isinstance(e, Empty):
                    continue
                print(f"⚠️ Erro ao processar mensagem: {str(e)}")
    
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
                
            # Log da mensagem para debug
            logging.debug(f"Mensagem recebida: {msg}")
            
            # Verificar se é uma mensagem de erro
            if msg.startswith("E:"):
                print(f"⚠️ Erro na cotação: {msg}")
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
                            print(f"⚠️ Preço inválido para {symbol}: {last_price}")
                            return
                            
                        # Encontrar o ticker completo (com sufixo) no mapeamento
                        original_ticker = self.ticker_map.get(symbol)
                        
                        if not original_ticker:
                            print(f"⚠️ Ticker não encontrado no mapeamento: {symbol}")
                            return
                        
                        print(f"💲 Preço de {symbol} ({original_ticker}): R$ {last_price:.2f}")
                        
                        # Atualizar o preço no banco de dados
                        self._update_price(original_ticker, last_price)
                    except ValueError:
                        print(f"⚠️ Não foi possível converter o preço: {msg[idx:end_idx]}")
                else:
                    # Se não encontrar o campo 2, é uma atualização parcial - ignorar silenciosamente
                    # Estas são atualizações de outros campos que não o preço
                    logging.debug(f"Mensagem de atualização parcial ignorada: {msg}")
                    
        except Exception as e:
            print(f"⚠️ Erro ao processar mensagem de cotação: {str(e)}")
            print(f"Mensagem: {msg}")
                
        except Exception as e:
            print(f"⚠️ Erro ao processar mensagem de cotação: {str(e)}")
            print(f"Mensagem: {msg}")
    
    def _update_price(self, ticker: str, price: float):
        """
        Atualiza o preço de um ativo no banco de dados
        
        Args:
            ticker (str): Ticker do ativo
            price (float): Novo preço
        """
        if not self.supabase:
            print("⚠️ Supabase não inicializado. Não é possível atualizar preços.")
            return
            
        try:
            # Atualizar o preço e a data de atualização
            update_data = {
                'preco_atual': price,
                'data_atualizacao': time.strftime('%Y-%m-%dT%H:%M:%S')
            }
            
            # Executar atualização
            response = self.supabase.table('ativos').update(update_data).eq('ticker', ticker).execute()
            
            if response.data and len(response.data) > 0:
                print(f"✅ Preço atualizado para {ticker}: R$ {price:.2f}")
                
                # Atualizar o objeto local também para manter consistência
                for ativo in self.ativos:
                    if ativo['ticker'] == ticker:
                        ativo['preco_atual'] = price
                        ativo['data_atualizacao'] = update_data['data_atualizacao']
                        break
            else:
                print(f"⚠️ Nenhum registro atualizado para {ticker}")
                
        except Exception as e:
            print(f"⚠️ Erro ao atualizar preço para {ticker}: {str(e)}")


def main():
    print("\n🚀 Iniciando atualizador de preços via Cedro CD3...")
    print(f"Usuário: {CD3_USERNAME}")
    print(f"Nota: Os dados de servidor ({CD3_SERVER}:{CD3_PORT}) são configurados internamente pela biblioteca.")
    
    # Tickers não disponíveis na Cedro
    print("\n⚠️ Aviso: CDI e USDBRL não estão disponíveis na Cedro e gerarão erros de consulta")
    print("   Estes ativos devem ser atualizados por outras fontes (ex: script.py existente)\n")
    
    # Criar e iniciar o atualizador
    updater = CedroUpdater(CD3_USERNAME, CD3_PASSWORD)
    updater.start()
    
    # Exibir instruções para o usuário
    print("\n🔥 Atualizador de preços está rodando.")
    print("🔄 Os preços serão atualizados automaticamente quando recebermos cotações.")
    print("⌨️  Pressione Ctrl+C para finalizar o programa.\n")
    
    try:
        # Manter o programa em execução
        while updater._running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Interrupção detectada. Finalizando...")
    finally:
        # Garantir que o updater seja corretamente finalizado
        updater.stop()
        
    print("👋 Obrigado por usar o atualizador de preços via Cedro CD3!")


if __name__ == "__main__":
    main()