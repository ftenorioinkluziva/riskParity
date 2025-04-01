import os
from supabase import create_client
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

# Carregar variáveis do arquivo .env
load_dotenv()

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

def migrar_tabela_ativos():
    """
    Migra a tabela 'ativos' para o novo formato sem os campos calculados
    
    1. Cria uma tabela temporária com a nova estrutura
    2. Copia os dados necessários da tabela antiga para a nova
    3. Renomeia as tabelas para completar a migração
    """
    try:
        # Conectar ao Supabase
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Conexão com Supabase estabelecida.")
        
        # Verificar se a tabela 'ativos' existe
        print("Verificando se a tabela 'ativos' existe...")
        
        response = supabase.table('ativos').select('*').limit(1).execute()
        if not response.data:
            print("⚠️ A tabela 'ativos' não existe ou está vazia. Nada a migrar.")
            return
        
        print("✅ Tabela 'ativos' encontrada.")
        
        # 1. Obter todos os dados da tabela atual
        print("Obtendo dados da tabela atual...")
        response = supabase.table('ativos').select('*').execute()
        dados_atuais = response.data
        
        if not dados_atuais:
            print("⚠️ A tabela 'ativos' está vazia. Nada a migrar.")
            return
        
        print(f"✅ Obtidos {len(dados_atuais)} registros da tabela 'ativos'")
        
        # 2. Criar tabela temporária com a nova estrutura
        # Nota: Como não é possível executar DDL diretamente pelo cliente Supabase,
        # você precisará criar essa tabela manualmente no console do Supabase
        # ou através de uma ferramenta SQL separada.
        print("\n⚠️ AVISO: Você precisa criar manualmente uma tabela chamada 'ativos_nova' no Supabase")
        print("com a seguinte estrutura:")
        print("- id (bigint, auto-incremento, chave primária)")
        print("- ticker (texto, não nulo, único)")
        print("- nome (texto, não nulo)")
        print("- preco_atual (numeric, não nulo)")
        print("- data_atualizacao (timestamp, não nulo)")
        print("\nPressione Enter quando a tabela estiver criada, ou 'q' para sair: ", end="")
        resposta = input()
        
        if resposta.lower() == 'q':
            print("Operação cancelada pelo usuário.")
            return
        
        # 3. Migrar dados para a nova tabela
        print("\nMigrando dados para a nova tabela 'ativos_nova'...")
        
        # Preparar dados para migração, mantendo apenas os campos necessários
        dados_migrados = []
        for ativo in dados_atuais:
            # Filtrar apenas os campos necessários
            ativo_migrado = {
                'ticker': ativo['ticker'],
                'nome': ativo['nome'],
                'preco_atual': ativo['preco_atual'],
                'data_atualizacao': ativo.get('data_atualizacao') or datetime.now().isoformat()
            }
            dados_migrados.append(ativo_migrado)
        
        # Inserir dados na nova tabela
        for ativo in dados_migrados:
            supabase.table('ativos_nova').upsert(ativo, on_conflict='ticker').execute()
        
        print(f"✅ Migrados {len(dados_migrados)} registros para a tabela 'ativos_nova'")
        
        # 4. Instruções para renomear tabelas no Supabase
        print("\n⚠️ AVISO: Para finalizar a migração, execute os seguintes passos no SQL Editor do Supabase:")
        print("1. Faça backup da tabela antiga: ALTER TABLE ativos RENAME TO ativos_backup;")
        print("2. Renomeie a nova tabela: ALTER TABLE ativos_nova RENAME TO ativos;")
        print("3. Verifique se tudo está correto e então você pode excluir a tabela backup:")
        print("   DROP TABLE ativos_backup;")
        
        print("\n✅ Processo de migração concluído com sucesso!")
        
    except Exception as e:
        print(f"⚠️ Erro durante a migração: {str(e)}")

if __name__ == "__main__":
    print("\n🚀 Iniciando migração da tabela 'ativos'...\n")
    migrar_tabela_ativos()