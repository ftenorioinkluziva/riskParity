import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime
import numpy as np
import argparse

# Load environment variables from .env file
load_dotenv()

def remodelar_tabela_dados_historicos():
    """
    Remodela a tabela dados_historicos:
    
    Removendo as colunas:
    - pico
    - drawdown
    - fechamento_ajustado
    - volume
    
    Adicionando as novas colunas:
    - mm20 (média móvel de 20 períodos)
    - bb2s (banda de Bollinger superior - 2 desvios padrão)
    - bb2i (banda de Bollinger inferior - 2 desvios padrão)
    
    E insere/atualiza dados da planilha Excel
    """
    # Obter credenciais do Supabase do arquivo .env
    SUPABASE_URL = os.environ.get('SUPABASE_URL')
    SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
    
    # Verificar se as credenciais existem
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("\n⚠️ ERRO: Variáveis de ambiente SUPABASE_URL e/ou SUPABASE_KEY não definidas.")
        print("Defina estas variáveis no arquivo .env ou forneça-as como argumentos.")
        print("Valores usados nos outros scripts:")
        print('SUPABASE_URL = "https://dxwebxduuazebqtkumtv.supabase.co"')
        print('SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR4d2VieGR1dWF6ZWJxdGt1bXR2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDE1OTMxMzcsImV4cCI6MjA1NzE2OTEzN30.v53W6iz_BJup66qst03jWqjHzJ0DGKmUC6WrVGLpt-Y"')
        return False
    
    try:
        # Inicializar cliente Supabase
        print("Conectando ao Supabase...")
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Conexão estabelecida com sucesso.")
        
        # Passo 1: Informar ao usuário que ele precisa criar a tabela temporária manualmente
        print("\n⚠️ AVISO: Primeiro você precisa criar manualmente a tabela temporária 'dados_historicos_temp'.")
        print("Execute a seguinte query SQL no SQL Editor do Supabase:")
        print("""
CREATE TABLE IF NOT EXISTS public.dados_historicos_temp (
    id serial NOT NULL,
    ticker text NOT NULL,
    nome_ativo text NOT NULL,
    data date NOT NULL,
    abertura numeric NULL,
    maxima numeric NULL,
    minima numeric NULL,
    fechamento numeric NULL,
    retorno_diario numeric NULL,
    mm20 numeric NULL,
    bb2s numeric NULL,
    bb2i numeric NULL,
    CONSTRAINT dados_historicos_temp_pkey PRIMARY KEY (id),
    CONSTRAINT dados_historicos_temp_ticker_data_key UNIQUE (ticker, data)
);
        """)
        input("Pressione Enter quando a tabela estiver criada para continuar...")
        
        # Passo 2: Ler dados da planilha Excel
        print("\nLendo dados da planilha Excel 'dadoshistoricos.xlsx'...")
        try:
            # Ajustar o caminho conforme necessário
            df = pd.read_excel('dadoshistoricos.xlsx')
            print(f"✅ Leitura concluída. {len(df)} registros encontrados.")
            
            # Exibir primeiras linhas para verificação
            print("\nPrimeiras linhas da planilha:")
            print(df.head())
            
            # Verificar colunas disponíveis
            print("\nColunas disponíveis na planilha:")
            print(df.columns.tolist())
            
            # Mapear colunas da planilha para colunas da tabela
            print("\nMapeando colunas da planilha para a tabela...")
            
            # Este mapeamento precisa ser ajustado com base nas colunas reais da sua planilha
            colunas_mapeadas = {
                'ticker': 'ticker',
                'nome_ativo': 'nome_ativo',
                'data': 'data',
                'abertura': 'abertura',
                'maxima': 'maxima', 
                'minima': 'minima',
                'fechamento': 'fechamento',
                'retorno_diario': 'retorno_diario',
                'mm20': 'mm20',
                'bb2s': 'bb2s',
                'bb2i': 'bb2i'
            }
            
            # Validar se todas as colunas necessárias estão presentes
            colunas_necessarias = ['ticker', 'nome_ativo', 'data']
            colunas_excel = df.columns.tolist()
            
            # Mapeamento automático de colunas (case insensitive)
            for coluna_db, coluna_excel in colunas_mapeadas.items():
                if coluna_excel not in colunas_excel:
                    # Tentar encontrar alternativas
                    alternativas = [col for col in colunas_excel if col.lower() == coluna_excel.lower()]
                    if alternativas:
                        colunas_mapeadas[coluna_db] = alternativas[0]
                    else:
                        if coluna_db in colunas_necessarias:
                            print(f"⚠️ Coluna obrigatória '{coluna_db}' não encontrada na planilha!")
            
            # Passo 3: Migrar dados da planilha para a tabela temporária
            print("\nPreparando dados para inserção...")
            
            # Renomear colunas conforme o mapeamento
            df_renamed = df.rename(columns={v: k for k, v in colunas_mapeadas.items() if v in df.columns})
            
            # Garantir que as colunas necessárias existam
            for coluna in colunas_necessarias:
                if coluna not in df_renamed.columns:
                    print(f"⚠️ Coluna obrigatória '{coluna}' não encontrada na planilha após mapeamento!")
                    return False
            
            # Garantir que todas as colunas da tabela existam no DataFrame
            colunas_tabela = ['ticker', 'nome_ativo', 'data', 'abertura', 'maxima', 'minima', 'fechamento', 
                             'retorno_diario', 'mm20', 'bb2s', 'bb2i']
            for coluna in colunas_tabela:
                if coluna not in df_renamed.columns:
                    print(f"Adicionando coluna '{coluna}' ausente como nula...")
                    df_renamed[coluna] = None
            
            # Converter data para o formato correto (se necessário)
            if 'data' in df_renamed.columns:
                if not pd.api.types.is_datetime64_any_dtype(df_renamed['data']):
                    df_renamed['data'] = pd.to_datetime(df_renamed['data'])
                df_renamed['data'] = df_renamed['data'].dt.strftime('%Y-%m-%d')
            
            # Substituir NaN por None para compatibilidade com Supabase
            df_renamed = df_renamed.replace({np.nan: None})
            
            # Converter para registros
            registros = df_renamed[colunas_tabela].to_dict('records')
            
            # Inserir dados em lotes
            print("\nInserindo dados na tabela temporária...")
            tamanho_lote = 100
            total_lotes = (len(registros) - 1) // tamanho_lote + 1
            
            # Verificar se a tabela temporária existe
            try:
                supabase.table("dados_historicos_temp").select("*").limit(1).execute()
                print("✅ Tabela temporária encontrada. Iniciando inserção de dados...")
                
                for i in range(0, len(registros), tamanho_lote):
                    lote = registros[i:i+tamanho_lote]
                    try:
                        # Usar upsert para inserir ou atualizar registros
                        response = supabase.table("dados_historicos_temp").upsert(
                            lote,
                            on_conflict="ticker,data"
                        ).execute()
                        print(f"  Processado lote {i//tamanho_lote + 1}/{total_lotes}")
                    except Exception as e:
                        print(f"⚠️ Erro ao inserir lote {i//tamanho_lote + 1}: {str(e)}")
                        # Continuar com próximo lote em vez de abortar
                
                # Passo 4: Verificar se a migração foi bem-sucedida
                print("\nVerificando migração dos dados...")
                response = supabase.table("dados_historicos_temp").select("*", count="exact").execute()
                count_temp = response.count if hasattr(response, 'count') else len(response.data)
                
                print(f"  Registros na tabela temporária: {count_temp}")
                print(f"  Registros na planilha Excel: {len(df)}")
                
                # Passo 5: Substituir a tabela original pela temporária
                print("\nSubstituindo tabela original pela nova versão...")
                print("⚠️ ATENÇÃO: Você deve executar manualmente os seguintes comandos SQL:")
                print("1. Fazer backup da tabela original:")
                print("   ALTER TABLE public.dados_historicos RENAME TO dados_historicos_backup;")
                print("2. Renomear a tabela temporária para se tornar a tabela principal:")
                print("   ALTER TABLE public.dados_historicos_temp RENAME TO dados_historicos;")
                print("3. Verificar se tudo está correto e então você pode excluir o backup:")
                print("   DROP TABLE public.dados_historicos_backup;")
                
                print("\n✅ Processo de remodelagem completado com sucesso!")
                
            except Exception as e:
                print(f"⚠️ A tabela temporária 'dados_historicos_temp' não existe ou não está acessível.")
                print(f"⚠️ Erro: {str(e)}")
                print("\nPor favor, crie a tabela conforme instruído acima e tente novamente.")
                return False
            
        except Exception as e:
            print(f"⚠️ Erro ao ler a planilha Excel: {str(e)}")
            return False
        
    except Exception as e:
        print(f"⚠️ Erro durante a remodelagem: {str(e)}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Remodelar tabela dados_historicos')
    
    args = parser.parse_args()
    
    print("\n🚀 Iniciando remodelagem da tabela dados_historicos...\n")
    remodelar_tabela_dados_historicos()
    