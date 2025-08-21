import sqlite3
import pandas as pd
from fastapi import FastAPI, HTTPException, UploadFile, File, Query
from typing import Dict, Any, List, Optional
import io

# Inicializa a aplicação FastAPI
app = FastAPI(
    title="Desafio Técnico - Solução Final",
    description="API completa para upload, consulta e análise de métricas.",
    version="1.0.0"
)

# --- Funções de processamento de dados ---

def get_processed_data(db_file: str) -> pd.DataFrame:
    """
    Conecta-se ao banco de dados, extrai e unifica os dados das tabelas de processos,
    e depois processa a coluna 'metrics'.
    """
    try:
        conn = sqlite3.connect(db_file)
        
        # Lê os dados de cada tabela para um DataFrame do pandas
        df_tabela1 = pd.read_sql_query("SELECT * FROM processes1", conn)
        df_tabela2 = pd.read_sql_query("SELECT * FROM processes2", conn)
        df_tabela3 = pd.read_sql_query("SELECT * FROM processes3", conn)
        
        conn.close()
        
        # 1. Unifica os dados
        df_unificado = pd.concat([df_tabela1, df_tabela2, df_tabela3], ignore_index=True)
        
        # 2. Processa a coluna 'metrics'
        # Define os nomes das métricas na ordem correta
        metric_names = ['timestamp', 'usagetime', 'delta_cpu_time', 'cpu_usage', 'rx_data', 'tx_data']
        
        # Cria uma nova coluna temporária, dividindo a string 'metrics' por ';' e depois por '::'
        # e criando um DataFrame com as novas colunas
        metrics_df = df_unificado['metrics'].str.split(';').str[0].str.split('::', expand=True)
        metrics_df.columns = metric_names
        
        # Converte os tipos de dados conforme especificado
        metrics_df['cpu_usage'] = pd.to_numeric(metrics_df['cpu_usage'], errors='coerce')
        metrics_df['rx_data'] = pd.to_numeric(metrics_df['rx_data'], errors='coerce', downcast='integer')
        metrics_df['tx_data'] = pd.to_numeric(metrics_df['tx_data'], errors='coerce', downcast='integer')
        
        # Une as novas colunas ao DataFrame original e remove a coluna 'metrics'
        df_final = pd.concat([df_unificado.drop(columns=['metrics']), metrics_df], axis=1)

        # Remove linhas com valores nulos resultantes da conversão
        df_final = df_final.dropna(subset=['timestamp'])

        return df_final

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Arquivo '{db_file}' não encontrado.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar dados: {str(e)}")

# --- Endpoints da API ---

@app.get("/")
def raiz():
    """Retorna uma mensagem de boas-vindas."""
    return {"message": "API rodando. Acesse a documentação em /docs para interagir com o banco de dados."}

@app.post("/upload-db/")
async def upload_arquivo(file: UploadFile = File(...)):
    """
    Faz o upload de um arquivo .sqlite para o servidor e o armazena.
    O arquivo é salvo na pasta Base Teste Prova e substitui um arquivo existente.
    """
    if file.filename.endswith('.sqlite'):
        db_caminho = f"Base Teste Prova/{file.filename}"
        with open(db_caminho, "wb") as f:
            f.write(await file.read())
        return {"message": f"Arquivo '{file.filename}' carregado com sucesso."}
    else:
        raise HTTPException(status_code=400, detail="Apenas arquivos .sqlite são permitidos.")

@app.get("/processes/")
def get_processes(start: Optional[int] = Query(None, description="Timestamp inicial (em milissegundos)"),
                  end: Optional[int] = Query(None, description="Timestamp final (em milissegundos)")):
    """
    Retorna processos em um intervalo de tempo, já tratados e consolidados.
    Se 'start' e 'end' não forem fornecidos, todos os registros são retornados.
    """
    db_file = "Base Teste Prova/live.sqlite"
    df_final = get_processed_data(db_file)
    
    # Converte a coluna timestamp para numérico para garantir a comparação
    df_final['timestamp'] = pd.to_numeric(df_final['timestamp'], errors='coerce')
    
    # Aplica o filtro de tempo se os parâmetros start e end forem fornecidos
    if start is not None and end is not None:
        df_filtrado = df_final[
            (df_final['timestamp'] >= start) & 
            (df_final['timestamp'] <= end)
        ]
        data_to_return = df_filtrado
    else:
        data_to_return = df_final

    if data_to_return.empty:
        return {"message": "Nenhum registro encontrado para o intervalo de tempo fornecido."}
    
    # Converte o DataFrame para o formato JSON esperado
    json_response = data_to_return.to_dict(orient='records')
    return {"data": json_response}
