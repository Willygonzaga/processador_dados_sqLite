import sqlite3
import pandas as pd
from fastapi import FastAPI, HTTPException, UploadFile, File
from typing import Dict, Any, List

# Inicializa a aplicação FastAPI
app = FastAPI(
    title="API de Desafio - Dados SQLite",
    description="API para upload de banco de dados e consulta de processos por tempo.",
    version="1.0.0"
)

# --- Função para extrair e unificar dados ---
# Refatoramos a lógica de banco de dados para uma função separada.
def get_processed_data(db_file: str):
    """
    Conecta-se ao banco de dados, extrai e unifica os dados das tabelas de processos.
    """
    try:
        conn = sqlite3.connect(db_file)
        
        df_tabela1 = pd.read_sql_query("SELECT * FROM processes1", conn)
        df_tabela2 = pd.read_sql_query("SELECT * FROM processes2", conn)
        df_tabela3 = pd.read_sql_query("SELECT * FROM processes3", conn)
        
        conn.close()
        
        df_unificado = pd.concat([df_tabela1, df_tabela2, df_tabela3], ignore_index=True)
        return df_unificado

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Arquivo '{db_file}' não encontrado.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar dados: {str(e)}")

# --- Endpoints da API ---

@app.get("/")
def read_root():
    return {"message": "API rodando. Use os endpoints para interagir com o banco de dados."}

# Rota antiga para consulta dos dados unificados
@app.get("/get-data/", response_model=Dict[str, Any])
def get_unified_data_endpoint():
    db_file = "Base Teste Prova/live.sqlite"
    df_unificado = get_processed_data(db_file)
    unified_data_json = df_unificado.to_dict(orient='records')
    return {"data": unified_data_json}

# NOVO: Rota para upload do arquivo SQLite
@app.post("/upload-db/")
async def upload_sqlite_file(file: UploadFile = File(...)):
    """
    Faz o upload de um arquivo .sqlite e o salva no servidor.
    O arquivo antigo será substituído.
    """
    if file.filename.endswith('.sqlite'):
        db_file_path = f"Base Teste Prova/{file.filename}"
        with open(db_file_path, "wb") as f:
            f.write(await file.read())
        return {"message": f"Arquivo '{file.filename}' carregado com sucesso."}
    else:
        raise HTTPException(status_code=400, detail="Apenas arquivos .sqlite são permitidos.")

# NOVO: Rota de consulta com filtro de tempo
@app.get("/processes/")
def get_data_by_time_range(start_timestamp: int, end_timestamp: int):
    """
    Consulta dados unificados de um intervalo de tempo (timestamp em milissegundos).
    A coluna 'ByteSize' está sendo usada como o timestamp para este exemplo.
    """
    db_file = "Base Teste Prova/live.sqlite"
    df_unificado = get_processed_data(db_file)

    # Filtrar os dados pelo intervalo de tempo (coluna 'ByteSize')
    df_filtrado = df_unificado[
        (df_unificado['ByteSize'] >= start_timestamp) & 
        (df_unificado['ByteSize'] <= end_timestamp)
    ]
    
    if df_filtrado.empty:
        return {"message": "Nenhum registro encontrado para o intervalo de tempo fornecido."}
    
    filtered_data_json = df_filtrado.to_dict(orient='records')
    return {"data": filtered_data_json}