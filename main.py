import sqlite3
import pandas as pd
from fastapi import FastAPI, HTTPException
from typing import Dict, Any, List

# Inicializa a aplicação FastAPI
app = FastAPI(
    title="API de Desafio - Dados SQLite",
    description="API para extrair, tratar e unificar dados de um banco SQLite.",
    version="1.0.0"
)

# --- Endpoints da API ---

@app.get("/")
def read_root():
    return {"message": "API rodando. Acesse /get-data para ver os dados unificados."}

@app.get("/get-data/", response_model=Dict[str, Any])
def get_unified_data():
    """
    Extrai dados de 3 tabelas do arquivo live.sqlite, unifica e retorna em formato JSON.
    """
    db_file = "Base Teste Prova/live.sqlite"
    
    try:
        # Conecta-se ao banco de dados SQLite
        conn = sqlite3.connect(db_file)
        
        # Lê os dados de cada tabela para um DataFrame do pandas
        df_tabela1 = pd.read_sql_query("SELECT * FROM processes1", conn)
        df_tabela2 = pd.read_sql_query("SELECT * FROM processes2", conn)
        df_tabela3 = pd.read_sql_query("SELECT * FROM processes3", conn)
        
        # Fecha a conexão com o banco de dados
        conn.close()
        
        # --- Lógica de Unificação (Concatenação) ---
        # Empilha os DataFrames um sobre o outro
        df_unificado = pd.concat([df_tabela1, df_tabela2, df_tabela3], ignore_index=True)
        
        # Converte o DataFrame do pandas para uma lista de dicionários Python (JSON)
        unified_data_json = df_unificado.to_dict(orient='records')
        
        return {"data": unified_data_json}

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Arquivo '{db_file}' não encontrado.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar dados: {str(e)}")