import sqlite3
import pandas as pd
from fastapi import FastAPI, HTTPException, UploadFile, File
from typing import Dict, Any, List

# Inicializa a aplicação FastAPI
app = FastAPI(
    title="API de Desafio",
    description="API para upload, consulta e análise de métricas.",
    version="1.0.0"
)

# Função para processar os dados do banco
def get_processar_dados(db_arquivo: str):
    

    # Extrai os dados
    try:
        conn = sqlite3.connect(db_arquivo)
        
        df_tabela1 = pd.read_sql_query("SELECT * FROM processes1", conn)
        df_tabela2 = pd.read_sql_query("SELECT * FROM processes2", conn)
        df_tabela3 = pd.read_sql_query("SELECT * FROM processes3", conn)
        
        conn.close()
    # ===================================================================
        
        # Unifica os dados
        df_unificado = pd.concat([df_tabela1, df_tabela2, df_tabela3], ignore_index=True)
        
        # Processa a coluna metrics
        df_unificado['Metrics'] = df_unificado['Metrics'].str.split(';')

        def parse_metrics(metric_list):
            if not metric_list or not isinstance(metric_list, list):
                return {}
            
            metrics_dict = {}
            for item in metric_list:
                if ':' in item:
                    key, value = item.split(':', 1)
                    metrics_dict[key.strip()] = value.strip()
            return metrics_dict

        df_unificado['ParsedMetrics'] = df_unificado['Metrics'].apply(parse_metrics)
        
        df_metrics_expanded = pd.json_normalize(df_unificado['ParsedMetrics'])
        
        # 4. Concatena as novas colunas ao DataFrame unificado original e remove a coluna temporária
        df_unificado = pd.concat([df_unificado.drop(columns=['Metrics', 'ParsedMetrics']), df_metrics_expanded], axis=1)

        # Retorna o DataFrame processado
        return df_unificado

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Arquivo '{db_arquivo}' não encontrado.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar dados: {str(e)}")

# --- Endpoints da API ---
@app.get("/")
def raiz():
    return {"API rodando. Use os endpoints para interagir com o banco de dados."}

# Rota para consultar dados
@app.get("/get-dados/", response_model=Dict[str, Any])
def get_dados_unificados():
    db_arquivo = "Base Teste Prova/live.sqlite"
    df_unificado = get_processar_dados(db_arquivo)
    unified_data_json = df_unificado.to_dict(orient='records')
    return {"data": unified_data_json}

# Rota para upload do arquivo
@app.post("/upload-db/")
async def upload_arquivo(file: UploadFile = File(...)):
    if file.filename.endswith('.sqlite'):
        db_caminho = f"Base Teste Prova/{file.filename}"
        with open(db_caminho, "wb") as f:
            f.write(await file.read())
        return {"message": f"Arquivo '{file.filename}' carregado com sucesso."}
    else:
        raise HTTPException(status_code=400, detail="Apenas arquivos .sqlite são permitidos.")

# Rota de consulta com filtro de tempo
@app.get("/processes/")
def get_dados_intervalos(start_timestamp: int, end_timestamp: int):
  
    db_arquivo = "Base Teste Prova/live.sqlite"
    df_unificado = get_processar_dados(db_arquivo)

    df_filtrado = df_unificado[
        (df_unificado['ByteSize'] >= start_timestamp) & 
        (df_unificado['ByteSize'] <= end_timestamp)
    ]
    
    if df_filtrado.empty:
        return {"Nenhum registro encontrado para o intervalo de tempo fornecido."}
    
    filtered_data_json = df_filtrado.to_dict(orient='records')
    return {"data": filtered_data_json}