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
    """
    Conecta-se ao banco de dados, extrai e unifica os dados das tabelas de processos,
    e depois processa a coluna 'Metrics'.
    """
    try:
        conn = sqlite3.connect(db_arquivo)
        
        df_tabela1 = pd.read_sql_query("SELECT * FROM processes1", conn)
        df_tabela2 = pd.read_sql_query("SELECT * FROM processes2", conn)
        df_tabela3 = pd.read_sql_query("SELECT * FROM processes3", conn)
        
        conn.close()
        
        # Unifica os dados
        df_unificado = pd.concat([df_tabela1, df_tabela2, df_tabela3], ignore_index=True)
        
        # --- NOVO: Lógica para processar a coluna 'Metrics' ---
        # 1. Divide a string da coluna 'Metrics' em uma lista de métricas.
        # Ex: "cpu:10;mem:20" -> ["cpu:10", "mem:20"]
        df_unificado['Metrics'] = df_unificado['Metrics'].str.split(';')

        # 2. Transforma cada lista de métricas em um dicionário.
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
        
        # 3. Expande o dicionário em novas colunas no DataFrame
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
    return {"message": "API rodando. Use os endpoints para interagir com o banco de dados."}

# Rota para consultar dados
@app.get("/get-dados/", response_model=Dict[str, Any])
def get_dados_unificados_endpoint():
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
def get_data_by_time_range(start_timestamp: int, end_timestamp: int):
    """
    Consulta dados unificados de um intervalo de tempo (timestamp em milissegundos).
    A coluna 'ByteSize' está sendo usada como o timestamp para este exemplo.
    """
    db_file = "Base Teste Prova/live.sqlite"
    df_unificado = get_processar_dados(db_file)

    df_filtrado = df_unificado[
        (df_unificado['ByteSize'] >= start_timestamp) & 
        (df_unificado['ByteSize'] <= end_timestamp)
    ]
    
    if df_filtrado.empty:
        return {"message": "Nenhum registro encontrado para o intervalo de tempo fornecido."}
    
    filtered_data_json = df_filtrado.to_dict(orient='records')
    return {"data": filtered_data_json}