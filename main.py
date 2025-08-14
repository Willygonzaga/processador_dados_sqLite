from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import requests

# Inicializa a aplicação FastAPI
app = FastAPI(
    title="API",
    description="API base.",
    version="1.0.0"
)

# --- Modelos de Dados ---
# Usa o Pydantic para definir a estrutura dos dados
class Item(BaseModel):
    nome: str
    preco: float
    disponivel: bool = True

# --- Endpoints da API ---

# Rota de teste
@app.get("/")
def raiz():
    return {"mensagem": "Olá, Mundo!"}



# Rota para criar um novo item
@app.post("/items/")
def criar_item(item: Item):
    # Lógica para processar o item
    return {"mensagem": "Item recebido com sucesso", "item": item}



# Rota para consumir dados de uma API externa
@app.get("/github-user/{username}")
def get_github_user(username: str):
    try:
        response = requests.get(f"https://api.github.com/users/{username}")
        response.raise_for_status()  # Lança um erro para status de erro (4xx ou 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=404, detail=f"Erro ao buscar usuário: {str(e)}")




# Para rodar a aplicação: uvicorn main:app --reload

# Documentação interativa: http://127.0.0.1:8000/docs