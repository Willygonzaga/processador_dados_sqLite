# Processador de Dados SQLite

Este projeto é uma API REST em Python construída com o framework FastAPI. Sua função principal é processar e analisar dados de processos de sistema extraídos de um banco de dados SQLite, disponibilizando-os para consulta.

## Funcionalidades

* **API REST**: Desenvolvida com FastAPI para documentação automática.
* **Upload de Arquivos**: Aceita arquivos `.sqlite` via upload para atualização da base de dados.
* **Processamento de Dados**: Unifica dados de múltiplas tabelas, analisando a coluna de métricas para extrair informações estruturadas (e.g., CPU, memória).
* **Consulta por Intervalo de Tempo**: Filtra e retorna os registros de processo com base em um intervalo de `timestamp`.

## Tecnologias

* Python
* FastAPI
* Pandas
* SQLite
* Uvicorn

## Como Rodar

1.  Clone o repositório.
2.  Instale as dependências: `pip install fastapi uvicorn[standard] pandas`.
3.  Inicie a API: `uvicorn main:app --reload`.

A documentação interativa completa da API está disponível em `http://127.0.0.1:8000/docs`.

## Autor

Willy Gonzaga Balieiro