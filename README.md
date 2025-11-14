# Desafio Técnico: Engenheiro de Dados FHIR (Indra)

Este repositório contém a solução para o teste técnico de Engenheiro de Dados especialista em HL7 FHIR. O projeto é dividido em duas partes principais, ambas executadas localmente usando Docker e Docker Compose.

##  Arquitetura da Solução

A solução consiste em dois ambientes Docker Compose independentes:

1.  **Servidor FHIR (Parte 1):** Um servidor HAPI FHIR persistindo os dados em um banco PostgreSQL.
2.  **Pipeline ETL (Parte 2):** Um ambiente Apache Airflow que orquestra um pipeline em Python para ler o CSV carga_pacientes, transformar os dados e carregá-los no Servidor FHIR.


## Componentes

### 1. Parte 1: Servidor HAPI FHIR
* **Localização:** `./servidor_FHIR/`
* **Descrição:** Contém o `docker-compose.yml` para subir o servidor HAPI FHIR e o banco de dados Postgres. O servidor é exposto na porta `8080`.
* **Instruções:** [README da Parte 1](./servidor_FHIR/README.md)

### 2. Parte 2: Pipeline de Carga (Airflow)
* **Localização:** `./pipeline_Airflow/`
* **Descrição:** Contém a infraestrutura do Apache Airflow e a DAG de ETL.
* **Instruções:** [README da Parte 2](./pipeline_Airflow/README.md)

## Requisitos

* Docker
* Docker Compose

## Como Executar a Solução Completa

1.  **Iniciar o Servidor FHIR (Parte 1):**
    * Siga as instruções no [README da Parte 1](./servidor_FHIR/README.md) para subir o servidor HAPI.
2.  **Executar o Pipeline (Parte 2):**
    * Siga as instruções no [README da Parte 2](./pipeline_Airflow/README.md) para configurar o ambiente Airflow e disparar a DAG de carga.