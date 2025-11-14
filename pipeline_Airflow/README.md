# Parte 2: Pipeline de Carga de Pacientes (Airflow)

Este componente provisiona um ambiente Apache Airflow para executar um pipeline (DAG) de ETL.

## Tecnologias
* Apache Airflow customizada via `Dockerfile`
* Python (Pandas, Pendulum, Requests)
* Docker Compose

## Funcionalidades do Pipeline (DAG)

A DAG `carga_pacientes` (definida em `dags/carga_pacientes_dag.py`) é responsável por:

1.  **Extrair (E):** Ler o arquivo `dags/data/patients.csv` (usando `latin-1` para lidar com acentuação).
2.  **Transformar (T):**
    * Limpar e formatar dados (Datas, Gênero, CPF).
    * Mapear os dados para os *Resources* `Patient` e `Observation`.
    * Implementar o Profile `BRIndividuo`, adicionando o CPF no campo `identifier` e a `meta.profile` no recurso `Patient`.
3.  **Carregar (L):**
    * Enviar os dados para o servidor HAPI FHIR (na `http://host.docker.internal:8080`).

## Como Executar

**Pré-requisito:** Certifique-se de que o servidor HAPI FHIR (Parte 1) esteja em execução.

1.  **Configurar o Ambiente (`.env`):**
    Este setup usa um `Dockerfile` customizado. Primeiro, renomeie o arquivo `.env.example` para `.env` para definir o UID do usuário (necessário no Linux para permissões de pasta).
    
    # (Substitua 1000 pelo resultado de 'id -u' se for diferente)
    AIRFLOW_UID=1000

2.  **Construir a Imagem Customizada:**
    Este comando irá construir a imagem Docker local (definida no `Dockerfile`), instalando as dependências Python (`pandas`, `requests`, `pendulum`).
    
    docker-compose build
    
3.  **Iniciar o Airflow:**
    Agora, inicie todos os serviços com o comando
    
    docker-compose up -d
    

## Como Disparar a Carga

1.  **Acesse a Interface Web:**
    Abra o Airflow no seu navegador: **[http://localhost:8081](http://localhost:8081)**
    * **Login:** `airflow`
    * **Senha:** `airflow`

2.  **Execute a DAG:**
    * Na página principal, encontre a DAG `carga_pacientes`.
    * Clique no ícone "Play" à direita para disparar a execução.

3.  **Monitore:**
    * Clique no nome da DAG para ver a execução.
    * Clique na tarefa (`etl_pacientes_csv_para_fhir`) e vá até "Logs" para ver o resultado da carga.
    * Se tudo correr bem, o log terminará com `INFO - Carga finalizada. Sucessos: 50, Falhas: 0`.