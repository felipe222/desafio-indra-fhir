import pandas as pd
import requests
import pendulum
import logging
from airflow.decorators import dag, task

# =============================================================================
# 1. CONFIGURAÇÕES GLOBAIS
# =============================================================================

# URL do seu servidor FHIR (Parte 1). 
# 'host.docker.internal' permite que o container do Airflow "enxergue" o localhost da sua máquina.
FHIR_SERVER_URL = "http://host.docker.internal:8080/fhir"

# Caminho de onde o Airflow vai ler o CSV (de dentro do container).
CSV_PATH = "/opt/airflow/dags/data/patients.csv"


# =============================================================================
# 2. FUNÇÕES DE TRANSFORMAÇÃO (Helpers)
# =============================================================================

def formatar_data(data_str):
    """Converte datas do formato DD/MM/YYYY para YYYY-MM-DD (padrão FHIR)."""
    if not data_str or pd.isna(data_str):
        return None
    try:
        # Usa a biblioteca Pendulum para "entender" a data no formato brasileiro
        return pendulum.from_format(str(data_str), 'DD/MM/YYYY').to_date_string()
    except Exception:
        logging.warning(f"Formato de data inválido: {data_str}")
        return None

def mapear_genero(genero_str):
    """Converte o gênero do CSV ('Masculino') para o padrão FHIR ('male')."""
    genero = str(genero_str).lower()
    if genero == 'masculino':
        return 'male'
    elif genero == 'feminino':
        return 'female'
    else:
        # 'unknown' é o valor padrão do FHIR para casos não mapeados
        return 'unknown'

# =============================================================================
# 3. FUNÇÕES DE CONSTRUÇÃO DE RECURSOS FHIR (A Lógica 'T' do ETL)
# =============================================================================

def criar_recurso_patient_bonus(row):
    """
    Monta o Resource Patient CONFORME O PROFILE BRIndividuo
    usando uma linha do CSV.
    """
    identificadores = []
    
    # --- Lógica do CPF (Parte do Bônus) ---
    if pd.notna(row['CPF']):
        # Limpa o CPF (ex: "123.456.789-00" -> "12345678900")
        cpf_limpo = str(row['CPF']).strip().replace('.', '').replace('-', '')
        identificadores.append({
            # Este 'system' é a URL oficial do CPF para o FHIR Brasil.
            "system": "http://www.saude.gov.br/fhir/r4/sid/cpf", 
            "value": cpf_limpo
        })

    # --- Lógica do Telefone (Opcional, mas boa prática) ---
    telecom_list = []
    if pd.notna(row['Telefone']):
        telecom_list.append({
            "system": "phone",
            "value": str(row['Telefone']),
            "use": "mobile" # Assume que é um celular
        })

    # --- Montagem do Recurso Paciente ---
    patient = {
        "resourceType": "Patient",
        
        # --- Parte Essencial do Bônus ---
        # "Carimba" o recurso, declarando que ele segue o Profile BRIndividuo
        "meta": {
            "profile": [
                "http://www.saude.gov.br/fhir/r4/StructureDefinition/BRIndividuo"
            ]
        },
        
        "identifier": identificadores, # Adiciona o CPF que processamos
        "name": [{
            "use": "official",
            "text": str(row['Nome'])
        }],
        "gender": mapear_genero(row['Gênero']), # Usa a função helper
        "birthDate": formatar_data(row['Data de Nascimento']), # Usa a função helper
        "telecom": telecom_list # Adiciona o telefone que processamos
    }
    return patient

def criar_recurso_observation(patient_id, observacao_texto):
    """
    Monta o Resource Observation, VINCULADO ao Patient.
    """
    return {
        "resourceType": "Observation",
        "status": "final",
        "code": {
            # Código LOINC padrão para "Histórico Clínico"
            "coding": [{"system": "http://loinc.org", "code": "11348-0", "display": "History of Clinical finding"}],
            "text": "Observação Clínica"
        },
        # --- O Vínculo Crucial ---
        # Conecta esta observação ao ID do paciente que foi criado.
        "subject": {
            "reference": f"Patient/{patient_id}" 
        },
        "valueString": str(observacao_texto) # O texto do CSV (ex: "Gestante|Diabético")
    }


# =============================================================================
# 4. DEFINIÇÃO DA DAG E TAREFA (O Pipeline do Airflow)
# =============================================================================

@dag(
    dag_id="carga_pacientes",
    start_date=pendulum.datetime(2025, 11, 12, tz="UTC"),
    schedule_interval=None, # Define que a DAG só roda manualmente (clicando em "Play")
    catchup=False,
    tags=['fhir', 'teste-indra', 'bonus']
)
def dag_carga_pacientes_fhir():
    """
    Pipeline ETL para carregar pacientes de um CSV para um servidor FHIR.
    Esta DAG implementa o bônus (Profile BRIndividuo) e
    é idempotente (evita duplicação de pacientes e observações).
    """

    @task
    def etl_pacientes_csv_para_fhir():
        
        # --- FASE DE EXTRAÇÃO (E) ---
        try:
            # Lê o CSV, especificando o separador (vírgula) e a codificação (latin-1)
            df = pd.read_csv(CSV_PATH, sep=',', encoding='latin-1') 
        except FileNotFoundError:
            logging.error(f"ERRO: Arquivo CSV não encontrado em {CSV_PATH}")
            raise

        sucessos = 0
        falhas = 0
        
        logging.info(f"Iniciando carga de {len(df)} registros do CSV...")

        # --- FASE DE TRANSFORMAÇÃO (T) E CARGA (L) ---
        for index, row in df.iterrows():
            try:
                # 1. (T) Criar o JSON do Paciente
                patient_json = criar_recurso_patient_bonus(row)
                
                
                # --- LÓGICA DE IDEMPOTÊNCIA (Evitar Duplicatas) ---
                cpf_limpo = None
                if pd.notna(row['CPF']):
                    cpf_limpo = str(row['CPF']).strip().replace('.', '').replace('-', '')

                headers = {"Content-Type": "application/fhir+json"}

                if cpf_limpo:
                    # Este cabeçalho diz ao HAPI: "Não crie este recurso se um 
                    # paciente com este CPF já existir".
                    headers["If-None-Exist"] = f"identifier=http://www.saude.gov.br/fhir/r4/sid/cpf|{cpf_limpo}"
                
                
                # 2. (L) Enviar o Paciente (Criação Condicional)
                response = requests.post(
                    f"{FHIR_SERVER_URL}/Patient", 
                    json=patient_json, 
                    headers=headers # Envia o cabeçalho "If-None-Exist"
                )
                response.raise_for_status() 
                
                # Pega o ID do paciente (seja ele novo ou existente)
                patient_id = response.json().get('id') 

                # --- CORREÇÃO DA DUPLICAÇÃO DE OBSERVAÇÃO ---
                # Só crie a observação se o paciente também foi CRIADO (Status 201),
                # e não se ele foi apenas ENCONTRADO (Status 200).
                if response.status_code == 201 and pd.notna(row['Observação']) and str(row['Observação']).strip():
                    
                    # 3. (T+L) Criar e Enviar a Observação
                    obs_json = criar_recurso_observation(patient_id, row['Observação'])
                    obs_response = requests.post(
                        f"{FHIR_SERVER_URL}/Observation", 
                        json=obs_json, 
                        headers={"Content-Type": "application/fhir+json"} # Header normal
                    )
                    obs_response.raise_for_status()
                
                sucessos += 1

            except requests.exceptions.HTTPError as e:
                # Captura erros do HAPI FHIR (ex: 400 Bad Request, 500 Server Error)
                logging.error(f"Falha na linha {index} ({row['Nome']}): {e.response.status_code} - {e.response.text}")
                falhas += 1
            except Exception as e:
                # Captura outros erros (ex: falha de rede)
                logging.error(f"Falha inesperada na linha {index} ({row['Nome']}): {e}")
                falhas += 1
        
        logging.info(f"Carga finalizada. Sucessos: {sucessos}, Falhas: {falhas}")
        
        # Faz a tarefa do Airflow falhar se algum registro deu erro
        if falhas > 0:
            raise Exception(f"{falhas} registros falharam ao carregar.")

    
    # Define a ordem das tarefas (neste caso, só temos uma)
    etl_pacientes_csv_para_fhir()

    
# Instancia a DAG para que o Airflow a reconheça
dag_carga_pacientes_fhir()