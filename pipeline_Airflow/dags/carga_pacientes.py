import pandas as pd
import requests
import pendulum
import logging
from airflow.decorators import dag, task

FHIR_SERVER_URL = "http://host.docker.internal:8080/fhir"
CSV_PATH = "/opt/airflow/dags/data/patients.csv"

def formatar_data(data_str):
    if not data_str or pd.isna(data_str):
        return None
    try:
        return pendulum.from_format(str(data_str), 'DD/MM/YYYY').to_date_string()
    except Exception:
        logging.warning(f"Formato de data inválido: {data_str}")
        return None

def mapear_genero(genero_str):
    if pd.isna(genero_str):
        return "unknown"
    genero = str(genero_str).lower()
    if genero == 'masculino':
        return 'male'
    elif genero == 'feminino':
        return 'female'
    return 'unknown'

def criar_recurso_patient_bonus(row):
    identificadores = []

    if pd.notna(row['CPF']):
        cpf_limpo = str(row['CPF']).strip().replace('.', '').replace('-', '')
        identificadores.append({
            "system": "http://www.saude.gov.br/fhir/r4/sid/cpf",
            "value": cpf_limpo
        })

    telecom_list = []
    if pd.notna(row.get('Telefone')):
        telecom_list.append({
            "system": "phone",
            "value": str(row['Telefone']),
            "use": "mobile"
        })

    patient = {
        "resourceType": "Patient",
        "meta": {
            "profile": [
                "http://www.saude.gov.br/fhir/r4/StructureDefinition/BRIndividuo"
            ]
        },
        "identifier": identificadores,
        "name": [{
            "use": "official",
            "text": str(row['Nome'])
        }],
        "gender": mapear_genero(row.get('Gênero')),
        "birthDate": formatar_data(row['Data de Nascimento']),
        "telecom": telecom_list
    }
    return patient

def criar_recurso_observation(patient_id, observacao_texto):
    return {
        "resourceType": "Observation",
        "status": "final",
        "code": {
            "coding": [{"system": "http://loinc.org", "code": "11348-0"}],
            "text": "Observação Clínica"
        },
        "subject": {"reference": f"Patient/{patient_id}"},
        "valueString": str(observacao_texto)
    }

@dag(
    dag_id="carga_pacientes",
    start_date=pendulum.datetime(2025, 11, 12, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    tags=['fhir','indra','bonus']
)
def dag_carga_pacientes_fhir():

    @task
    def etl_pacientes_csv_para_fhir():

        try:
            df = pd.read_csv(CSV_PATH, sep=',', encoding='latin-1')
        except FileNotFoundError:
            logging.error(f"ERRO: Arquivo CSV não encontrado em {CSV_PATH}")
            raise

        sucessos, falhas = 0, 0

        for index, row in df.iterrows():
            try:
                patient_json = criar_recurso_patient_bonus(row)

                cpf_limpo = None
                if pd.notna(row['CPF']):
                    cpf_limpo = str(row['CPF']).replace('.', '').replace('-', '')

                headers = {"Content-Type": "application/fhir+json"}
                if cpf_limpo:
                    headers["If-None-Exist"] = f"identifier=http://www.saude.gov.br/fhir/r4/sid/cpf|{cpf_limpo}"

                response = requests.post(
                    f"{FHIR_SERVER_URL}/Patient",
                    json=patient_json,
                    headers=headers
                )
                response.raise_for_status()

                # --- CORREÇÃO DO ID ---
                body = response.json()
                if "id" in body:
                    patient_id = body["id"]
                else:
                    patient_id = body["entry"][0]["resource"]["id"]

                # Observation só se paciente criado
                if response.status_code == 201 and pd.notna(row['Observação']):
                    obs_json = criar_recurso_observation(patient_id, row['Observação'])
                    obs_response = requests.post(
                        f"{FHIR_SERVER_URL}/Observation",
                        json=obs_json,
                        headers={"Content-Type": "application/fhir+json"}
                    )
                    obs_response.raise_for_status()

                sucessos += 1

            except Exception as e:
                logging.error(f"Erro linha {index} ({row['Nome']}): {e}")
                falhas += 1

        if falhas > 0:
            raise Exception(f"{falhas} registros falharam.")

    etl_pacientes_csv_para_fhir()

dag_carga_pacientes_fhir()
