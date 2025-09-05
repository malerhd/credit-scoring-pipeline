from prefect import flow, task
from google.cloud import bigquery

@task
def cargar_raw():
    print("Simulación: cargo datos de GCS -> BigQuery raw")

@task
def procesar():
    print("Simulación: limpio y calculo scoring")

@task
def guardar_gold():
    client = bigquery.Client()
    print("Simulación: guardo resultados en BigQuery gold")

@flow(log_prints=True)
def scoring_flow():
    cargar_raw()
    procesar()
    guardar_gold()

if __name__ == "__main__":
    scoring_flow()
