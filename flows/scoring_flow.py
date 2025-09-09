from prefect import flow, task
import os, json, base64, datetime as dt
from google.oauth2 import service_account
from google.cloud import bigquery


# --- Credenciales desde Job Variables (Prefect Deployment) ---
def _get_creds_and_project():
    """
    Lee la clave de servicio GCP codificada en base64 desde la var de entorno
    GCP_SA_KEY_B64 y devuelve (credentials, project_id).
    """
    b64 = os.environ.get("GCP_SA_KEY_B64")
    if not b64:
        raise RuntimeError("Falta env GCP_SA_KEY_B64 en Job Variables del deployment")

    try:
        info = json.loads(base64.b64decode(b64))
    except Exception as e:
        raise RuntimeError(f"GCP_SA_KEY_B64 inválido (no es base64/JSON): {e}")

    creds = service_account.Credentials.from_service_account_info(info)
    project = os.environ.get("BQ_PROJECT_ID") or info.get("project_id")
    if not project:
        raise RuntimeError("No se encontró BQ_PROJECT_ID ni project_id en la key de servicio")
    return creds, project


@task
def ensure_dataset_table() -> str:
    """
    Crea dataset y tabla si no existen.
    Devuelve el nombre completo de la tabla (project.dataset.table).
    """
    creds, project = _get_creds_and_project()
    client = bigquery.Client(project=project, credentials=creds)

    dataset_id = os.environ.get("BQ_DATASET", "mvp_scoring")
    table_id = os.environ.get("BQ_TABLE", "results")

    # Dataset
    ds_ref = bigquery.Dataset(f"{project}.{dataset_id}")
    # Ajustá la región a la tuya si corresponde (p.ej. 'US', 'southamerica-east1', etc.)
    ds_ref.location = os.environ.get("BQ_LOCATION", "US")
    try:
        client.get_dataset(ds_ref)
    except Exception:
        client.create_dataset(ds_ref, exists_ok=True)

    # Tabla
    table_ref = bigquery.Table(
        f"{project}.{dataset_id}.{table_id}",
        schema=[
            bigquery.SchemaField("run_at", "TIMESTAMP"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("score", "FLOAT"),
        ],
    )
    try:
        client.get_table(table_ref)
    except Exception:
        client.create_table(table_ref, exists_ok=True)

    full_table = f"{project}.{dataset_id}.{table_id}"
    print(f"Tabla OK: {full_table}")
    return full_table


@task
def write_sample_row(full_table_name: str) -> str:
    """
    Inserta una fila de prueba.
    """
    creds, project = _get_creds_and_project()
    client = bigquery.Client(project=project, credentials=creds)

    rows = [
        {
            "run_at": dt.datetime.utcnow().isoformat(),
            "customer_id": "demo-001",
            "score": 0.87,
        }
    ]
    errors = client.insert_rows_json(full_table_name, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    msg = f"Inserted {len(rows)} row(s) into {full_table_name}"
    print(msg)
    return msg


@flow(name="scoring-flow")
def scoring_flow():
    table = ensure_dataset_table()
    _ = write_sample_row(table)


if __name__ == "__main__":
    scoring_flow()
