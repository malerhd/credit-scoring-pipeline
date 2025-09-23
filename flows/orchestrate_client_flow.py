# flows/orchestrate_client_flow.py
from __future__ import annotations

import json, os, base64
from prefect import flow, get_run_logger
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account
from flows.transform_flow import transform_flow
from flows.score_simple_flow import score_monthly_simple

VALID_PLATFORMS = {"tn", "ga", "meta-ads"}

def _get_gcp_credentials():
    b64 = os.getenv("SERVICE_ACCOUNT_B64")
    if not b64:
        raise RuntimeError("Falta SERVICE_ACCOUNT_B64 (mapeá el Secret en Job Variables del deployment).")
    info = json.loads(base64.b64decode(b64).decode("utf-8"))
    return service_account.Credentials.from_service_account_info(info)

def _path_for(platform: str, client_key: str) -> str:
    base = f"gs://loopi-data-dev/{client_key}"
    return {
        "tn":       f"{base}/tiendanube/snapshot-latest.json",
        "ga":       f"{base}/ga/snapshot-latest.json",
        "meta-ads": f"{base}/meta-ads/snapshot-latest.json",
    }[platform]

def _normalize_platforms_arg(platforms) -> list[str] | None:
    if platforms is None:
        return None
    if isinstance(platforms, (list, tuple, set)):
        return [str(x).strip() for x in platforms if str(x).strip()]
    if isinstance(platforms, str):
        s = platforms.strip()
        if s in ("", "[]", "null", "None"):
            return []
        try:
            val = json.loads(s)
            if isinstance(val, list):
                return [str(x).strip() for x in val if str(x).strip()]
            if isinstance(val, str):
                return [val.strip()]
        except Exception:
            return [t.strip() for t in s.replace("\n", ",").split(",") if t.strip()]
    return [str(platforms).strip()]

def _available_platforms(bq: bigquery.Client, project_id: str, client_key: str, max_age_minutes: int) -> list[str]:
    sql = f"""
    SELECT platform
    FROM `{project_id}.ops.snapshot_status`
    WHERE client_key = @ck
      AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), updated, MINUTE) <= @max_age
    """
    try:
        job = bq.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("ck","STRING", client_key),
                    bigquery.ScalarQueryParameter("max_age","INT64", max_age_minutes),
                ]
            ),
        )
        return [r["platform"] for r in job.result()]
    except Exception:
        return []

@flow(name="orchestrate-client")
def orchestrate_client(
    project_id: str,
    client_key: str,
    target_table: str = "loopi-470817.gold.scoring_client_minimal",
    months_back: int = 24,
    aggregate_last_n: int = 12,
    platforms: list[str] | None = None,
    max_age_minutes: int = 1440,
    seguidores: int = 376000,
    engagement_rate_redes: float = 0.045,
):
    logger = get_run_logger()
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds)

    # Normalizar parámetro
    platforms = _normalize_platforms_arg(platforms)

    # Autodetectar si viene vacío
    if not platforms:
        platforms = _available_platforms(bq, project_id, client_key, max_age_minutes)
        logger.info(f"Plataformas detectadas para {client_key}: {platforms}")

    # Filtrar válidas
    platforms = [p for p in platforms if p in VALID_PLATFORMS]
    logger.info(f"Plataformas a procesar (normalizadas): {platforms}")

    if not platforms:
        logger.warning(f"No hay plataformas para procesar en {client_key}. Salgo.")
        return 0

    # Transform por cada plataforma; si falta snapshot, saltar
    for p in platforms:
        try:
            n = transform_flow(
                gcs_path=_path_for(p, client_key),
                client_key=client_key,
                platform=p,
                project_id=project_id,
            )
            logger.info(f"[{p}] upsert rows: {n}")
        except NotFound as e:
            logger.warning(f"Skipping {p}: snapshot no encontrado ({e})")

    # Scoring
    score = score_monthly_simple(
        project_id=project_id,
        client_key=client_key,
        target_table=target_table,
        months_back=months_back,
        aggregate_last_n=aggregate_last_n,
        seguidores=seguidores,
        engagement_rate_redes=engagement_rate_redes,
    )
    logger.info(f"Score {client_key}: {score}")
    return score
