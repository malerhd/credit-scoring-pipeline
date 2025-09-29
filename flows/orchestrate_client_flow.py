# flows/orchestrate_client_flow.py

from __future__ import annotations

# Stdlib
import os
import re
import json
import base64
from urllib.parse import urlparse

# Terceros
from prefect import flow, get_run_logger
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account

# Flows internos
from flows.transform_flow import transform_flow
from flows.score_simple_flow import score_monthly_simple

# Plataformas soportadas
VALID_PLATFORMS = {"tn", "ga", "meta-ads"}


# ─────────────────────────────────────────────────────────
# Credenciales (tolerante: JSON crudo o Base64; corrige padding)
# ─────────────────────────────────────────────────────────
def _get_gcp_credentials():
    """
    Lee credenciales desde SERVICE_ACCOUNT_B64 (JSON crudo o Base64).
    Si no hay secret, devuelve None para usar ADC/default.
    """
    log = get_run_logger()
    raw = (os.getenv("SERVICE_ACCOUNT_B64") or "").strip()
    if not raw:
        log.warning("[AUTH] SERVICE_ACCOUNT_B64 vacío; uso ADC/default")
        return None

    try:
        # ¿Pegaron JSON crudo en el Secret?
        if raw.lstrip().startswith("{"):
            info = json.loads(raw)
        else:
            # Limpiar espacios/saltos y arreglar padding de Base64
            s = re.sub(r"\s+", "", raw)
            s += "=" * (-len(s) % 4)
            info = json.loads(base64.b64decode(s).decode("utf-8"))
    except Exception as e:
        raise RuntimeError(f"SERVICE_ACCOUNT_B64 inválido (JSON/Base64): {e}")

    log.info("[AUTH] usando credenciales del Secret")
    return service_account.Credentials.from_service_account_info(info)


# ─────────────────────────────────────────────────────────
# Helpers GCS / Paths
# ─────────────────────────────────────────────────────────
def _parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    u = urlparse(gcs_uri)
    return u.netloc, u.path.lstrip("/")


def _path_for(platform: str, client_key: str) -> str:
    base = f"gs://loopi-data-dev/{client_key}"
    return {
        "tn":       f"{base}/tiendanube/snapshot-latest.json",
        "ga":       f"{base}/ga/snapshot-latest.json",
        "meta-ads": f"{base}/meta-ads/snapshot-latest.json",
    }[platform]


def _gcs_fingerprint(gcs_uri: str, creds) -> str | None:
    """
    Devuelve 'generation' del blob si existe; None si no existe.
    """
    bucket, blob_path = _parse_gcs_uri(gcs_uri)
    sc = storage.Client(credentials=creds) if creds else storage.Client()
    blob = sc.bucket(bucket).blob(blob_path)
    if not blob.exists():
        return None
    blob.reload()  # carga metadatos
    return str(blob.generation)


# ─────────────────────────────────────────────────────────
# Cursor en BigQuery (para no reprocesar)
# ─────────────────────────────────────────────────────────
def _read_cursor(bq: bigquery.Client, project_id: str, client_key: str, platform: str) -> str | None:
    sql = f"""
    SELECT last_generation
    FROM `{project_id}.ops.snapshot_cursor`
    WHERE client_key=@ck AND platform=@pf
    LIMIT 1
    """
    rows = list(bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ck","STRING", client_key),
                bigquery.ScalarQueryParameter("pf","STRING", platform),
            ]
        )
    ).result())
    return rows[0]["last_generation"] if rows else None


def _write_cursor(bq: bigquery.Client, project_id: str, client_key: str, platform: str, generation: str):
    sql = f"""
    MERGE `{project_id}.ops.snapshot_cursor` T
    USING (SELECT @ck AS client_key, @pf AS platform, @gen AS last_generation, CURRENT_TIMESTAMP() AS last_updated) S
    ON T.client_key=S.client_key AND T.platform=S.platform
    WHEN MATCHED THEN UPDATE SET last_generation=S.last_generation, last_updated=S.last_updated
    WHEN NOT MATCHED THEN INSERT (client_key, platform, last_generation, last_updated)
    VALUES (S.client_key, S.platform, S.last_generation, S.last_updated)
    """
    bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ck","STRING", client_key),
                bigquery.ScalarQueryParameter("pf","STRING", platform),
                bigquery.ScalarQueryParameter("gen","STRING", generation),
            ]
        )
    ).result()


# ─────────────────────────────────────────────────────────
# Helpers varios
# ─────────────────────────────────────────────────────────
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


def _available_platforms_by_table(bq: bigquery.Client, project_id: str, client_key: str, max_age_minutes: int) -> list[str]:
    """
    Fallback opcional: leer plataformas recientes desde una tabla de estado si existiera.
    """
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


# ─────────────────────────────────────────────────────────
# Flow principal
# ─────────────────────────────────────────────────────────
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

    # Credenciales (Secret si está, si no ADC)
    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)

    # Log de identidad efectiva
    principal = getattr(creds, "service_account_email", None)
    if principal:
        logger.info(f"[AUTH] usando SA del Secret: {principal}")
    else:
        import google.auth
        adc_creds, adc_proj = google.auth.default()
        principal = getattr(adc_creds, "service_account_email", None) or type(adc_creds).__name__
        logger.info(f"[AUTH] ADC project={adc_proj} principal={principal}")

    logger.info(f"[BQ] client.project={bq.project} location={bq.location or 'default'}")

    # 1) Normalizar parámetro
    platforms = _normalize_platforms_arg(platforms)

    # 2) Autodetectar por GCS si viene vacío; si sigue vacío, opcionalmente probar tabla
    if not platforms:
        detected = []
        for p in VALID_PLATFORMS:
            uri = _path_for(p, client_key)
            if _gcs_fingerprint(uri, creds):
                detected.append(p)
        if not detected:
            detected = _available_platforms_by_table(bq, project_id, client_key, max_age_minutes)
        platforms = detected
        logger.info(f"Plataformas detectadas para {client_key}: {platforms}")

    # 3) Filtrar válidas
    platforms = [p for p in platforms if p in VALID_PLATFORMS]
    logger.info(f"Plataformas a procesar (normalizadas): {platforms}")

    if not platforms:
        logger.warning(f"No hay plataformas para procesar en {client_key}. Salgo.")
        return 0

    processed_any = False

    # 4) Por cada plataforma, procesar solo si hay snapshot NUEVO (por generation)
    for p in platforms:
        uri = _path_for(p, client_key)
        fp = _gcs_fingerprint(uri, creds)
        if not fp:
            logger.warning(f"[{p}] snapshot no existe en GCS → salto")
            continue

        last = _read_cursor(bq, project_id, client_key, p)
        if last == fp:
            logger.info(f"[{p}] misma generation {fp} ya procesada → nada que hacer")
            continue

        try:
            n = transform_flow(
                gcs_path=uri,
                client_key=client_key,
                platform=p,
                project_id=project_id,
            )
            logger.info(f"[{p}] transform upsert rows: {n}")
            _write_cursor(bq, project_id, client_key, p, fp)
            processed_any = True
        except NotFound as e:
            logger.warning(f"Skipping {p}: snapshot no encontrado ({e})")

    # 5) Scoring una sola vez si hubo novedades
    if processed_any:
        logger.info("Ejecutando scoring…")
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
    else:
        logger.info("No hubo novedades; no ejecuto scoring.")
        return 0
