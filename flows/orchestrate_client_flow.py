from __future__ import annotations

# Stdlib
import os
import re
import json
import base64
from urllib.parse import urlparse
from typing import Optional, List

# Third-party
from prefect import flow, get_run_logger
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account

# Internal flows
from flows.transform_flow import transform_flow
from flows.score_simple_flow import score_monthly_simple


# -----------------------------------------------------------------------------
# Supported platforms (lógicas)
# -----------------------------------------------------------------------------
VALID_PLATFORMS = {
    "tn",
    "ga",
    "meta-ads",
    "ig",        # instagram / ig
    "bcra",
    "merchant",
    "tiktok",
}


# -----------------------------------------------------------------------------
# GCP CREDENTIALS
# -----------------------------------------------------------------------------
def _get_gcp_credentials():
    logger = get_run_logger()
    raw = (os.getenv("SERVICE_ACCOUNT_B64") or "").strip()

    if not raw:
        logger.warning("[AUTH] SERVICE_ACCOUNT_B64 empty, using ADC/default")
        return None

    try:
        if raw.lstrip().startswith("{"):
            info = json.loads(raw)
        else:
            clean = re.sub(r"\s+", "", raw)
            clean += "=" * (-len(clean) % 4)
            info = json.loads(base64.b64decode(clean).decode("utf-8"))
    except Exception as e:
        raise RuntimeError(f"SERVICE_ACCOUNT_B64 invalid: {e}")

    logger.info("[AUTH] Using service account from secret")
    return service_account.Credentials.from_service_account_info(info)


# -----------------------------------------------------------------------------
# GCS HELPERS
# -----------------------------------------------------------------------------
def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    u = urlparse(uri)
    return u.netloc, u.path.lstrip("/")


def _paths_for(platform: str, client_key: str) -> List[str]:
    """
    Devuelve TODOS los paths posibles para una plataforma lógica
    """
    base = f"gs://loopi-data-dev/{client_key}"

    paths = {
        "tn": [f"{base}/tiendanube/snapshot-latest.json"],
        "ga": [f"{base}/ga/snapshot-latest.json"],
        "meta-ads": [f"{base}/meta-ads/snapshot-latest.json"],

        # Instagram: alias soportados
        "ig": [
            f"{base}/ig/snapshot-latest.json",
            f"{base}/instagram/snapshot-latest.json",
        ],

        "bcra": [f"{base}/bcra/snapshot-latest.json"],
        "merchant": [f"{base}/merchant/snapshot-latest.json"],

        # TikTok
        "tiktok": [f"{base}/tiktok/snapshot-latest.json"],
    }

    return paths[platform]


def _gcs_fingerprint(uris: List[str], creds) -> str | None:
    """
    Devuelve generation del PRIMER snapshot existente
    """
    client = storage.Client(credentials=creds) if creds else storage.Client()

    for uri in uris:
        bucket, path = _parse_gcs_uri(uri)
        blob = client.bucket(bucket).blob(path)

        if blob.exists():
            blob.reload()
            return str(blob.generation)

    return None


def _resolve_existing_uri(uris: List[str], creds) -> str | None:
    """
    Devuelve el path real existente (para pasarlo al transform_flow)
    """
    client = storage.Client(credentials=creds) if creds else storage.Client()

    for uri in uris:
        bucket, path = _parse_gcs_uri(uri)
        if client.bucket(bucket).blob(path).exists():
            return uri

    return None


# -----------------------------------------------------------------------------
# BIGQUERY CURSORS
# -----------------------------------------------------------------------------
def _read_cursor(bq, project_id, client_key, platform):
    sql = f"""
    SELECT last_generation
    FROM `{project_id}.ops.snapshot_cursor`
    WHERE client_key=@ck AND platform=@pf
    LIMIT 1
    """
    rows = list(
        bq.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("ck", "STRING", client_key),
                    bigquery.ScalarQueryParameter("pf", "STRING", platform),
                ]
            ),
        ).result()
    )
    return rows[0]["last_generation"] if rows else None


def _write_cursor(bq, project_id, client_key, platform, generation):
    sql = f"""
    MERGE `{project_id}.ops.snapshot_cursor` T
    USING (
      SELECT
        @ck AS client_key,
        @pf AS platform,
        @g  AS last_generation,
        CURRENT_TIMESTAMP() AS last_updated
    ) S
    ON T.client_key=S.client_key AND T.platform=S.platform
    WHEN MATCHED THEN
      UPDATE SET last_generation=S.last_generation, last_updated=S.last_updated
    WHEN NOT MATCHED THEN
      INSERT (client_key, platform, last_generation, last_updated)
      VALUES (S.client_key, S.platform, S.last_generation, S.last_updated)
    """
    bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ck", "STRING", client_key),
                bigquery.ScalarQueryParameter("pf", "STRING", platform),
                bigquery.ScalarQueryParameter("g", "STRING", generation),
            ]
        ),
    ).result()


# -----------------------------------------------------------------------------
# PLATFORM NORMALIZATION
# -----------------------------------------------------------------------------
def _normalize_platforms_arg(platforms):
    if platforms is None:
        return None
    if isinstance(platforms, (list, tuple, set)):
        return [str(p).strip() for p in platforms if str(p).strip()]
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
            return [t.strip() for t in s.split(",") if t.strip()]
    return [str(platforms).strip()]


# -----------------------------------------------------------------------------
# MAIN FLOW
# -----------------------------------------------------------------------------
@flow(name="orchestrate-client")
def orchestrate_client(
    project_id: str,
    client_key: str,
    target_table: str | None = None,
    months_back: int = 24,
    aggregate_last_n: int = 12,
    platforms: list[str] | None = None,
):
    logger = get_run_logger()

    creds = _get_gcp_credentials()
    bq = bigquery.Client(project=project_id, credentials=creds) if creds else bigquery.Client(project=project_id)

    logger.info(f"[START] client={client_key} project={bq.project}")

    platforms = _normalize_platforms_arg(platforms)

    # Autodetección
    if not platforms:
        detected = []
        for p in VALID_PLATFORMS:
            uris = _paths_for(p, client_key)
            if _gcs_fingerprint(uris, creds):
                detected.append(p)
        platforms = detected
        logger.info(f"[AUTODETECT] platforms={platforms}")

    platforms = [p for p in platforms if p in VALID_PLATFORMS]
    if not platforms:
        logger.warning("No platforms to process. Exiting.")
        return 0

    processed_any = False

    for p in platforms:
        uris = _paths_for(p, client_key)
        fp = _gcs_fingerprint(uris, creds)

        if not fp:
            logger.warning(f"[{p}] No snapshot found, skipping")
            continue

        last = _read_cursor(bq, project_id, client_key, p)
        if last == fp:
            logger.info(f"[{p}] Snapshot unchanged ({fp}), skipping")
            continue

        uri = _resolve_existing_uri(uris, creds)
        if not uri:
            logger.warning(f"[{p}] Snapshot vanished before processing")
            continue

        try:
            n = transform_flow(
                gcs_path=uri,
                client_key=client_key,
                platform=p,
                project_id=project_id,
            )
            logger.info(f"[{p}] transform rows={n}")
            _write_cursor(bq, project_id, client_key, p, fp)
            processed_any = True

        except NotFound:
            logger.warning(f"[{p}] Not found in GCS, skipping")
        except Exception as e:
            logger.error(f"[{p}] Transform error: {e}")

    # Scoring
    if processed_any:
        score = score_monthly_simple(
            project_id=project_id,
            client_key=client_key,
            target_table=target_table or f"{project_id}.gold.scoring_client_minimal",
            months_back=months_back,
            aggregate_last_n=aggregate_last_n,
        )
        logger.info(f"[SCORE] client={client_key} score={score}")
        return score

    logger.info("No updates → skip scoring.")
    return 0
